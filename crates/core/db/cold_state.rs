//! Cold state: historical trajectories of tracked objects
//!
//! This module manages the historical data of moving objects, optimized for
//! append-only writes and time-range queries. It uses a persistent log for
//! durability and a memory buffer for recent history access.

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use spatio_types::config::{SyncMode, SyncPolicy};
use spatio_types::point::Point3d;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::PersistenceConfig;
use crate::error::{Result, SpatioError};

/// Durability settings governing when buffered writes are flushed to the OS
/// and synced to stable storage.
#[derive(Debug, Clone, Copy)]
pub struct SyncSettings {
    pub policy: SyncPolicy,
    pub mode: SyncMode,
    pub batch_size: usize,
}

impl Default for SyncSettings {
    fn default() -> Self {
        Self {
            policy: SyncPolicy::default(),
            mode: SyncMode::default(),
            batch_size: 1,
        }
    }
}

/// Single location update in history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationUpdate {
    pub timestamp: SystemTime,
    pub position: Point3d,
    pub metadata: serde_json::Value,
}

/// Recent updates of one object.
#[derive(Default)]
struct RecentBuffer {
    updates: VecDeque<LocationUpdate>,
    /// Updates evicted from this buffer are all older than this, in micros.
    horizon: u64,
    /// Write clock at the last update, for least-recently-written eviction.
    last_write: u64,
}

/// Result of [`ColdState::compact`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub bytes_before: u64,
    pub bytes_after: u64,
}

/// Cold state: historical trajectories
pub struct ColdState {
    /// Append-only log file
    trajectory_log: Arc<Mutex<TrajectoryLog>>,

    /// Recent updates per "namespace::object_id". A cache over the log for
    /// file databases, and the only history `:memory:` databases keep.
    recent_buffer: DashMap<String, RecentBuffer>,

    /// Buffer size per object (e.g., last 100 updates)
    buffer_capacity: usize,

    /// Path of the file-backed log, if any (used for checkpoint/recovery).
    log_path: Option<std::path::PathBuf>,

    /// File databases drop the least recently written buffers beyond this.
    max_buffered_objects: usize,

    /// Every update missing from the buffers is older than this, in micros
    /// (0: none is missing, `u64::MAX`: unknown), so newer ones can be read
    /// from the buffers alone.
    horizon: AtomicU64,

    /// Ticks once per update to order `RecentBuffer::last_write`.
    write_clock: AtomicU64,

    /// Set while a sweep is dropping idle buffers.
    evicting: AtomicBool,

    /// Held by log scans, and exclusively while compaction swaps the file.
    scan_gate: RwLock<()>,

    /// Records the last recovery could not read. Non-zero blocks checkpoints.
    replay_skipped: AtomicU64,

    /// Exclusive lock on `<log>.lock`, held until close or drop.
    lock: Mutex<Option<File>>,

    /// Background `EverySecond` syncer; dropping the sender stops it.
    syncer: Option<(mpsc::Sender<()>, JoinHandle<()>)>,
}

impl ColdState {
    /// Create a new cold state
    pub fn new(
        log_path: &Path,
        buffer_capacity: usize,
        config: PersistenceConfig,
        sync: SyncSettings,
    ) -> Result<Self> {
        // Ensure directory exists
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lock = lock_log(log_path)?;

        let has_prior_history =
            std::fs::metadata(log_path).is_ok_and(|m| m.len() > LOG_HEADER_V2.len() as u64 + 1);

        let trajectory_log = Arc::new(Mutex::new(TrajectoryLog::open_file(
            log_path,
            config.buffer_size,
            sync,
        )?));
        let syncer = match sync.policy {
            SyncPolicy::EverySecond => Some(spawn_syncer(trajectory_log.clone())?),
            _ => None,
        };

        Ok(Self {
            trajectory_log,
            recent_buffer: DashMap::new(),
            buffer_capacity,
            max_buffered_objects: usize::MAX,
            log_path: Some(log_path.to_path_buf()),
            horizon: AtomicU64::new(if has_prior_history { u64::MAX } else { 0 }),
            write_clock: AtomicU64::new(0),
            evicting: AtomicBool::new(false),
            scan_gate: RwLock::new(()),
            replay_skipped: AtomicU64::new(0),
            lock: Mutex::new(Some(lock)),
            syncer,
        })
    }

    /// Create a purely in-memory cold state.
    ///
    /// Used by `:memory:` databases: no file is created and no temp directory
    /// is touched. History is bounded: each object keeps its last
    /// `buffer_capacity` updates, and `delete` drops them.
    pub fn new_memory(buffer_capacity: usize) -> Self {
        Self {
            trajectory_log: Arc::new(Mutex::new(TrajectoryLog::open_memory())),
            recent_buffer: DashMap::new(),
            buffer_capacity,
            max_buffered_objects: usize::MAX,
            log_path: None,
            horizon: AtomicU64::new(0),
            write_clock: AtomicU64::new(0),
            evicting: AtomicBool::new(false),
            scan_gate: RwLock::new(()),
            replay_skipped: AtomicU64::new(0),
            lock: Mutex::new(None),
            syncer: None,
        }
    }

    /// Cap the objects with a recent buffer. File databases only: a memory
    /// database has nothing behind its buffers.
    pub fn with_max_buffered_objects(mut self, max: usize) -> Self {
        self.max_buffered_objects = max;
        self
    }

    /// Create a composite key from namespace and object ID
    #[inline]
    fn make_key(namespace: &str, object_id: &str) -> String {
        format!("{}::{}", namespace, object_id)
    }

    /// Get detailed statistics about cold state
    pub fn stats(&self) -> (usize, usize) {
        let trajectory_count = self.recent_buffer.len();

        // Estimate buffer size: sum of all trajectory lengths * ~100 bytes per point
        let buffer_bytes = self
            .recent_buffer
            .iter()
            .map(|entry| entry.value().updates.len() * 100)
            .sum();

        (trajectory_count, buffer_bytes)
    }

    /// Append location update to persistent log + buffer
    pub fn append_update(
        &self,
        namespace: &str,
        object_id: &str,
        position: Point3d,
        metadata: serde_json::Value,
        timestamp: SystemTime,
    ) -> Result<()> {
        // Truncate to the log's microsecond precision so buffer and disk
        // results dedupe when merged.
        let update = LocationUpdate {
            timestamp: truncate_to_micros(timestamp),
            position,
            metadata,
        };

        self.trajectory_log
            .lock()
            .append(namespace, object_id, &update)?;

        let full_key = Self::make_key(namespace, object_id);
        let mut buffer = self.recent_buffer.entry(full_key).or_default();
        let is_new = buffer.updates.is_empty();

        buffer.updates.push_back(update);
        buffer.last_write = self.write_clock.fetch_add(1, Ordering::Relaxed);

        // Each call appends one record to a buffer that held <= capacity, so at
        // most one eviction is ever needed.
        if buffer.updates.len() > self.buffer_capacity
            && let Some(evicted) = buffer.updates.pop_front()
        {
            buffer.horizon = buffer.horizon.max(horizon_after(evicted.timestamp));
        }
        drop(buffer);

        if is_new && self.log_path.is_some() && self.recent_buffer.len() > self.max_buffered_objects
        {
            self.evict_idle_buffers();
        }

        Ok(())
    }

    /// Drop the least recently written buffers, leaving 10% headroom so the
    /// sweep's cost is spread over many inserts.
    fn evict_idle_buffers(&self) {
        if self.evicting.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut by_age: Vec<(u64, String)> = self
            .recent_buffer
            .iter()
            .map(|entry| (entry.value().last_write, entry.key().clone()))
            .collect();
        let excess = by_age
            .len()
            .saturating_sub(self.max_buffered_objects - self.max_buffered_objects / 10);
        if excess > 0 && excess < by_age.len() {
            by_age.select_nth_unstable(excess);
        }
        for (_, key) in by_age.iter().take(excess) {
            self.drop_buffer(key);
        }
        self.evicting.store(false, Ordering::Release);
    }

    /// Drop a buffer, raising the horizon first so a query can't trust the
    /// buffers for records that just left them.
    fn drop_buffer(&self, key: &str) {
        self.recent_buffer.remove_if(key, |_, buffer| {
            let newest = buffer
                .updates
                .iter()
                .map(|u| horizon_after(u.timestamp))
                .max()
                .unwrap_or(0);
            self.horizon
                .fetch_max(newest.max(buffer.horizon), Ordering::AcqRel);
            true
        });
    }

    /// Append a deletion marker for an object. On recovery, tombstones are
    /// resolved by append order (a tombstone hides any earlier record; a later
    /// update revives the object) — unlike updates, which resolve by timestamp.
    pub fn append_tombstone(&self, namespace: &str, object_id: &str) -> Result<()> {
        let micros = micros_since_epoch(SystemTime::now());
        self.trajectory_log
            .lock()
            .append_tombstone(micros, namespace, object_id)?;
        self.drop_buffer(&Self::make_key(namespace, object_id));
        Ok(())
    }

    /// Force flush of the trajectory log to disk
    pub fn flush(&self) -> Result<()> {
        let mut log = self.trajectory_log.lock();
        log.flush()
    }

    /// Flush, reject further appends, and release the file lock so the same
    /// path can be reopened while this handle is still alive.
    pub fn close(&self) -> Result<()> {
        self.trajectory_log.lock().close()?;
        self.lock.lock().take();
        Ok(())
    }

    /// Query trajectory history
    pub fn query_trajectory(
        &self,
        namespace: &str,
        object_id: &str,
        start_time: SystemTime,
        end_time: SystemTime,
        limit: usize,
    ) -> Result<Vec<LocationUpdate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let full_key = Self::make_key(namespace, object_id);

        let (mut recent, own_horizon): (Vec<LocationUpdate>, u64) =
            match self.recent_buffer.get(&full_key) {
                Some(buffer) => (
                    buffer
                        .updates
                        .iter()
                        .rev()
                        .filter(|u| u.timestamp >= start_time && u.timestamp <= end_time)
                        .cloned()
                        .collect(),
                    buffer.horizon,
                ),
                None => (Vec::new(), 0),
            };

        // Read after the buffer: a concurrent drop raises it before removing.
        let horizon = match self.log_path {
            Some(_) => own_horizon.max(self.horizon.load(Ordering::Acquire)),
            None => 0,
        };

        // Sort by timestamp (newest first) rather than trusting insertion order,
        // which can differ from time order under custom timestamps.
        recent.sort_by_key(|u| Reverse(u.timestamp));
        let certain = recent
            .iter()
            .take_while(|u| micros_since_epoch(u.timestamp) >= horizon as u128)
            .count();
        if micros_since_epoch(start_time) >= horizon as u128 || certain >= limit {
            recent.truncate(limit);
            return Ok(recent);
        }

        // The log holds everything the buffers do, so it alone is the answer.
        let _scan = self.scan_gate.read();
        let target = self.trajectory_log.lock().flush_and_file_target()?;
        match target {
            Some(target) => scan_file(&target, namespace, object_id, start_time, end_time, limit),
            None => Ok(Vec::new()),
        }
    }

    /// Rewrite the log without history older than `keep_since`, returning its
    /// size before and after. Each live object keeps its latest update, so
    /// recovery is unaffected. Writes wait until it finishes, and it needs free
    /// disk space for a second copy of what it keeps. No-op for memory databases.
    pub fn compact(&self, keep_since: SystemTime) -> Result<CompactionStats> {
        if self.log_path.is_none() {
            return Ok(CompactionStats::default());
        }
        let stats = {
            let _scans = self.scan_gate.write();
            self.trajectory_log.lock().compact(keep_since)?
        };
        // Buffered updates older than this may be gone from the log, so they
        // must not be trusted any more.
        self.horizon
            .fetch_max(horizon_after(keep_since), Ordering::AcqRel);
        Ok(stats)
    }

    /// Recover current locations on startup.
    ///
    /// Returns a map of "namespace::object_id" → latest surviving LocationUpdate.
    /// For file-backed logs this loads the checkpoint snapshot (if present and
    /// valid) and then replays only the log tail written after it, so recovery
    /// cost is O(live objects + tail) instead of O(entire history). A missing or
    /// corrupt snapshot safely falls back to a full replay.
    pub fn recover_current_locations(&self) -> Result<HashMap<String, LocationUpdate>> {
        let mut entries: HashMap<String, Option<LocationUpdate>> = HashMap::new();
        let mut from_offset = 0u64;
        let mut horizon = 0u64;

        if let Some(log_path) = &self.log_path {
            let log_len = std::fs::metadata(log_path).map(|m| m.len()).unwrap_or(0);
            // Only trust the snapshot if it covers a prefix the log still has;
            // a shorter log means the snapshot is stale, so full-replay instead.
            if let Some((snapshot, covered_len, snapshot_horizon)) =
                read_snapshot(&snapshot_path_for(log_path))
                && covered_len <= log_len
            {
                for (key, update) in snapshot {
                    entries.insert(key, Some(update));
                }
                from_offset = covered_len;
                horizon = snapshot_horizon;
            }
        }

        {
            let log = self.trajectory_log.lock();
            let replayed = log.replay(from_offset, &mut entries)?;
            self.replay_skipped
                .store(replayed.skipped, Ordering::Relaxed);
            // A skipped record's timestamp is unknown, so the horizon is too.
            if replayed.skipped == 0 {
                self.horizon
                    .store(horizon.max(replayed.horizon), Ordering::Release);
            }
        }

        Ok(entries
            .into_iter()
            .filter_map(|(key, slot)| slot.map(|u| (key, u)))
            .collect())
    }

    /// Persist a checkpoint snapshot of `state` (the recovered current
    /// locations) covering the current on-disk log length, so the next startup
    /// replays only records appended afterwards. The full history log is left
    /// intact (trajectory queries still see everything). No-op for memory logs.
    pub fn write_checkpoint(&self, state: &HashMap<String, LocationUpdate>) -> Result<()> {
        let Some(log_path) = &self.log_path else {
            return Ok(());
        };
        // A checkpoint would mark the skipped records as covered, so a repaired
        // log could never bring them back.
        let skipped = self.replay_skipped.load(Ordering::Relaxed);
        if skipped > 0 {
            log::warn!("Skipping recovery checkpoint: replay skipped {skipped} log records");
            return Ok(());
        }
        // The snapshot covers exactly the bytes recovery read. write_checkpoint
        // runs at open with no concurrent writers, so the current on-disk length
        // is that boundary — no flush needed (which keeps buffered writes
        // buffered). Any not-yet-flushed bytes are simply replayed next time.
        let covered_len = std::fs::metadata(log_path).map(|m| m.len()).unwrap_or(0);
        write_snapshot(
            &snapshot_path_for(log_path),
            state,
            covered_len,
            self.horizon.load(Ordering::Acquire),
        )
    }
}

/// On-disk log format version.
///
/// Legacy `V1` logs have no header and no per-record checksum. `V2` logs begin
/// with [`LOG_HEADER_V2`] and prefix each record with a CRC32 of the record
/// body, so torn/merged/corrupt lines are detected and skipped on recovery.
/// Existing V1 logs are still read; new logs are written as V2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogVersion {
    V1,
    V2,
}

const LOG_HEADER_V2: &str = "#spatio-log v2";

/// CRC32 (IEEE 802.3 / ISO-HDLC, reflected). Implemented inline to avoid adding
/// a dependency. Check value: `crc32(b"123456789") == 0xCBF43926`.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in bytes {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Extract the parseable record body from a raw log line for `version`,
/// returning `None` for the header, comments, and CRC-failed records.
fn record_body(line: &str, version: LogVersion) -> Option<&str> {
    match version {
        LogVersion::V1 => Some(line),
        LogVersion::V2 => {
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (crc_hex, body) = line.split_once('|')?;
            let expected = u32::from_str_radix(crc_hex, 16).ok()?;
            if crc32(body.as_bytes()) != expected {
                log::warn!("Skipping log record with CRC mismatch (corrupt or torn write)");
                return None;
            }
            Some(body)
        }
    }
}

/// Best-effort `fsync` of a file's parent directory so a newly created file's
/// directory entry is durable across power loss. No-op where a directory handle
/// can't be opened/synced (e.g. Windows).
fn sync_parent_dir(path: &Path) {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    if let Ok(dir) = File::open(&parent) {
        let _ = dir.sync_all();
    }
}

/// Write one record body as a newline-terminated log line, prefixing a CRC32
/// (hex) under V2.
/// The line is written with a single `write_all`, so a failure never leaves
/// part of one record buffered ahead of the next.
fn write_record<W: Write>(w: &mut W, version: LogVersion, body: &str) -> std::io::Result<()> {
    let line = match version {
        LogVersion::V2 => format!("{:08x}|{}\n", crc32(body.as_bytes()), body),
        LogVersion::V1 => format!("{}\n", body),
    };
    w.write_all(line.as_bytes())
}

/// Truncate to the microsecond precision stored in the log.
pub(crate) fn truncate_to_micros(t: SystemTime) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(micros_since_epoch(t) as u64)
}

/// Microseconds since the Unix epoch (saturating at 0 for pre-epoch times).
fn micros_since_epoch(t: SystemTime) -> u128 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_micros()
}

/// The smallest horizon that covers an update at `t`.
fn horizon_after(t: SystemTime) -> u64 {
    u64::try_from(micros_since_epoch(t))
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

/// The canonical update-record body: `micros|ns|id|lat|lon|alt|json_len|json`,
/// coordinates at ~0.1 m precision. The single source of truth for the on-disk
/// layout, shared by the log and the snapshot writers.
fn format_update_body(
    micros: u128,
    namespace: &str,
    object_id: &str,
    position: &Point3d,
    metadata: &serde_json::Value,
) -> String {
    let json = serde_json::to_string(metadata).unwrap_or_else(|_| "null".to_string());
    format!(
        "{}|{}|{}|{:.6}|{:.6}|{:.6}|{}|{}",
        micros,
        namespace,
        object_id,
        position.y(), // lat
        position.x(), // lon
        position.z(), // alt
        json.len(),
        json,
    )
}

/// Parse an update-record body into `(timestamp, namespace, object_id, position,
/// raw metadata JSON)`. Returns `None` for tombstones and malformed bodies (wrong
/// field count or unparseable numbers) — the single parser shared by every read
/// path. Metadata is left raw so callers parse it only for records they keep.
fn parse_update_body(body: &str) -> Option<(SystemTime, &str, &str, Point3d, &str)> {
    // splitn keeps the metadata (last field) intact even if it contains '|'.
    let parts: Vec<&str> = body.splitn(8, '|').collect();
    if parts.len() != 8 {
        return None;
    }
    let micros: u128 = parts[0].parse().ok()?;
    let timestamp = UNIX_EPOCH + Duration::from_micros(u64::try_from(micros).unwrap_or(u64::MAX));
    let lat: f64 = parts[3].parse().ok()?;
    let lon: f64 = parts[4].parse().ok()?;
    let alt: f64 = parts[5].parse().ok()?;
    Some((
        timestamp,
        parts[1],
        parts[2],
        Point3d::new(lon, lat, alt),
        parts[7],
    ))
}

fn parse_metadata(raw: &str) -> serde_json::Value {
    serde_json::from_str(raw).unwrap_or(serde_json::Value::Null)
}

/// Followed by the covered log length and the horizon. Older snapshots have no
/// horizon and don't match, so they fall back to a full replay.
const SNAPSHOT_HEADER_PREFIX: &str = "#spatio-snap v2 ";

/// Path of the checkpoint snapshot beside a log file (`<log>.snap`).
fn snapshot_path_for(log_path: &Path) -> std::path::PathBuf {
    let mut s = log_path.as_os_str().to_os_string();
    s.push(".snap");
    std::path::PathBuf::from(s)
}

/// Read a checkpoint snapshot: the current-locations map, the log byte length it
/// covers, and the horizon of that covered log. Returns `None` (→ safe full replay) if the snapshot is
/// absent, has a malformed header, or contains *any* corrupt record — never a
/// partial or wrong state.
fn read_snapshot(path: &Path) -> Option<(HashMap<String, LocationUpdate>, u64, u64)> {
    let content = std::fs::read_to_string(path).ok()?;
    let mut lines = content.lines();
    let (covered_len, horizon) = lines
        .next()?
        .strip_prefix(SNAPSHOT_HEADER_PREFIX)?
        .trim()
        .split_once(' ')?;
    let covered_len: u64 = covered_len.parse().ok()?;
    let horizon: u64 = horizon.parse().ok()?;

    let mut map: HashMap<String, LocationUpdate> = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        // A corrupt record invalidates the whole snapshot (caller full-replays).
        let body = record_body(line, LogVersion::V2)?;
        let (timestamp, ns, id, position, metadata) = parse_update_body(body)?;
        map.insert(
            format!("{}::{}", ns, id),
            LocationUpdate {
                timestamp,
                position,
                metadata: parse_metadata(metadata),
            },
        );
    }
    Some((map, covered_len, horizon))
}

/// Atomically write a checkpoint snapshot: temp file → fsync → rename → fsync
/// parent dir, so a crash never leaves a half-written or stale-but-trusted snapshot.
fn write_snapshot(
    path: &Path,
    state: &HashMap<String, LocationUpdate>,
    covered_len: u64,
    horizon: u64,
) -> Result<()> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);

    {
        let file = File::create(&tmp)?;
        let mut w = BufWriter::new(file);
        writeln!(w, "{}{} {}", SNAPSHOT_HEADER_PREFIX, covered_len, horizon)?;
        for (key, update) in state {
            // Keys are validated delimiter-free, so the first "::" splits ns/id.
            let (ns, id) = key.split_once("::").unwrap_or((key.as_str(), ""));
            let micros = micros_since_epoch(update.timestamp);
            let body = format_update_body(micros, ns, id, &update.position, &update.metadata);
            write_record(&mut w, LogVersion::V2, &body)?;
        }
        w.flush()?;
        w.get_ref().sync_all()?;
    }

    std::fs::rename(&tmp, path)?;
    sync_parent_dir(path);
    Ok(())
}

/// A point-in-time view of the file-backed log, captured under the log lock:
/// the path, on-disk format, and the byte length to scan. Bounding reads to
/// `len` keeps a concurrent writer's appended tail (possibly a half-written
/// final line) out of the scan.
struct FileScanTarget {
    path: std::path::PathBuf,
    version: LogVersion,
    len: u64,
}

/// A scan result, ordered by time and then by position in the log.
struct ScanHit {
    order: (SystemTime, u64),
    update: LocationUpdate,
}

impl PartialEq for ScanHit {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }
}

impl Eq for ScanHit {}

impl PartialOrd for ScanHit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScanHit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order.cmp(&other.order)
    }
}

/// Unwrap a line read from a log. I/O errors are fatal, because skipping them
/// would silently lose records. `None` is a line that isn't UTF-8: a corrupt
/// record the reader has already moved past.
fn readable_line(line: std::io::Result<String>) -> Result<Option<String>> {
    match line {
        Ok(line) => Ok(Some(line)),
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Scan a file-backed log for the newest `limit` updates of an object within
/// `[start, end]`, newest first, holding only `limit` of them in memory. A free
/// function so it can run *without* the log lock held (the [`FileScanTarget`] is
/// captured under the lock first). Reading stops at `target.len` — the stable
/// prefix that existed at capture time — so concurrent appends past it never
/// present a torn line.
fn scan_file(
    target: &FileScanTarget,
    namespace: &str,
    object_id: &str,
    start_time: SystemTime,
    end_time: SystemTime,
    limit: usize,
) -> Result<Vec<LocationUpdate>> {
    let FileScanTarget { path, version, len } = target;
    let (version, len) = (*version, *len);
    if !path.exists() || len == 0 {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    // Bound the read to the prefix captured under the lock; anything appended
    // afterwards (a possibly half-written final line) is intentionally ignored.
    let reader = std::io::BufReader::new(std::io::Read::take(file, len));
    let mut newest: BinaryHeap<Reverse<ScanHit>> = BinaryHeap::new();

    for (seq, line_result) in std::io::BufRead::lines(reader).enumerate() {
        let Some(line) = readable_line(line_result)? else {
            continue;
        };

        // Strip the version header and verify the per-record CRC (V2); corrupt
        // / torn / tombstone lines are skipped by the parser.
        let Some(body) = record_body(&line, version) else {
            continue;
        };
        let Some((timestamp, ns, id, position, metadata)) = parse_update_body(body) else {
            continue;
        };
        if ns != namespace || id != object_id {
            continue;
        }
        if timestamp < start_time || timestamp > end_time {
            continue;
        }
        if newest.len() == limit {
            match newest.peek() {
                Some(Reverse(oldest)) if timestamp >= oldest.order.0 => {
                    newest.pop();
                }
                _ => continue,
            }
        }

        newest.push(Reverse(ScanHit {
            order: (timestamp, seq as u64),
            update: LocationUpdate {
                timestamp,
                position,
                metadata: parse_metadata(metadata),
            },
        }));
    }

    Ok(newest
        .into_sorted_vec()
        .into_iter()
        .map(|Reverse(hit)| hit.update)
        .collect())
}

/// Write the records of the log at `target` that `compact` keeps to `path`: its
/// updates from `keep_since` on, the tombstones that follow a kept update, and
/// finally any `live` latest update older than `keep_since`. Coming last, those
/// win over tombstones just as they did in the original order.
fn write_compacted(
    target: &FileScanTarget,
    path: &Path,
    keep_since: SystemTime,
    live: &HashMap<String, Option<LocationUpdate>>,
) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(writer, "{}", LOG_HEADER_V2)?;

    let source = std::io::Read::take(File::open(&target.path)?, target.len);
    let mut kept: HashSet<String> = HashSet::new();
    for line in std::io::BufRead::lines(std::io::BufReader::new(source)) {
        let line = line?;
        let Some(body) = record_body(&line, target.version) else {
            continue;
        };
        if let Some(rest) = body.strip_prefix("TOMBSTONE|") {
            if let Some((_, key)) = rest.split_once('|')
                && let Some((ns, id)) = key.split_once('|')
                && kept.contains(&format!("{}::{}", ns, id))
            {
                write_record(&mut writer, LogVersion::V2, body)?;
            }
            continue;
        }
        let Some((timestamp, ns, id, ..)) = parse_update_body(body) else {
            continue;
        };
        if timestamp >= keep_since {
            kept.insert(format!("{}::{}", ns, id));
            write_record(&mut writer, LogVersion::V2, body)?;
        }
    }

    for (key, update) in live {
        let Some(update) = update.as_ref().filter(|u| u.timestamp < keep_since) else {
            continue;
        };
        let (ns, id) = key.split_once("::").unwrap_or((key.as_str(), ""));
        let micros = micros_since_epoch(update.timestamp);
        let body = format_update_body(micros, ns, id, &update.position, &update.metadata);
        write_record(&mut writer, LogVersion::V2, &body)?;
    }

    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

/// Take an exclusive advisory lock on a `<log>.lock` sidecar. A sidecar rather
/// than the log itself, since Windows locks would also block log reads.
fn lock_log(log_path: &Path) -> Result<File> {
    let mut lock_path = log_path.as_os_str().to_os_string();
    lock_path.push(".lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(crate::error::SpatioError::Other(format!(
            "database {} is already open by another instance",
            log_path.display()
        ))),
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

/// Drop a trailing partial line left by a crash mid-append, so the next record
/// starts on a fresh line. Returns the resulting file length (0 if absent).
fn truncate_torn_tail(path: &Path) -> Result<u64> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let len = file.metadata()?.len();
    let mut end = len;
    let mut buf = [0u8; 4096];
    while end > 0 {
        let start = end.saturating_sub(buf.len() as u64);
        let chunk = &mut buf[..(end - start) as usize];
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(chunk)?;
        if let Some(i) = chunk.iter().rposition(|&b| b == b'\n') {
            end = start + i as u64 + 1;
            break;
        }
        end = start;
    }
    if end != len {
        log::warn!(
            "Truncating torn tail of trajectory log ({} bytes)",
            len - end
        );
        file.set_len(end)?;
        file.sync_all()?;
    }
    Ok(end)
}

/// Detect the format of an existing log; new or empty logs are V2.
///
/// A file that is neither V2 nor a real V1 log is an error, not an empty log:
/// its records would all be skipped and the database would open with no data.
fn detect_log_version(path: &Path) -> Result<LogVersion> {
    use std::io::BufRead;
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(LogVersion::V2),
        Err(e) => return Err(e.into()),
    };
    let mut first_line = Vec::new();
    std::io::BufReader::new(file).read_until(b'\n', &mut first_line)?;

    match first_line.strip_suffix(b"\n") {
        // A crash while writing the header leaves a prefix of it.
        None if LOG_HEADER_V2.as_bytes().starts_with(&first_line) => Ok(LogVersion::V2),
        None => Err(unrecognized_log(path)),
        Some(line) => {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line == LOG_HEADER_V2.as_bytes() {
                Ok(LogVersion::V2)
            } else if std::str::from_utf8(line).is_ok_and(is_v1_record) {
                Ok(LogVersion::V1)
            } else {
                Err(unrecognized_log(path))
            }
        }
    }
}

fn is_v1_record(line: &str) -> bool {
    if line.starts_with("TOMBSTONE|") {
        return line.split('|').count() >= 4;
    }
    // The length field rejects a V2 record that lost its header: its seventh
    // field is an altitude.
    parse_update_body(line).is_some()
        && line
            .split('|')
            .nth(6)
            .is_some_and(|len| len.parse::<usize>().is_ok())
}

fn unrecognized_log(path: &Path) -> SpatioError {
    SpatioError::Other(format!(
        "unrecognized trajectory log {}: its first line is neither a Spatio header nor a \
         record, so it is corrupt or from a newer version; the file was not modified",
        path.display()
    ))
}

/// Storage backend for the trajectory log.
///
/// File-backed databases serialize records to a durable append-only text log;
/// `:memory:` databases store nothing here and never touch the filesystem.
enum LogBackend {
    File {
        writer: BufWriter<File>,
        path: std::path::PathBuf,
        /// Records buffered in `writer` that have not yet been pushed to the OS.
        pending_writes: usize,
        /// Records written since the last `fsync`.
        writes_since_sync: usize,
        /// Wall-clock instant of the last `fsync`, used by [`SyncPolicy::EverySecond`].
        last_sync: Instant,
        buffer_limit: usize,
        sync: SyncSettings,
        /// On-disk format of this log (V2 for new files, V1 for legacy logs).
        version: LogVersion,
    },
    Memory,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LogState {
    Open,
    Closed,
    /// A write or sync failed: the file may end in a partial record, so
    /// nothing more may be appended after it.
    Failed,
}

/// What a replay learned beyond the entries it produced.
#[derive(Default)]
struct Replayed {
    /// Records that could not be read.
    skipped: u64,
    /// Every update read is older than this, in micros.
    horizon: u64,
}

/// Trajectory log: durable file-backed log or an in-memory log.
struct TrajectoryLog {
    backend: LogBackend,
    state: LogState,
}

impl TrajectoryLog {
    fn open_file(path: &Path, buffer_limit: usize, sync: SyncSettings) -> Result<Self> {
        // Before truncate_torn_tail, so a rejected file is left untouched.
        let version = detect_log_version(path)?;
        let existing_len = truncate_torn_tail(path)?;

        let file = OpenOptions::new().create(true).append(true).open(path)?;
        if existing_len == 0 {
            // Make the newly created file's directory entry durable.
            sync_parent_dir(path);
        }
        let mut writer = BufWriter::new(file);
        if existing_len == 0 {
            // Stamp the version header so later opens parse this log as V2.
            writeln!(writer, "{}", LOG_HEADER_V2)?;
        }

        Ok(Self {
            backend: LogBackend::File {
                writer,
                path: path.to_path_buf(),
                pending_writes: 0,
                writes_since_sync: 0,
                last_sync: Instant::now(),
                buffer_limit,
                sync,
                version,
            },
            state: LogState::Open,
        })
    }

    fn open_memory() -> Self {
        Self {
            backend: LogBackend::Memory,
            state: LogState::Open,
        }
    }

    /// Run a mutating operation, latching the log into `Failed` on error.
    fn guarded<T>(&mut self, op: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        match self.state {
            LogState::Open => {}
            LogState::Closed => return Err(SpatioError::DatabaseClosed),
            LogState::Failed => {
                return Err(SpatioError::Other(
                    "trajectory log is unusable after a failed write; reopen the database".into(),
                ));
            }
        }
        let result = op(self);
        if result.is_err() {
            self.state = LogState::Failed;
        }
        result
    }

    fn close(&mut self) -> Result<()> {
        if self.state == LogState::Closed {
            return Ok(());
        }
        self.guarded(|log| log.maybe_sync(true))?;
        self.state = LogState::Closed;
        Ok(())
    }

    /// Called periodically by the background syncer.
    fn sync_idle(&mut self) -> Result<()> {
        match &self.backend {
            LogBackend::File {
                writes_since_sync, ..
            } if *writes_since_sync > 0 && self.state == LogState::Open => {
                self.guarded(|log| log.maybe_sync(false))
            }
            _ => Ok(()),
        }
    }

    /// Flush the in-memory write buffer to the OS and, depending on the
    /// configured [`SyncPolicy`], `fsync` it to stable storage.
    ///
    /// `force` is set on explicit flush/close/drop: it triggers an `fsync`
    /// regardless of batch/interval thresholds (unless the policy is
    /// [`SyncPolicy::Never`], which never syncs). A no-op for memory logs.
    fn maybe_sync(&mut self, force: bool) -> Result<()> {
        let LogBackend::File {
            writer,
            pending_writes,
            writes_since_sync,
            last_sync,
            buffer_limit,
            sync,
            ..
        } = &mut self.backend
        else {
            return Ok(());
        };

        let fsync = match sync.policy {
            SyncPolicy::Never => false,
            SyncPolicy::Always => force || *writes_since_sync >= sync.batch_size,
            SyncPolicy::EverySecond => force || last_sync.elapsed() >= Duration::from_secs(1),
        };

        if fsync {
            writer.flush()?;
            match sync.mode {
                SyncMode::All => writer.get_ref().sync_all()?,
                SyncMode::Data => writer.get_ref().sync_data()?,
            }
            *pending_writes = 0;
            *writes_since_sync = 0;
            *last_sync = Instant::now();
        } else if force || *pending_writes >= *buffer_limit {
            // Push buffered bytes to the OS even when not syncing, so a clean
            // process exit doesn't lose writes still sitting in the BufWriter.
            writer.flush()?;
            *pending_writes = 0;
        }

        Ok(())
    }

    fn append(&mut self, namespace: &str, object_id: &str, update: &LocationUpdate) -> Result<()> {
        self.guarded(|log| log.append_inner(namespace, object_id, update))
    }

    fn append_inner(
        &mut self,
        namespace: &str,
        object_id: &str,
        update: &LocationUpdate,
    ) -> Result<()> {
        match &mut self.backend {
            // Log format (pipe-separated, 8 fields per line):
            //   timestamp_micros|namespace|object_id|lat|lon|alt|json_len|json_metadata
            //
            // Coordinates are written to 6 decimal places (~0.1 m precision).
            // Namespace and object_id must not contain the `|` character.
            LogBackend::File {
                writer,
                pending_writes,
                writes_since_sync,
                version,
                ..
            } => {
                let body = format_update_body(
                    micros_since_epoch(update.timestamp),
                    namespace,
                    object_id,
                    &update.position,
                    &update.metadata,
                );
                write_record(writer, *version, &body)?;

                *pending_writes += 1;
                *writes_since_sync += 1;
            }
            LogBackend::Memory => return Ok(()),
        }
        self.maybe_sync(false)
    }

    fn append_tombstone(&mut self, micros: u128, namespace: &str, object_id: &str) -> Result<()> {
        self.guarded(|log| log.append_tombstone_inner(micros, namespace, object_id))
    }

    fn append_tombstone_inner(
        &mut self,
        micros: u128,
        namespace: &str,
        object_id: &str,
    ) -> Result<()> {
        match &mut self.backend {
            LogBackend::File {
                writer,
                pending_writes,
                writes_since_sync,
                version,
                ..
            } => {
                let body = format!("TOMBSTONE|{}|{}|{}", micros, namespace, object_id);
                write_record(writer, *version, &body)?;
                *pending_writes += 1;
                *writes_since_sync += 1;
            }
            LogBackend::Memory => return Ok(()),
        }
        self.maybe_sync(false)
    }

    fn flush(&mut self) -> Result<()> {
        self.guarded(|log| log.maybe_sync(true))
    }

    /// Prepare a file-backed trajectory scan: flush buffered writes to the OS so
    /// a fresh read sees every appended record (including ones already evicted
    /// from the recent buffer), and return the path, version, and the on-disk
    /// length captured *under the lock*. The caller scans only this stable prefix
    /// (without holding the lock), so concurrent appends past the boundary can't
    /// present a torn final line — which would otherwise trip a spurious CRC
    /// warning and drop a record. Returns `None` for memory logs.
    fn flush_and_file_target(&mut self) -> Result<Option<FileScanTarget>> {
        match &mut self.backend {
            LogBackend::File {
                writer,
                path,
                pending_writes,
                version,
                ..
            } => {
                // Push to the OS page cache (not a full fsync) so a subsequent
                // File::open read observes all appended bytes.
                writer.flush()?;
                *pending_writes = 0;
                let len = std::fs::metadata(&*path).map(|m| m.len()).unwrap_or(0);
                Ok(Some(FileScanTarget {
                    path: path.clone(),
                    version: *version,
                    len,
                }))
            }
            LogBackend::Memory => Ok(None),
        }
    }

    /// Replace the log with its compacted form. Nothing is touched until the new
    /// file is complete and synced, and a failure before the swap leaves this log
    /// usable. A crash at any step leaves a log that opens with a full replay:
    /// the snapshot's offsets go stale in the swap, so it is removed first.
    fn compact(&mut self, keep_since: SystemTime) -> Result<CompactionStats> {
        let Some(target) = self.guarded(|log| log.flush_and_file_target())? else {
            return Ok(CompactionStats::default());
        };

        let mut entries = HashMap::new();
        let replayed = self.replay(0, &mut entries)?;
        if replayed.skipped > 0 {
            return Err(SpatioError::Other(format!(
                "refusing to compact: {} log records are unreadable",
                replayed.skipped
            )));
        }

        let mut compacted = target.path.as_os_str().to_os_string();
        compacted.push(".compact");
        let compacted = std::path::PathBuf::from(compacted);
        let snapshot = snapshot_path_for(&target.path);
        let swapped = write_compacted(&target, &compacted, keep_since, &entries)
            .and_then(|()| match std::fs::remove_file(&snapshot) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
                _ => {
                    sync_parent_dir(&snapshot);
                    Ok(())
                }
            })
            .and_then(|()| Ok(std::fs::rename(&compacted, &target.path)?));
        if let Err(e) = swapped {
            let _ = std::fs::remove_file(&compacted);
            return Err(e);
        }
        sync_parent_dir(&target.path);

        let reopened = OpenOptions::new().append(true).open(&target.path);
        let file = match reopened {
            Ok(file) => file,
            Err(e) => {
                self.state = LogState::Failed;
                return Err(e.into());
            }
        };
        let bytes_after = file.metadata()?.len();
        if let LogBackend::File {
            writer,
            pending_writes,
            writes_since_sync,
            version,
            ..
        } = &mut self.backend
        {
            *writer = BufWriter::new(file);
            *pending_writes = 0;
            *writes_since_sync = 0;
            *version = LogVersion::V2;
        }

        let live: HashMap<String, LocationUpdate> = entries
            .into_iter()
            .filter_map(|(key, update)| update.map(|u| (key, u)))
            .collect();
        if let Err(e) = write_snapshot(&snapshot, &live, bytes_after, replayed.horizon) {
            log::warn!("Failed to write snapshot after compaction: {}", e);
        }

        Ok(CompactionStats {
            bytes_before: target.len,
            bytes_after,
        })
    }

    /// Apply file log records from byte `from_offset` into `entries`, resolving
    /// the latest surviving update per key (tombstones clear an object; a later
    /// update revives it).
    fn replay(
        &self,
        from_offset: u64,
        entries: &mut HashMap<String, Option<LocationUpdate>>,
    ) -> Result<Replayed> {
        let mut skipped = 0u64;
        let mut horizon = 0u64;
        // Keep an update if the slot is empty/tombstoned, or not older (ties go
        // to the later record, matching the hot state).
        fn merge(slot: &mut Option<LocationUpdate>, update: LocationUpdate) {
            match slot {
                None => *slot = Some(update),
                Some(existing) if update.timestamp >= existing.timestamp => *slot = Some(update),
                _ => {}
            }
        }

        match &self.backend {
            LogBackend::File { path, version, .. } => {
                use std::io::{BufRead, BufReader, Seek, SeekFrom};
                let version = *version;
                if !path.exists() {
                    return Ok(Replayed::default());
                }
                let mut file = std::fs::File::open(path)?;
                if from_offset > 0 {
                    file.seek(SeekFrom::Start(from_offset))?;
                }
                let reader = BufReader::new(file);

                for (line_num, line_result) in reader.lines().enumerate() {
                    let Some(line) = readable_line(line_result)? else {
                        log::warn!("Line {} of trajectory log isn't UTF-8", line_num + 1);
                        skipped += 1;
                        continue;
                    };

                    // Strip header + verify CRC (V2); skip corrupt/torn lines.
                    let Some(body) = record_body(&line, version) else {
                        if !line.is_empty() && !line.starts_with('#') {
                            skipped += 1;
                        }
                        continue;
                    };

                    // Tombstone: TOMBSTONE|timestamp_micros|namespace|object_id
                    if body.starts_with("TOMBSTONE|") {
                        let parts: Vec<&str> = body.splitn(4, '|').collect();
                        if parts.len() != 4 {
                            log::warn!("Malformed tombstone on line {}", line_num + 1);
                            skipped += 1;
                            continue;
                        }
                        entries.insert(format!("{}::{}", parts[2], parts[3]), None);
                        continue;
                    }

                    let Some((timestamp, namespace, object_id, position, metadata)) =
                        parse_update_body(body)
                    else {
                        log::warn!("Malformed log line {}", line_num + 1);
                        skipped += 1;
                        continue;
                    };

                    horizon = horizon.max(horizon_after(timestamp));
                    let slot = entries
                        .entry(format!("{}::{}", namespace, object_id))
                        .or_insert(None);
                    merge(
                        slot,
                        LocationUpdate {
                            timestamp,
                            position,
                            metadata: parse_metadata(metadata),
                        },
                    );
                }
            }
            LogBackend::Memory => {}
        }

        Ok(Replayed { skipped, horizon })
    }
}

impl Drop for TrajectoryLog {
    fn drop(&mut self) {
        if self.state == LogState::Closed {
            return;
        }
        if let Err(e) = self.maybe_sync(true) {
            log::warn!("Failed to flush trajectory log on drop: {}", e);
        }
    }
}

impl Drop for ColdState {
    fn drop(&mut self) {
        if let Some((stop, handle)) = self.syncer.take() {
            drop(stop);
            let _ = handle.join();
        }
    }
}

/// Fsync idle `EverySecond` writes, which would otherwise wait for the next
/// append to trigger the interval check.
fn spawn_syncer(log: Arc<Mutex<TrajectoryLog>>) -> Result<(mpsc::Sender<()>, JoinHandle<()>)> {
    let (stop, stopped) = mpsc::channel::<()>();
    let handle = std::thread::Builder::new()
        .name("spatio-sync".into())
        .spawn(move || {
            while let Err(mpsc::RecvTimeoutError::Timeout) =
                stopped.recv_timeout(Duration::from_millis(250))
            {
                if let Err(e) = log.lock().sync_idle() {
                    log::warn!("Background sync of trajectory log failed: {}", e);
                }
            }
        })?;
    Ok((stop, handle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PersistenceConfig;
    use std::time::Duration;
    use tempfile::tempdir;

    /// Under `EverySecond`, an idle write must reach disk without a later append.
    #[test]
    fn test_every_second_syncs_idle_writes() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig {
                buffer_size: 10_000,
            },
            SyncSettings {
                policy: SyncPolicy::EverySecond,
                ..SyncSettings::default()
            },
        )
        .unwrap();

        cold.append_update(
            "v",
            "o",
            Point3d::new(1.0, 2.0, 3.0),
            serde_json::json!({}),
            UNIX_EPOCH + Duration::from_secs(1),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(1600));

        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            contents.contains("|v|o|"),
            "idle write not synced: {contents:?}"
        );
    }

    /// After a failed write the log may end mid-record, so appends must stop.
    #[test]
    fn test_failed_write_latches_log() {
        let mut log = TrajectoryLog::open_memory();
        let failed: Result<()> = log.guarded(|_| Err(SpatioError::Other("disk full".into())));
        assert!(failed.is_err());
        assert!(log.append_tombstone(0, "ns", "o").is_err());
        assert!(log.flush().is_err());
    }

    /// With `SyncPolicy::Always` and a large write buffer, a single append must
    /// already be on disk (flushed past the BufWriter) without any explicit flush.
    #[test]
    fn test_sync_policy_always_persists_immediately() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            // Large buffer: without fsync, one write would not reach disk.
            PersistenceConfig {
                buffer_size: 10_000,
            },
            SyncSettings {
                policy: SyncPolicy::Always,
                mode: SyncMode::Data,
                batch_size: 1,
            },
        )
        .unwrap();

        cold.append_update(
            "v",
            "o",
            Point3d::new(1.0, 2.0, 3.0),
            serde_json::json!({"k": "v"}),
            UNIX_EPOCH + Duration::from_secs(1),
        )
        .unwrap();

        // Read the raw file directly (a separate handle): the bytes must be there.
        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            contents.contains("|v|o|"),
            "Always policy must fsync the record to disk immediately, got: {contents:?}"
        );
    }

    /// `flush()`/close must push buffered writes to disk even under
    /// `SyncPolicy::Never` (which otherwise never syncs).
    #[test]
    fn test_flush_persists_under_never_policy() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig {
                buffer_size: 10_000,
            },
            SyncSettings {
                policy: SyncPolicy::Never,
                mode: SyncMode::All,
                batch_size: 1,
            },
        )
        .unwrap();

        cold.append_update(
            "v",
            "o",
            Point3d::new(1.0, 2.0, 3.0),
            serde_json::json!({}),
            UNIX_EPOCH + Duration::from_secs(1),
        )
        .unwrap();

        // Not yet flushed: still buffered in the BufWriter.
        assert!(std::fs::read_to_string(&log_path).unwrap().is_empty());

        cold.flush().unwrap();
        assert!(
            std::fs::read_to_string(&log_path)
                .unwrap()
                .contains("|v|o|")
        );
    }

    #[test]
    fn test_append_and_query_buffer() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig::default(),
            SyncSettings::default(),
        )
        .unwrap();

        let pos1 = Point3d::new(-74.0, 40.7, 0.0);
        let pos2 = Point3d::new(-74.1, 40.8, 0.0);

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + Duration::from_secs(2000);

        cold.append_update(
            "vehicles",
            "truck_001",
            pos1,
            serde_json::json!({"msg": "m1"}),
            t1,
        )
        .unwrap();
        cold.append_update(
            "vehicles",
            "truck_001",
            pos2,
            serde_json::json!({"msg": "m2"}),
            t2,
        )
        .unwrap();

        let history = cold
            .query_trajectory(
                "vehicles",
                "truck_001",
                t1,
                t2 + Duration::from_secs(1),
                100,
            )
            .unwrap();

        assert_eq!(history.len(), 2);
        assert_eq!(history[0].position.x(), -74.1); // Newest first
        assert_eq!(history[1].position.x(), -74.0);
    }

    #[test]
    fn test_buffer_capacity() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");

        let cold = ColdState::new(
            &log_path,
            2,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap(); // Capacity 2

        let pos = Point3d::new(0.0, 0.0, 0.0);

        for i in 0..5 {
            let t = UNIX_EPOCH + Duration::from_secs(i);
            cold.append_update("v", "o", pos.clone(), serde_json::json!({"id": i}), t)
                .unwrap();
        }

        // Check buffer directly - should only have last 2
        let key = ColdState::make_key("v", "o");
        let buffer = cold.recent_buffer.get(&key).unwrap();
        assert_eq!(buffer.updates.len(), 2);
        assert_eq!(
            buffer.updates[0].timestamp,
            UNIX_EPOCH + Duration::from_secs(3)
        );
        assert_eq!(
            buffer.updates[1].timestamp,
            UNIX_EPOCH + Duration::from_secs(4)
        );

        // But query_trajectory should return all from disk
        let history = cold
            .query_trajectory(
                "v",
                "o",
                UNIX_EPOCH,
                UNIX_EPOCH + Duration::from_secs(10),
                100,
            )
            .unwrap();

        assert_eq!(history.len(), 5); // All 5 from disk scan
        assert_eq!(history[0].timestamp, UNIX_EPOCH + Duration::from_secs(4));
        assert_eq!(history[1].timestamp, UNIX_EPOCH + Duration::from_secs(3));
    }

    #[test]
    fn test_trajectory_includes_history_from_before_reopen() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let open = || {
            ColdState::new(
                &log_path,
                100,
                PersistenceConfig { buffer_size: 0 },
                SyncSettings::default(),
            )
            .unwrap()
        };
        let pos = Point3d::new(0.0, 0.0, 0.0);

        {
            let cold = open();
            for i in 0..10 {
                let t = UNIX_EPOCH + Duration::from_secs(i);
                cold.append_update("v", "o", pos.clone(), serde_json::json!({}), t)
                    .unwrap();
            }
            cold.flush().unwrap();
        }

        let cold = open();
        cold.append_update(
            "v",
            "o",
            pos,
            serde_json::json!({}),
            UNIX_EPOCH + Duration::from_secs(10),
        )
        .unwrap();

        let history = cold
            .query_trajectory(
                "v",
                "o",
                UNIX_EPOCH,
                UNIX_EPOCH + Duration::from_secs(20),
                100,
            )
            .unwrap();
        assert_eq!(history.len(), 11);
        assert_eq!(history[0].timestamp, UNIX_EPOCH + Duration::from_secs(10));
    }

    #[test]
    fn test_recover_current_locations() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");

        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap();

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + Duration::from_secs(2000);
        let t3 = UNIX_EPOCH + Duration::from_secs(3000);

        // Add multiple updates for same object (should keep latest)
        cold.append_update(
            "vehicles",
            "truck_001",
            Point3d::new(-74.0, 40.0, 100.0),
            serde_json::json!({"data": "old"}),
            t1,
        )
        .unwrap();

        cold.append_update(
            "vehicles",
            "truck_001",
            Point3d::new(-74.1, 40.1, 200.0),
            serde_json::json!({"data": "new"}),
            t2,
        )
        .unwrap();

        // Add different object
        cold.append_update(
            "aircraft",
            "plane_001",
            Point3d::new(-75.0, 41.0, 5000.0),
            serde_json::json!({"type": "flight"}),
            t3,
        )
        .unwrap();

        // Recover
        let recovered = cold.recover_current_locations().unwrap();

        assert_eq!(recovered.len(), 2);

        // Check truck - should have latest position
        let truck_key = "vehicles::truck_001";
        let truck = recovered.get(truck_key).unwrap();
        assert_eq!(truck.position.x(), -74.1);
        assert_eq!(truck.position.y(), 40.1);
        assert_eq!(truck.timestamp, t2);
        assert_eq!(truck.metadata, serde_json::json!({"data": "new"}));

        // Check plane
        let plane_key = "aircraft::plane_001";
        let plane = recovered.get(plane_key).unwrap();
        assert_eq!(plane.position.x(), -75.0);
        assert_eq!(plane.position.z(), 5000.0);
        assert_eq!(plane.timestamp, t3);
    }

    #[test]
    fn test_tombstone_beats_future_timestamp_on_recovery() {
        // An object inserted with a future SetOptions timestamp must still stay deleted
        // after a tombstone is written, even though its timestamp is > current time.
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap();

        // Insert with a timestamp far in the future.
        let future_ts = UNIX_EPOCH + Duration::from_secs(99_999_999_999);
        cold.append_update(
            "ns",
            "obj",
            Point3d::new(1.0, 2.0, 0.0),
            serde_json::json!({}),
            future_ts,
        )
        .unwrap();
        // Tombstone written after the insert (later in log order).
        cold.append_tombstone("ns", "obj").unwrap();

        let recovered = cold.recover_current_locations().unwrap();
        assert!(
            !recovered.contains_key("ns::obj"),
            "future-timestamped object must not reappear after tombstone"
        );
    }

    #[test]
    fn test_tombstone_excludes_deleted_object_on_recovery() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap();

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + Duration::from_secs(2000);

        cold.append_update(
            "ns",
            "obj_keep",
            Point3d::new(1.0, 2.0, 0.0),
            serde_json::json!({}),
            t1,
        )
        .unwrap();
        cold.append_update(
            "ns",
            "obj_del",
            Point3d::new(3.0, 4.0, 0.0),
            serde_json::json!({}),
            t1,
        )
        .unwrap();
        cold.append_tombstone("ns", "obj_del").unwrap();
        cold.append_update(
            "ns",
            "obj_keep",
            Point3d::new(1.1, 2.1, 0.0),
            serde_json::json!({}),
            t2,
        )
        .unwrap();

        let recovered = cold.recover_current_locations().unwrap();
        assert!(
            recovered.contains_key("ns::obj_keep"),
            "kept object should be recovered"
        );
        assert!(
            !recovered.contains_key("ns::obj_del"),
            "deleted object must not be recovered"
        );
    }

    #[test]
    fn test_tombstone_then_reinsert_recovers_object() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap();

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        cold.append_update(
            "ns",
            "obj",
            Point3d::new(1.0, 2.0, 0.0),
            serde_json::json!({}),
            t1,
        )
        .unwrap();
        cold.append_tombstone("ns", "obj").unwrap();

        // Re-insert with a timestamp guaranteed to be after the tombstone (now + margin).
        let t2 = SystemTime::now() + Duration::from_secs(1);
        cold.append_update(
            "ns",
            "obj",
            Point3d::new(5.0, 6.0, 0.0),
            serde_json::json!({}),
            t2,
        )
        .unwrap();

        let recovered = cold.recover_current_locations().unwrap();
        assert!(
            recovered.contains_key("ns::obj"),
            "re-inserted object should survive recovery"
        );
        assert_eq!(recovered["ns::obj"].position.x(), 5.0);
    }

    /// With a full buffer and out-of-order timestamps, the newest record may
    /// have been evicted to disk. The query must still return it rather than
    /// short-circuiting on the buffer's contents.
    #[test]
    fn test_trajectory_query_out_of_order_timestamps() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        // Capacity 2: only the two most recently *inserted* records stay buffered.
        let cold = ColdState::new(
            &log_path,
            2,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap();

        let mk = |secs| UNIX_EPOCH + Duration::from_secs(secs);
        // Insert the newest-timestamped record FIRST so it gets evicted to disk.
        cold.append_update(
            "v",
            "o",
            Point3d::new(5.0, 0.0, 0.0),
            serde_json::json!({}),
            mk(5000),
        )
        .unwrap();
        cold.append_update(
            "v",
            "o",
            Point3d::new(1.0, 0.0, 0.0),
            serde_json::json!({}),
            mk(1000),
        )
        .unwrap();
        cold.append_update(
            "v",
            "o",
            Point3d::new(2.0, 0.0, 0.0),
            serde_json::json!({}),
            mk(2000),
        )
        .unwrap();
        cold.append_update(
            "v",
            "o",
            Point3d::new(3.0, 0.0, 0.0),
            serde_json::json!({}),
            mk(3000),
        )
        .unwrap();

        // Buffer now holds only t=2000 and t=3000; t=5000 lives on disk.
        let newest = cold
            .query_trajectory("v", "o", UNIX_EPOCH, mk(6000), 1)
            .unwrap();
        assert_eq!(newest.len(), 1);
        assert_eq!(
            newest[0].timestamp,
            mk(5000),
            "must return the globally-newest record even when it was evicted from the buffer"
        );
    }

    #[test]
    fn test_disk_based_trajectory_query() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");

        let cold = ColdState::new(
            &log_path,
            2,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap(); // Small buffer to force disk scan

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + Duration::from_secs(2000);
        let t3 = UNIX_EPOCH + Duration::from_secs(3000);
        let t4 = UNIX_EPOCH + Duration::from_secs(4000);
        let t5 = UNIX_EPOCH + Duration::from_secs(5000);

        // Add 5 updates - buffer only keeps last 2
        for (i, t) in [t1, t2, t3, t4, t5].iter().enumerate() {
            cold.append_update(
                "vehicles",
                "truck_001",
                Point3d::new(-74.0 + i as f64 * 0.1, 40.0, 0.0),
                serde_json::json!({"data": format!("data_{}", i)}),
                *t,
            )
            .unwrap();
        }

        // Query entire range - should scan disk for older entries
        let trajectory = cold
            .query_trajectory("vehicles", "truck_001", t1, t5, 10)
            .unwrap();

        // Should get all 5 results
        assert_eq!(trajectory.len(), 5);

        // Should be sorted newest first
        assert_eq!(trajectory[0].timestamp, t5);
        assert_eq!(trajectory[1].timestamp, t4);
        assert_eq!(trajectory[2].timestamp, t3);
        assert_eq!(trajectory[3].timestamp, t2);
        assert_eq!(trajectory[4].timestamp, t1);

        // Query with limit
        let limited = cold
            .query_trajectory("vehicles", "truck_001", t1, t5, 3)
            .unwrap();
        assert_eq!(limited.len(), 3);
        assert_eq!(limited[0].timestamp, t5);
    }

    #[test]
    fn test_trajectory_sees_buffered_but_evicted_records() {
        // Records evicted from the recent buffer but not yet OS-flushed must
        // still be visible to a trajectory query: the disk scan flushes first.
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            2, // tiny recent-buffer capacity
            PersistenceConfig {
                buffer_size: 10_000,
            }, // large: no incidental OS flush
            SyncSettings {
                policy: SyncPolicy::Never, // never fsyncs on its own
                mode: SyncMode::All,
                batch_size: 1,
            },
        )
        .unwrap();

        let base = UNIX_EPOCH + Duration::from_secs(1000);
        for i in 0..5u64 {
            cold.append_update(
                "ns",
                "o",
                Point3d::new(i as f64, 0.0, 0.0),
                serde_json::json!({ "i": i }),
                base + Duration::from_secs(i),
            )
            .unwrap();
        }

        let traj = cold
            .query_trajectory("ns", "o", base, base + Duration::from_secs(10), 100)
            .unwrap();
        assert_eq!(
            traj.len(),
            5,
            "all records must be visible even though 3 were evicted from the \
             buffer and none were explicitly flushed"
        );
    }

    #[test]
    fn test_concurrent_trajectory_scan_is_stable() {
        // A reader scanning the log while a writer appends must see a coherent
        // point-in-time prefix: never an error, never a count that goes
        // backwards, and the full history once the writer finishes. (Before the
        // stable-prefix bound, a concurrent append could present a torn final
        // line and drop a record mid-scan.)
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let cold = Arc::new(
            ColdState::new(
                &dir.path().join("traj.log"),
                2, // tiny buffer => trajectory queries hit the disk-scan path
                PersistenceConfig::default(),
                SyncSettings::default(),
            )
            .unwrap(),
        );

        let base = UNIX_EPOCH + Duration::from_secs(1_000);
        let n = 500u64;

        let writer = {
            let cold = Arc::clone(&cold);
            thread::spawn(move || {
                for i in 0..n {
                    cold.append_update(
                        "ns",
                        "o",
                        Point3d::new(1.0, 2.0, 0.0),
                        serde_json::json!({ "i": i }),
                        base + Duration::from_millis(i),
                    )
                    .unwrap();
                }
            })
        };

        // Concurrent reads while the writer appends: append-only, so the visible
        // count must be monotonically non-decreasing and never error.
        let window_end = base + Duration::from_secs(10);
        let mut last = 0usize;
        for _ in 0..200 {
            let traj = cold
                .query_trajectory("ns", "o", base, window_end, 100_000)
                .unwrap();
            assert!(
                traj.len() >= last,
                "visible record count must not go backwards"
            );
            last = traj.len();
        }
        writer.join().unwrap();

        let final_traj = cold
            .query_trajectory("ns", "o", base, window_end, 100_000)
            .unwrap();
        assert_eq!(
            final_traj.len() as u64,
            n,
            "all appended records visible once the writer finishes"
        );
    }

    #[test]
    fn test_crc32_check_value() {
        // Standard CRC-32/ISO-HDLC check value.
        assert_eq!(super::crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(super::crc32(b""), 0);
    }

    /// A corrupted record body (simulating bit-rot or a torn write) must fail
    /// its CRC and be skipped on recovery, without taking out healthy records.
    #[test]
    fn test_corrupt_record_is_skipped_on_recovery() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");

        {
            let cold = ColdState::new(
                &log_path,
                10,
                PersistenceConfig::default(),
                SyncSettings::default(),
            )
            .unwrap();
            cold.append_update(
                "ns",
                "good",
                Point3d::new(1.0, 2.0, 0.0),
                serde_json::json!({"v": 1}),
                UNIX_EPOCH + Duration::from_secs(1),
            )
            .unwrap();
            cold.append_update(
                "ns",
                "bad",
                Point3d::new(3.0, 4.0, 0.0),
                serde_json::json!({"v": 2}),
                UNIX_EPOCH + Duration::from_secs(2),
            )
            .unwrap();
            cold.flush().unwrap();
        }

        // Corrupt the body of the "bad" record while leaving its CRC prefix.
        let contents = std::fs::read_to_string(&log_path).unwrap();
        let corrupted: String = contents
            .lines()
            .map(|line| {
                if line.contains("|ns|bad|") {
                    line.replacen("|ns|bad|", "|ns|bad|9", 1)
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&log_path, format!("{corrupted}\n")).unwrap();

        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig::default(),
            SyncSettings::default(),
        )
        .unwrap();
        let recovered = cold.recover_current_locations().unwrap();
        assert!(
            recovered.contains_key("ns::good"),
            "healthy record must survive"
        );
        assert!(
            !recovered.contains_key("ns::bad"),
            "CRC-failed record must be skipped, not silently trusted"
        );
    }

    #[test]
    fn test_append_after_torn_tail_is_recovered() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let open = || {
            ColdState::new(
                &log_path,
                10,
                PersistenceConfig::default(),
                SyncSettings::default(),
            )
            .unwrap()
        };
        let pos = Point3d::new(1.0, 2.0, 0.0);
        {
            let cold = open();
            cold.append_update("ns", "a", pos.clone(), serde_json::json!({}), UNIX_EPOCH)
                .unwrap();
        }
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        f.write_all(b"deadbeef|123|ns|torn").unwrap();
        drop(f);

        {
            let cold = open();
            cold.append_update("ns", "b", pos, serde_json::json!({}), UNIX_EPOCH)
                .unwrap();
        }
        let recovered = open().recover_current_locations().unwrap();
        assert!(recovered.contains_key("ns::a"));
        assert!(
            recovered.contains_key("ns::b"),
            "record appended after a torn tail must recover"
        );
    }

    #[test]
    fn test_second_open_of_same_log_is_rejected() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let open = || {
            ColdState::new(
                &log_path,
                10,
                PersistenceConfig::default(),
                SyncSettings::default(),
            )
        };
        let first = open().unwrap();
        assert!(open().is_err(), "second open must fail while locked");
        drop(first);
        assert!(open().is_ok(), "lock is released on drop");
    }

    #[test]
    fn test_memory_mode_keeps_last_updates_per_object() {
        let cold = ColdState::new_memory(3);
        let t = |s| UNIX_EPOCH + Duration::from_secs(s);
        for i in 0..10 {
            cold.append_update(
                "ns",
                "o",
                Point3d::new(0.0, 0.0, 0.0),
                serde_json::json!({ "i": i }),
                t(i),
            )
            .unwrap();
        }
        let traj = cold.query_trajectory("ns", "o", t(0), t(99), 100).unwrap();
        let kept: Vec<_> = traj.iter().map(|u| u.metadata["i"].as_u64()).collect();
        assert_eq!(kept, [Some(9), Some(8), Some(7)]);
    }

    #[test]
    fn test_memory_mode_delete_frees_history() {
        let cold = ColdState::new_memory(3);
        let t = |s| UNIX_EPOCH + Duration::from_secs(s);
        let pos = Point3d::new(0.0, 0.0, 0.0);
        cold.append_update("ns", "o", pos.clone(), serde_json::json!({}), t(1))
            .unwrap();
        cold.append_tombstone("ns", "o").unwrap();
        assert!(cold.recent_buffer.is_empty());
        assert!(
            cold.query_trajectory("ns", "o", t(0), t(9), 10)
                .unwrap()
                .is_empty()
        );

        cold.append_update("ns", "o", pos, serde_json::json!({}), t(2))
            .unwrap();
        let traj = cold.query_trajectory("ns", "o", t(0), t(9), 10).unwrap();
        assert_eq!(traj.len(), 1);
    }

    #[test]
    fn test_memory_mode_keeps_records_with_equal_timestamps() {
        let cold = ColdState::new_memory(3);
        let t = UNIX_EPOCH + Duration::from_secs(1);
        for _ in 0..5 {
            cold.append_update(
                "ns",
                "o",
                Point3d::new(0.0, 0.0, 0.0),
                serde_json::json!({}),
                t,
            )
            .unwrap();
        }
        let traj = cold.query_trajectory("ns", "o", UNIX_EPOCH, t, 10).unwrap();
        assert_eq!(traj.len(), 3);
    }

    #[test]
    fn test_memory_mode_rejects_writes_after_close() {
        let cold = ColdState::new_memory(3);
        cold.close().unwrap();
        let result = cold.append_update(
            "ns",
            "o",
            Point3d::new(0.0, 0.0, 0.0),
            serde_json::json!({}),
            UNIX_EPOCH,
        );
        assert!(matches!(result, Err(SpatioError::DatabaseClosed)));
        assert!(matches!(
            cold.append_tombstone("ns", "o"),
            Err(SpatioError::DatabaseClosed)
        ));
    }

    #[test]
    fn test_tombstone_drops_buffer_but_keeps_history() {
        let dir = tempdir().unwrap();
        let cold = ColdState::new(
            &dir.path().join("traj.log"),
            10,
            PersistenceConfig::default(),
            SyncSettings::default(),
        )
        .unwrap();
        let pos = Point3d::new(0.0, 0.0, 0.0);
        let t = |s| UNIX_EPOCH + Duration::from_secs(s);
        cold.append_update("ns", "o", pos.clone(), serde_json::json!({}), t(1))
            .unwrap();
        cold.append_tombstone("ns", "o").unwrap();
        assert!(cold.recent_buffer.is_empty(), "delete must drop the buffer");

        cold.append_update("ns", "o", pos, serde_json::json!({}), t(2))
            .unwrap();
        let traj = cold.query_trajectory("ns", "o", t(0), t(9), 10).unwrap();
        assert_eq!(traj.len(), 2, "pre-delete history must stay queryable");
    }

    #[test]
    fn test_equal_timestamps_later_record_wins_on_recovery() {
        let dir = tempdir().unwrap();
        let cold = ColdState::new(
            &dir.path().join("traj.log"),
            10,
            PersistenceConfig::default(),
            SyncSettings::default(),
        )
        .unwrap();
        for x in [1.0, 2.0] {
            cold.append_update(
                "ns",
                "o",
                Point3d::new(x, 0.0, 0.0),
                serde_json::json!({}),
                UNIX_EPOCH,
            )
            .unwrap();
        }
        cold.flush().unwrap();
        let recovered = cold.recover_current_locations().unwrap();
        assert_eq!(recovered["ns::o"].position.x(), 2.0);
    }

    /// Legacy V1 logs (no header, no CRC) must still be recoverable.
    #[test]
    fn test_legacy_v1_log_is_readable() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("legacy.log");
        // Hand-write a V1-format line: ts|ns|id|lat|lon|alt|len|json
        std::fs::write(
            &log_path,
            "1000000|ns|obj|2.000000|1.000000|0.000000|2|{}\n",
        )
        .unwrap();

        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig::default(),
            SyncSettings::default(),
        )
        .unwrap();
        let recovered = cold.recover_current_locations().unwrap();
        let loc = recovered
            .get("ns::obj")
            .expect("legacy V1 record must recover");
        assert_eq!(loc.position.x(), 1.0);
        assert_eq!(loc.position.y(), 2.0);
    }

    fn open_cold(path: &Path) -> Result<ColdState> {
        ColdState::new(
            path,
            10,
            PersistenceConfig::default(),
            SyncSettings::default(),
        )
    }

    fn write_log(path: &Path) -> Vec<u8> {
        let cold = open_cold(path).unwrap();
        let update = LocationUpdate {
            timestamp: UNIX_EPOCH + Duration::from_secs(1),
            position: Point3d::new(1.0, 2.0, 0.0),
            metadata: serde_json::Value::Null,
        };
        cold.append_update(
            "ns",
            "o",
            update.position,
            update.metadata,
            update.timestamp,
        )
        .unwrap();
        cold.close().unwrap();
        drop(cold);
        std::fs::read(path).unwrap()
    }

    /// Opening must fail, leaving the file as found, rather than come up empty.
    #[test]
    fn test_corrupt_header_is_refused_and_file_untouched() {
        for corrupt in [
            b"#spatio-lof v2".to_vec(),
            b"#spatio-log v3".to_vec(),
            vec![0u8; 14],
        ] {
            let dir = tempdir().unwrap();
            let log_path = dir.path().join("t.log");
            let mut bytes = write_log(&log_path);
            bytes[..14].copy_from_slice(&corrupt);
            std::fs::write(&log_path, &bytes).unwrap();

            assert!(open_cold(&log_path).is_err());
            assert_eq!(std::fs::read(&log_path).unwrap(), bytes);

            bytes[..14].copy_from_slice(LOG_HEADER_V2.as_bytes());
            std::fs::write(&log_path, &bytes).unwrap();
            let cold = open_cold(&log_path).unwrap();
            assert!(
                cold.recover_current_locations()
                    .unwrap()
                    .contains_key("ns::o")
            );
        }
    }

    #[test]
    fn test_foreign_file_without_newline_is_refused_and_untouched() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("foreign");
        std::fs::write(&log_path, b"not a spatio log").unwrap();
        assert!(open_cold(&log_path).is_err());
        assert_eq!(std::fs::read(&log_path).unwrap(), b"not a spatio log");
    }

    #[test]
    fn test_v2_record_without_header_is_refused() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("t.log");
        let bytes = write_log(&log_path);
        std::fs::write(&log_path, &bytes[LOG_HEADER_V2.len() + 1..]).unwrap();
        assert!(open_cold(&log_path).is_err());
    }

    /// A crash while the header is being written leaves a prefix of it.
    #[test]
    fn test_torn_header_is_treated_as_new_log() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("t.log");
        std::fs::write(&log_path, &LOG_HEADER_V2.as_bytes()[..6]).unwrap();
        let cold = open_cold(&log_path).unwrap();
        assert!(cold.recover_current_locations().unwrap().is_empty());
    }

    /// A skipped record must stay recoverable once the log is repaired.
    #[test]
    fn test_no_checkpoint_after_skipped_record() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("t.log");
        let bytes = write_log(&log_path);
        let snapshot = snapshot_path_for(&log_path);
        let _ = std::fs::remove_file(&snapshot);

        let mut damaged = bytes.clone();
        *damaged.last_mut().unwrap() = b'\n';
        let record_start = LOG_HEADER_V2.len() + 1;
        damaged[record_start] ^= 1;
        std::fs::write(&log_path, &damaged).unwrap();

        let cold = open_cold(&log_path).unwrap();
        let recovered = cold.recover_current_locations().unwrap();
        assert!(recovered.is_empty());
        cold.write_checkpoint(&recovered).unwrap();
        assert!(!snapshot.exists());
        cold.close().unwrap();
        drop(cold);

        std::fs::write(&log_path, &bytes).unwrap();
        let cold = open_cold(&log_path).unwrap();
        assert!(
            cold.recover_current_locations()
                .unwrap()
                .contains_key("ns::o")
        );
    }

    fn file_cold(path: &Path, capacity: usize) -> ColdState {
        ColdState::new(
            path,
            capacity,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap()
    }

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn put(cold: &ColdState, id: &str, secs: u64) {
        cold.append_update(
            "ns",
            id,
            Point3d::new(1.0, 2.0, 0.0),
            serde_json::json!({ "s": secs }),
            at(secs),
        )
        .unwrap();
    }

    fn seconds(cold: &ColdState, id: &str, from: u64, limit: usize) -> Vec<u64> {
        cold.query_trajectory("ns", id, at(from), at(1_000_000), limit)
            .unwrap()
            .iter()
            .map(|u| u.metadata["s"].as_u64().unwrap())
            .collect()
    }

    /// Reopen as `DB::open` does: recover, then checkpoint.
    fn reopen(path: &Path, capacity: usize) -> ColdState {
        let cold = file_cold(path, capacity);
        let recovered = cold.recover_current_locations().unwrap();
        cold.write_checkpoint(&recovered).unwrap();
        cold
    }

    #[test]
    fn test_recent_window_is_served_from_memory_after_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        {
            let cold = file_cold(&path, 10);
            put(&cold, "o", 100);
            cold.close().unwrap();
        }

        let cold = reopen(&path, 10);
        put(&cold, "o", 200);
        assert_eq!(seconds(&cold, "o", 0, 10), [200, 100], "needs the log");

        std::fs::write(&path, "").unwrap();
        assert_eq!(seconds(&cold, "o", 101, 10), [200]);
        assert_eq!(seconds(&cold, "o", 0, 1), [200], "limit met from memory");
    }

    #[test]
    fn test_horizon_survives_checkpointed_restart_of_deleted_history() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        {
            let cold = file_cold(&path, 10);
            put(&cold, "gone", 500);
            cold.append_tombstone("ns", "gone").unwrap();
            put(&cold, "other", 100);
            cold.close().unwrap();
        }
        // The second open reads only the snapshot, which omits the deleted object.
        drop(reopen(&path, 10));
        let cold = reopen(&path, 10);

        put(&cold, "gone", 600);
        assert_eq!(seconds(&cold, "gone", 0, 10), [600, 500]);
    }

    #[test]
    fn test_delete_keeps_recent_windows_of_other_objects_in_memory() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        let cold = file_cold(&path, 10);
        put(&cold, "a", 100);
        put(&cold, "b", 100);
        cold.append_tombstone("ns", "a").unwrap();
        put(&cold, "b", 200);

        std::fs::write(&path, "").unwrap();
        assert_eq!(seconds(&cold, "b", 101, 10), [200]);
    }

    #[test]
    fn test_evicted_newer_record_is_found_under_out_of_order_writes() {
        let dir = tempdir().unwrap();
        let cold = file_cold(&dir.path().join("traj.log"), 2);
        put(&cold, "o", 900);
        put(&cold, "o", 100);
        put(&cold, "o", 200);

        assert_eq!(seconds(&cold, "o", 0, 1), [900]);
        assert_eq!(seconds(&cold, "o", 0, 10), [900, 200, 100]);
    }

    #[test]
    fn test_same_timestamp_records_all_survive_past_capacity() {
        let dir = tempdir().unwrap();
        let cold = file_cold(&dir.path().join("traj.log"), 2);
        for i in 0..3 {
            cold.append_update(
                "ns",
                "o",
                Point3d::new(0.0, 0.0, 0.0),
                serde_json::json!({ "i": i }),
                at(5),
            )
            .unwrap();
        }
        let traj = cold.query_trajectory("ns", "o", at(0), at(9), 10).unwrap();
        assert_eq!(traj.len(), 3);
    }

    #[test]
    fn test_limit_returns_newest_from_log_with_later_ties_first() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        let target_of = |cold: &ColdState| {
            cold.trajectory_log
                .lock()
                .flush_and_file_target()
                .unwrap()
                .unwrap()
        };
        let cold = file_cold(&path, 2);
        for (secs, i) in [(3, 0), (1, 1), (5, 2), (5, 3), (2, 4), (4, 5)] {
            cold.append_update(
                "ns",
                "o",
                Point3d::new(0.0, 0.0, 0.0),
                serde_json::json!({ "i": i }),
                at(secs),
            )
            .unwrap();
        }
        let hits = scan_file(&target_of(&cold), "ns", "o", at(0), at(9), 3).unwrap();
        let order: Vec<_> = hits.iter().map(|u| u.metadata["i"].as_u64()).collect();
        assert_eq!(order, [Some(3), Some(2), Some(5)]);
        let hits = scan_file(&target_of(&cold), "ns", "o", at(5), at(5), 1).unwrap();
        assert_eq!(hits[0].metadata["i"], 3, "the cut must keep the later tie");
        assert!(
            cold.query_trajectory("ns", "o", at(0), at(9), 0)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_idle_buffers_are_evicted_without_losing_history() {
        let dir = tempdir().unwrap();
        let cold = file_cold(&dir.path().join("traj.log"), 10).with_max_buffered_objects(10);
        for i in 0..100 {
            put(&cold, &format!("o{i}"), 100 + i);
        }
        assert!(cold.recent_buffer.len() <= 10);
        assert!(cold.recent_buffer.contains_key("ns::o99"), "newest stays");
        assert_eq!(seconds(&cold, "o0", 0, 10), [100]);
    }

    #[test]
    fn test_memory_buffers_are_never_evicted() {
        let cold = ColdState::new_memory(10).with_max_buffered_objects(1);
        put(&cold, "a", 1);
        put(&cold, "b", 2);
        assert_eq!(seconds(&cold, "a", 0, 10), [1]);
    }

    #[test]
    fn test_io_errors_fail_scan_and_replay_instead_of_skipping() {
        let dir = tempdir().unwrap();
        let target = FileScanTarget {
            path: dir.path().to_path_buf(),
            version: LogVersion::V2,
            len: 10,
        };
        assert!(matches!(
            scan_file(&target, "ns", "o", at(0), at(9), 10),
            Err(SpatioError::Io(_))
        ));

        let mut log =
            TrajectoryLog::open_file(&dir.path().join("t.log"), 0, SyncSettings::default())
                .unwrap();
        if let LogBackend::File { path, .. } = &mut log.backend {
            *path = dir.path().to_path_buf();
        }
        assert!(matches!(
            log.replay(0, &mut HashMap::new()),
            Err(SpatioError::Io(_))
        ));
    }

    #[test]
    fn test_non_utf8_line_is_counted_and_blocks_the_checkpoint() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        {
            let cold = file_cold(&path, 10);
            put(&cold, "o", 100);
            cold.close().unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(b"\xff\xfe\n");
        std::fs::write(&path, bytes).unwrap();

        let cold = file_cold(&path, 10);
        let recovered = cold.recover_current_locations().unwrap();
        assert!(recovered.contains_key("ns::o"));
        cold.write_checkpoint(&recovered).unwrap();
        assert!(!snapshot_path_for(&path).exists());
        assert!(
            cold.compact(at(0)).is_err(),
            "must not discard the bad line"
        );
    }

    #[test]
    fn test_compact_drops_old_history_and_keeps_latest_of_live_objects() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        let cold = file_cold(&path, 2);
        for s in [100, 200, 300] {
            put(&cold, "old", s);
        }
        for s in [100, 800, 900] {
            put(&cold, "new", s);
        }

        let stats = cold.compact(at(500)).unwrap();
        assert!(stats.bytes_after < stats.bytes_before);
        assert_eq!(stats.bytes_after, std::fs::metadata(&path).unwrap().len());
        assert_eq!(seconds(&cold, "new", 0, 10), [900, 800]);
        assert_eq!(seconds(&cold, "old", 0, 10), [300]);

        put(&cold, "new", 1000);
        cold.close().unwrap();
        let cold = reopen(&path, 2);
        let recovered = cold.recover_current_locations().unwrap();
        assert_eq!(recovered["ns::old"].metadata["s"], 300);
        assert_eq!(recovered["ns::new"].metadata["s"], 1000);
        assert_eq!(seconds(&cold, "new", 0, 10), [1000, 900, 800]);
    }

    #[test]
    fn test_compact_keeps_deletes_and_revivals() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        let cold = file_cold(&path, 10);
        put(&cold, "dead", 900);
        cold.append_tombstone("ns", "dead").unwrap();
        put(&cold, "gone", 100);
        cold.append_tombstone("ns", "gone").unwrap();
        put(&cold, "back", 900);
        cold.append_tombstone("ns", "back").unwrap();
        put(&cold, "back", 100);

        cold.compact(at(500)).unwrap();
        cold.close().unwrap();
        std::fs::remove_file(snapshot_path_for(&path)).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(!content.contains("|gone"), "nothing left for it to delete");

        let cold = reopen(&path, 10);
        let recovered = cold.recover_current_locations().unwrap();
        let mut live: Vec<_> = recovered.keys().map(String::as_str).collect();
        live.sort();
        assert_eq!(live, ["ns::back"]);
        assert_eq!(recovered["ns::back"].metadata["s"], 100);
        assert_eq!(seconds(&cold, "dead", 0, 10), [900], "recent history stays");
        assert!(seconds(&cold, "gone", 0, 10).is_empty());
    }

    #[test]
    fn test_compact_refuses_a_log_with_a_corrupt_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        let cold = file_cold(&path, 10);
        put(&cold, "o", 100);
        cold.flush().unwrap();

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "00000000|100000000|ns|bad|2.0|1.0|0.0|2|{{}}").unwrap();
        let before = std::fs::read(&path).unwrap();

        assert!(cold.compact(at(500)).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        put(&cold, "o", 200);
        assert_eq!(seconds(&cold, "o", 0, 10), [200, 100]);
    }

    #[test]
    fn test_compact_failure_leaves_log_usable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        let cold = file_cold(&path, 10);
        put(&cold, "o", 100);

        let mut blocker = path.as_os_str().to_os_string();
        blocker.push(".compact");
        std::fs::create_dir(&blocker).unwrap();
        assert!(cold.compact(at(500)).is_err());
        std::fs::remove_dir(&blocker).unwrap();

        put(&cold, "o", 200);
        cold.compact(at(150)).unwrap();
        assert_eq!(seconds(&cold, "o", 0, 10), [200]);
    }

    #[test]
    fn test_compact_upgrades_v1_log() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        std::fs::write(&path, "100000000|ns|o|2.000000|1.000000|0.000000|2|{}\n").unwrap();

        let cold = reopen(&path, 10);
        cold.compact(at(500)).unwrap();
        put(&cold, "o", 600);
        cold.close().unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.starts_with(LOG_HEADER_V2));
        assert_eq!(content.lines().count(), 3);
        let cold = reopen(&path, 10);
        assert_eq!(
            cold.recover_current_locations().unwrap()["ns::o"].metadata["s"],
            600
        );
    }

    #[test]
    fn test_compact_on_memory_is_a_no_op() {
        let cold = ColdState::new_memory(10);
        put(&cold, "o", 100);
        assert_eq!(cold.compact(at(500)).unwrap(), CompactionStats::default());
        assert_eq!(seconds(&cold, "o", 0, 10), [100]);
    }

    #[test]
    fn test_snapshot_without_horizon_falls_back_to_full_replay() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("traj.log");
        {
            let cold = file_cold(&path, 10);
            put(&cold, "o", 100);
            cold.close().unwrap();
        }
        let len = std::fs::metadata(&path).unwrap().len();
        std::fs::write(snapshot_path_for(&path), format!("#spatio-snap v1 {len}\n")).unwrap();

        let cold = file_cold(&path, 10);
        assert!(
            cold.recover_current_locations()
                .unwrap()
                .contains_key("ns::o")
        );
    }
}
