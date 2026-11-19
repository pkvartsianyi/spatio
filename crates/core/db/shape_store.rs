//! Durable storage for shapes: an append-only file beside the trajectory log
//! (`<log>.shapes`), one CRC-checked record per set or delete. Shapes change
//! rarely, so every write is synced before it is applied.

use super::cold_state::{crc32, sync_parent_dir};
use crate::error::Result;
use parking_lot::Mutex;
use spatio_types::shape::Shape;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const HEADER: &str = "#spatio-shapes v1";

/// Rewrite the file on open once dead records outnumber live ones by this much.
const COMPACT_SLACK: usize = 64;

pub(crate) type Loaded = HashMap<(String, String), Shape>;

pub(crate) struct ShapeStore {
    file: Option<Mutex<Writer>>,
}

struct Writer {
    path: PathBuf,
    file: File,
}

impl ShapeStore {
    pub fn memory() -> Self {
        Self { file: None }
    }

    /// Open (or create) the shape file for `log_path` and return its shapes.
    pub fn open(log_path: &Path) -> Result<(Self, Loaded)> {
        let mut path = log_path.as_os_str().to_os_string();
        path.push(".shapes");
        let path = PathBuf::from(path);

        let (shapes, records, torn) = match std::fs::read_to_string(&path) {
            Ok(content) => {
                let (shapes, records) = replay(&content);
                (
                    shapes,
                    records,
                    !content.is_empty() && !content.ends_with('\n'),
                )
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (Loaded::new(), 0, false),
            Err(e) => return Err(e.into()),
        };

        // A torn final line would swallow the next appended record.
        if torn || records == 0 || records > shapes.len() * 2 + COMPACT_SLACK {
            rewrite(&path, &shapes)?;
        }
        let file = OpenOptions::new().append(true).open(&path)?;
        Ok((
            Self {
                file: Some(Mutex::new(Writer { path, file })),
            },
            shapes,
        ))
    }

    pub fn set(&self, namespace: &str, id: &str, shape: &Shape) -> Result<()> {
        let json = to_json(shape)?;
        self.append(&format!("S|{namespace}|{id}|{json}"))
    }

    pub fn delete(&self, namespace: &str, id: &str) -> Result<()> {
        self.append(&format!("D|{namespace}|{id}"))
    }

    /// Replace the file with exactly `shapes`.
    pub fn compact<'a>(
        &self,
        shapes: impl Iterator<Item = (&'a str, &'a str, &'a Shape)>,
    ) -> Result<()> {
        let Some(w) = &self.file else {
            return Ok(());
        };
        let mut w = w.lock();
        let map: Loaded = shapes
            .map(|(ns, id, s)| ((ns.to_string(), id.to_string()), s.clone()))
            .collect();
        rewrite(&w.path, &map)?;
        w.file = OpenOptions::new().append(true).open(&w.path)?;
        Ok(())
    }

    fn append(&self, body: &str) -> Result<()> {
        let Some(w) = &self.file else {
            return Ok(());
        };
        let mut w = w.lock();
        w.file.write_all(line(body).as_bytes())?;
        w.file.sync_data()?;
        Ok(())
    }
}

fn to_json(shape: &Shape) -> Result<String> {
    serde_json::to_string(shape)
        .map_err(|e| crate::error::SpatioError::SerializationErrorWithContext(e.to_string()))
}

fn line(body: &str) -> String {
    format!("{:08x}|{body}\n", crc32(body.as_bytes()))
}

/// Apply every intact record in order. Returns the shapes and how many
/// records the file held.
fn replay(content: &str) -> (Loaded, usize) {
    let mut shapes = Loaded::new();
    let mut records = 0;
    for raw in content.lines() {
        if raw.is_empty() || raw.starts_with('#') {
            continue;
        }
        records += 1;
        let Some(body) = raw.split_once('|').and_then(|(crc, body)| {
            (u32::from_str_radix(crc, 16).ok()? == crc32(body.as_bytes())).then_some(body)
        }) else {
            log::warn!("Skipping shape record with CRC mismatch (corrupt or torn write)");
            continue;
        };
        let mut parts = body.splitn(4, '|');
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some("S"), Some(ns), Some(id), Some(json)) => match serde_json::from_str(json) {
                Ok(shape) => {
                    shapes.insert((ns.to_string(), id.to_string()), shape);
                }
                Err(e) => log::warn!("Skipping unreadable shape {ns}/{id}: {e}"),
            },
            (Some("D"), Some(ns), Some(id), None) => {
                shapes.remove(&(ns.to_string(), id.to_string()));
            }
            _ => log::warn!("Skipping malformed shape record"),
        }
    }
    (shapes, records)
}

/// Atomically replace the file: temp file → fsync → rename → fsync dir.
fn rewrite(path: &Path, shapes: &Loaded) -> Result<()> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut w = BufWriter::new(File::create(&tmp)?);
        writeln!(w, "{HEADER}")?;
        let mut keys: Vec<_> = shapes.keys().collect();
        keys.sort();
        for key in keys {
            let json = to_json(&shapes[key])?;
            w.write_all(line(&format!("S|{}|{}|{json}", key.0, key.1)).as_bytes())?;
        }
        w.flush()?;
        w.get_ref().sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    sync_parent_dir(path);
    Ok(())
}
