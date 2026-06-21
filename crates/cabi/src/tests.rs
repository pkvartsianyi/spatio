//! Smoke tests that drive the C ABI exactly as a foreign caller would: through
//! raw pointers, C strings, and the binary result buffer.

use super::*;
use std::ffi::{CStr, CString};
use std::ptr;

/// Copy a result buffer into a `Vec` and free the library allocation.
unsafe fn take_buffer(ptr: *mut u8, len: usize) -> Vec<u8> {
    if ptr.is_null() || len == 0 {
        return Vec::new();
    }
    let v = unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec();
    spatio_buffer_free(ptr, len);
    v
}

/// Minimal little-endian reader matching `wire.rs`.
struct Reader<'a> {
    b: &'a [u8],
    o: usize,
}

impl<'a> Reader<'a> {
    fn new(b: &'a [u8]) -> Self {
        Reader { b, o: 0 }
    }
    fn u32(&mut self) -> u32 {
        let v = u32::from_le_bytes(self.b[self.o..self.o + 4].try_into().unwrap());
        self.o += 4;
        v
    }
    fn f64(&mut self) -> f64 {
        let v = f64::from_le_bytes(self.b[self.o..self.o + 8].try_into().unwrap());
        self.o += 8;
        v
    }
    fn bytes(&mut self) -> &'a [u8] {
        let n = self.u32() as usize;
        let s = &self.b[self.o..self.o + n];
        self.o += n;
        s
    }
    fn str(&mut self) -> &'a str {
        std::str::from_utf8(self.bytes()).unwrap()
    }
}

#[test]
fn open_upsert_query_get_close() {
    let mut handle_ptr: *mut c_void = ptr::null_mut();
    let mut err: *mut c_char = ptr::null_mut();

    assert_eq!(
        spatio_open_memory(ptr::null(), &mut handle_ptr, &mut err),
        SPATIO_OK
    );
    assert!(!handle_ptr.is_null());

    let ns = CString::new("cities").unwrap();
    let nyc = CString::new("nyc").unwrap();
    let sf = CString::new("sf").unwrap();
    let meta = CString::new(r#"{"population":8000000}"#).unwrap();

    assert_eq!(
        spatio_upsert(
            handle_ptr,
            ns.as_ptr(),
            nyc.as_ptr(),
            -74.0060,
            40.7128,
            0.0,
            meta.as_ptr(),
            ptr::null(),
            &mut err,
        ),
        SPATIO_OK
    );
    assert_eq!(
        spatio_upsert(
            handle_ptr,
            ns.as_ptr(),
            sf.as_ptr(),
            -122.4194,
            37.7749,
            0.0,
            ptr::null(),
            ptr::null(),
            &mut err,
        ),
        SPATIO_OK
    );

    // get returns a 1-record location buffer: x y z ts, id, meta.
    let mut ptr_out: *mut u8 = ptr::null_mut();
    let mut len_out: usize = 0;
    assert_eq!(
        spatio_get(
            handle_ptr,
            ns.as_ptr(),
            nyc.as_ptr(),
            &mut ptr_out,
            &mut len_out,
            &mut err,
        ),
        SPATIO_OK
    );
    let buf = unsafe { take_buffer(ptr_out, len_out) };
    let mut r = Reader::new(&buf);
    assert_eq!(r.u32(), 1);
    let x = r.f64();
    let _y = r.f64();
    let _z = r.f64();
    let _ts = r.f64();
    let id = r.str();
    let meta_bytes = r.bytes();
    assert_eq!(id, "nyc");
    assert!((x - -74.0060).abs() < 1e-9);
    let meta_val: serde_json::Value = serde_json::from_slice(meta_bytes).unwrap();
    assert_eq!(meta_val["population"], 8000000);

    // missing object -> 0-record buffer.
    let paris = CString::new("paris").unwrap();
    let mut p2: *mut u8 = ptr::null_mut();
    let mut l2: usize = 0;
    assert_eq!(
        spatio_get(
            handle_ptr,
            ns.as_ptr(),
            paris.as_ptr(),
            &mut p2,
            &mut l2,
            &mut err,
        ),
        SPATIO_OK
    );
    let buf2 = unsafe { take_buffer(p2, l2) };
    assert_eq!(Reader::new(&buf2).u32(), 0);

    // query_radius around NYC (large radius) finds both, each with a distance.
    let mut p3: *mut u8 = ptr::null_mut();
    let mut l3: usize = 0;
    assert_eq!(
        spatio_query_radius(
            handle_ptr,
            ns.as_ptr(),
            -74.0060,
            40.7128,
            0.0,
            5_000_000.0,
            10,
            &mut p3,
            &mut l3,
            &mut err,
        ),
        SPATIO_OK
    );
    let buf3 = unsafe { take_buffer(p3, l3) };
    let mut r3 = Reader::new(&buf3);
    let count = r3.u32();
    assert_eq!(count, 2);
    for _ in 0..count {
        let _x = r3.f64();
        let _y = r3.f64();
        let _z = r3.f64();
        let _ts = r3.f64();
        let dist = r3.f64();
        let _id = r3.str();
        let _meta = r3.bytes();
        assert!(dist >= 0.0);
    }

    // stats: 7 packed u64; index 3 is hot_state_objects.
    let mut stats = [0u64; 7];
    assert_eq!(
        spatio_stats(handle_ptr, stats.as_mut_ptr(), &mut err),
        SPATIO_OK
    );
    assert_eq!(stats[3], 2);

    assert_eq!(spatio_close(handle_ptr, &mut err), SPATIO_OK);
}

#[test]
fn errors_are_reported() {
    let mut err: *mut c_char = ptr::null_mut();

    // null handle -> NULL_ARG with a message.
    let ns = CString::new("x").unwrap();
    let id = CString::new("y").unwrap();
    let code = spatio_delete(ptr::null_mut(), ns.as_ptr(), id.as_ptr(), &mut err);
    assert_eq!(code, SPATIO_ERR_NULL_ARG);
    let msg = unsafe { CStr::from_ptr(err) }
        .to_string_lossy()
        .into_owned();
    spatio_string_free(err);
    assert_eq!(msg, "null pointer argument");

    // query_near on a missing object -> NOT_FOUND.
    let mut handle_ptr: *mut c_void = ptr::null_mut();
    let mut err2: *mut c_char = ptr::null_mut();
    assert_eq!(
        spatio_open_memory(ptr::null(), &mut handle_ptr, &mut err2),
        SPATIO_OK
    );
    let mut p: *mut u8 = ptr::null_mut();
    let mut l: usize = 0;
    let ghost = CString::new("ghost").unwrap();
    let code = spatio_query_near(
        handle_ptr,
        ns.as_ptr(),
        ghost.as_ptr(),
        1000.0,
        10,
        &mut p,
        &mut l,
        &mut err2,
    );
    assert_eq!(code, SPATIO_ERR_NOT_FOUND);
    assert!(!err2.is_null());
    spatio_string_free(err2);
    assert_eq!(spatio_close(handle_ptr, &mut ptr::null_mut()), SPATIO_OK);
}
