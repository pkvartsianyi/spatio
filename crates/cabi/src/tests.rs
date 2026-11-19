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

    let mut stats = [0u64; 5];
    assert_eq!(
        spatio_stats(handle_ptr, stats.as_mut_ptr(), &mut err),
        SPATIO_OK
    );
    assert_eq!(stats[1], 2);

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

#[test]
fn trajectory_records_carry_altitude() {
    let mut h: *mut c_void = ptr::null_mut();
    let mut err: *mut c_char = ptr::null_mut();
    assert_eq!(spatio_open_memory(ptr::null(), &mut h, &mut err), SPATIO_OK);
    let ns = CString::new("air").unwrap();
    let id = CString::new("drone").unwrap();
    let opts = CString::new(r#"{"timestamp":1000.0}"#).unwrap();
    assert_eq!(
        spatio_upsert(
            h,
            ns.as_ptr(),
            id.as_ptr(),
            1.0,
            2.0,
            150.0,
            ptr::null(),
            opts.as_ptr(),
            &mut err
        ),
        SPATIO_OK
    );
    let (mut p, mut l) = (ptr::null_mut(), 0usize);
    assert_eq!(
        spatio_query_trajectory(
            h,
            ns.as_ptr(),
            id.as_ptr(),
            0.0,
            2000.0,
            10,
            &mut p,
            &mut l,
            &mut err
        ),
        SPATIO_OK
    );
    let buf = unsafe { take_buffer(p, l) };
    let mut r = Reader::new(&buf);
    assert_eq!(r.u32(), 1);
    assert_eq!(
        (r.f64(), r.f64(), r.f64(), r.f64()),
        (1.0, 2.0, 150.0, 1000.0)
    );
    assert!(r.bytes().is_empty());
    assert_eq!(spatio_wire_version(), 3);
    assert_eq!(spatio_close(h, &mut err), SPATIO_OK);
}

#[test]
fn panics_become_error_codes() {
    let mut err: *mut c_char = ptr::null_mut();
    assert_eq!(guard(&mut err, || panic!("boom")), SPATIO_ERR_OTHER);
    assert!(!err.is_null());
    spatio_string_free(err);
}

unsafe fn take_err(err: &mut *mut c_char) -> String {
    let msg = unsafe { CStr::from_ptr(*err) }
        .to_string_lossy()
        .into_owned();
    spatio_string_free(*err);
    *err = ptr::null_mut();
    msg
}

fn set_shape(h: *mut c_void, ns: &CString, id: &str, json: &str) -> i32 {
    let id = CString::new(id).unwrap();
    let json = CString::new(json).unwrap();
    let mut err = ptr::null_mut();
    let code = spatio_set_shape(h, ns.as_ptr(), id.as_ptr(), json.as_ptr(), &mut err);
    spatio_string_free(err);
    code
}

fn get_shape(h: *mut c_void, ns: &CString, id: &str) -> Option<serde_json::Value> {
    let id = CString::new(id).unwrap();
    let mut out: *mut c_char = ptr::null_mut();
    let mut err = ptr::null_mut();
    assert_eq!(
        spatio_get_shape(h, ns.as_ptr(), id.as_ptr(), &mut out, &mut err),
        SPATIO_OK
    );
    if out.is_null() {
        return None;
    }
    let s = unsafe { CStr::from_ptr(out) }.to_str().unwrap().to_owned();
    spatio_string_free(out);
    Some(serde_json::from_str(&s).unwrap())
}

#[test]
fn shapes_roundtrip_and_query() {
    let mut h: *mut c_void = ptr::null_mut();
    let mut err: *mut c_char = ptr::null_mut();
    assert_eq!(spatio_open_memory(ptr::null(), &mut h, &mut err), SPATIO_OK);
    let ns = CString::new("zones").unwrap();

    let square =
        r#"{"type":"polygon","exterior":[[0.0,0.0],[1.0,0.0],[1.0,1.0],[0.0,1.0],[0.0,0.0]]}"#;
    let circle = r#"{"type":"circle","center":[0.5,0.5],"radius_m":1000.0}"#;
    let corridor = r#"{"type":"corridor","line":[[2.0,0.0],[2.0,1.0]],"half_width_m":50.0}"#;
    assert_eq!(set_shape(h, &ns, "square", square), SPATIO_OK);
    assert_eq!(set_shape(h, &ns, "circle", circle), SPATIO_OK);
    assert_eq!(set_shape(h, &ns, "road", corridor), SPATIO_OK);

    let got = get_shape(h, &ns, "circle").unwrap();
    assert_eq!(
        got,
        serde_json::from_str::<serde_json::Value>(circle).unwrap()
    );
    assert_eq!(get_shape(h, &ns, "road").unwrap()["half_width_m"], 50.0);
    assert!(get_shape(h, &ns, "missing").is_none());

    let (mut p, mut l) = (ptr::null_mut(), 0usize);
    assert_eq!(
        spatio_list_shapes(h, ns.as_ptr(), &mut p, &mut l, &mut err),
        SPATIO_OK
    );
    let buf = unsafe { take_buffer(p, l) };
    let mut r = Reader::new(&buf);
    assert_eq!(r.u32(), 3);
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(r.str().to_owned());
        let v: serde_json::Value = serde_json::from_slice(r.bytes()).unwrap();
        assert!(v["type"].is_string());
    }
    assert_eq!(ids, ["circle", "road", "square"]);

    let (mut p, mut l) = (ptr::null_mut(), 0usize);
    assert_eq!(
        spatio_shapes_containing(h, ns.as_ptr(), 0.5, 0.5, 0.0, 10, &mut p, &mut l, &mut err),
        SPATIO_OK
    );
    let buf = unsafe { take_buffer(p, l) };
    let mut r = Reader::new(&buf);
    assert_eq!(r.u32(), 2);
    assert_eq!((r.str(), r.str()), ("circle", "square"));

    let (mut p, mut l) = (ptr::null_mut(), 0usize);
    assert_eq!(
        spatio_shapes_within(
            h,
            ns.as_ptr(),
            2.001,
            0.5,
            0.0,
            200.0,
            10,
            &mut p,
            &mut l,
            &mut err
        ),
        SPATIO_OK
    );
    let buf = unsafe { take_buffer(p, l) };
    let mut r = Reader::new(&buf);
    assert_eq!(r.u32(), 1);
    let dist = r.f64();
    assert_eq!(r.str(), "road");
    assert!((dist - 61.2).abs() < 1.0, "{dist}");

    let id = CString::new("road").unwrap();
    let mut deleted = false;
    assert_eq!(
        spatio_delete_shape(h, ns.as_ptr(), id.as_ptr(), &mut deleted, &mut err),
        SPATIO_OK
    );
    assert!(deleted);
    assert_eq!(
        spatio_delete_shape(h, ns.as_ptr(), id.as_ptr(), &mut deleted, &mut err),
        SPATIO_OK
    );
    assert!(!deleted);
    assert!(get_shape(h, &ns, "road").is_none());

    assert_eq!(spatio_close(h, &mut err), SPATIO_OK);
}

#[test]
fn shape_errors() {
    let mut h: *mut c_void = ptr::null_mut();
    let mut err: *mut c_char = ptr::null_mut();
    assert_eq!(spatio_open_memory(ptr::null(), &mut h, &mut err), SPATIO_OK);
    let ns = CString::new("zones").unwrap();
    let id = CString::new("z").unwrap();

    assert_eq!(set_shape(h, &ns, "z", "not json"), SPATIO_ERR_INVALID_INPUT);
    assert_eq!(
        set_shape(h, &ns, "z", r#"{"type":"hexagon"}"#),
        SPATIO_ERR_INVALID_INPUT
    );
    assert_eq!(
        set_shape(
            h,
            &ns,
            "z",
            r#"{"type":"circle","center":[0.0,0.0],"radius_m":-1.0}"#
        ),
        SPATIO_ERR_INVALID_INPUT
    );
    assert_eq!(
        set_shape(
            h,
            &ns,
            "z",
            r#"{"type":"corridor","line":[[0.0,0.0]],"half_width_m":1.0}"#
        ),
        SPATIO_ERR_INVALID_INPUT
    );
    assert_eq!(
        set_shape(
            h,
            &ns,
            "z",
            r#"{"type":"circle","center":[200.0,0.0],"radius_m":1.0}"#
        ),
        SPATIO_ERR_INVALID_INPUT
    );

    let json = CString::new(r#"{"type":"circle","center":[0.0,0.0],"radius_m":1.0}"#).unwrap();
    assert_eq!(
        spatio_set_shape(h, ns.as_ptr(), id.as_ptr(), ptr::null(), &mut err),
        SPATIO_ERR_NULL_ARG
    );
    assert_eq!(unsafe { take_err(&mut err) }, "null pointer argument");
    assert_eq!(
        spatio_set_shape(
            ptr::null_mut(),
            ns.as_ptr(),
            id.as_ptr(),
            json.as_ptr(),
            &mut err
        ),
        SPATIO_ERR_NULL_ARG
    );
    unsafe { take_err(&mut err) };
    assert_eq!(
        spatio_get_shape(h, ns.as_ptr(), id.as_ptr(), ptr::null_mut(), &mut err),
        SPATIO_ERR_NULL_ARG
    );
    unsafe { take_err(&mut err) };

    let (mut p, mut l) = (ptr::null_mut(), 0usize);
    assert_eq!(
        spatio_shapes_within(
            h,
            ns.as_ptr(),
            0.0,
            0.0,
            0.0,
            -5.0,
            10,
            &mut p,
            &mut l,
            &mut err
        ),
        SPATIO_ERR_INVALID_INPUT
    );
    assert!(unsafe { take_err(&mut err) }.contains("non-negative"));
    assert_eq!(
        spatio_shapes_containing(h, ns.as_ptr(), 0.0, 95.0, 0.0, 10, &mut p, &mut l, &mut err),
        SPATIO_ERR_INVALID_INPUT
    );
    unsafe { take_err(&mut err) };
    assert_eq!(
        spatio_list_shapes(h, ns.as_ptr(), ptr::null_mut(), &mut l, &mut err),
        SPATIO_ERR_NULL_ARG
    );

    let (mut p, mut l) = (ptr::null_mut(), 0usize);
    assert_eq!(
        spatio_list_shapes(h, ns.as_ptr(), &mut p, &mut l, &mut err),
        SPATIO_OK
    );
    assert_eq!(Reader::new(&unsafe { take_buffer(p, l) }).u32(), 0);

    assert_eq!(spatio_close(h, &mut err), SPATIO_OK);
}
