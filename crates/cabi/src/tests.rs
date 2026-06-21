//! Smoke tests that drive the C ABI exactly as a foreign caller would: through
//! raw pointers and C strings.

use super::*;
use std::ffi::{CStr, CString};
use std::ptr;

/// Take ownership of a string the library produced and return it as a `String`,
/// freeing the underlying allocation.
unsafe fn take_string(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let s = unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned();
    spatio_string_free(ptr);
    Some(s)
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

    // get returns flattened JSON with x/y/z/metadata/timestamp.
    let mut out: *mut c_char = ptr::null_mut();
    assert_eq!(
        spatio_get(handle_ptr, ns.as_ptr(), nyc.as_ptr(), &mut out, &mut err),
        SPATIO_OK
    );
    let got: serde_json::Value =
        serde_json::from_str(&unsafe { take_string(out) }.unwrap()).unwrap();
    assert_eq!(got["object_id"], "nyc");
    assert_eq!(got["metadata"]["population"], 8000000);
    assert!((got["x"].as_f64().unwrap() - -74.0060).abs() < 1e-9);

    // missing object -> null out_json, status OK.
    let missing = CString::new("paris").unwrap();
    let mut out2: *mut c_char = ptr::null_mut();
    assert_eq!(
        spatio_get(
            handle_ptr,
            ns.as_ptr(),
            missing.as_ptr(),
            &mut out2,
            &mut err
        ),
        SPATIO_OK
    );
    assert!(out2.is_null());

    // query_radius around NYC (very large radius) should find both cities.
    let mut out3: *mut c_char = ptr::null_mut();
    assert_eq!(
        spatio_query_radius(
            handle_ptr,
            ns.as_ptr(),
            -74.0060,
            40.7128,
            0.0,
            5_000_000.0,
            10,
            &mut out3,
            &mut err,
        ),
        SPATIO_OK
    );
    let neighbors: serde_json::Value =
        serde_json::from_str(&unsafe { take_string(out3) }.unwrap()).unwrap();
    assert_eq!(neighbors.as_array().unwrap().len(), 2);
    assert!(neighbors[0].get("distance").is_some());

    // stats reflects two hot-state objects.
    let mut out4: *mut c_char = ptr::null_mut();
    assert_eq!(spatio_stats(handle_ptr, &mut out4, &mut err), SPATIO_OK);
    let stats: serde_json::Value =
        serde_json::from_str(&unsafe { take_string(out4) }.unwrap()).unwrap();
    assert_eq!(stats["hot_state_objects"], 2);

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
    assert_eq!(
        unsafe { take_string(err) }.as_deref(),
        Some("null pointer argument")
    );

    // query_near on a missing object -> NOT_FOUND.
    let mut handle_ptr: *mut c_void = ptr::null_mut();
    let mut err2: *mut c_char = ptr::null_mut();
    assert_eq!(
        spatio_open_memory(ptr::null(), &mut handle_ptr, &mut err2),
        SPATIO_OK
    );
    let mut out: *mut c_char = ptr::null_mut();
    let ghost = CString::new("ghost").unwrap();
    let code = spatio_query_near(
        handle_ptr,
        ns.as_ptr(),
        ghost.as_ptr(),
        1000.0,
        10,
        &mut out,
        &mut err2,
    );
    assert_eq!(code, SPATIO_ERR_NOT_FOUND);
    assert!(unsafe { take_string(err2) }.is_some());
    spatio_close(handle_ptr, &mut ptr::null_mut());
}
