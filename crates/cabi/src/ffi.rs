//! Low-level helpers for the C ABI: status codes, string marshaling, and
//! handle dereferencing. Kept separate from the `extern "C"` surface in
//! `lib.rs` so the boundary functions stay readable.

use spatio::{Spatio, SpatioError};
use std::ffi::{CStr, CString, c_char, c_void};

/// Status codes returned by every fallible C ABI function. `0` is success;
/// the Go side maps the rest onto typed error values.
pub const SPATIO_OK: i32 = 0;
pub const SPATIO_ERR_CLOSED: i32 = 1;
pub const SPATIO_ERR_SERIALIZATION: i32 = 2;
pub const SPATIO_ERR_INVALID_TIMESTAMP: i32 = 3;
pub const SPATIO_ERR_INVALID_INPUT: i32 = 4;
pub const SPATIO_ERR_NOT_FOUND: i32 = 5;
pub const SPATIO_ERR_IO: i32 = 6;
pub const SPATIO_ERR_OTHER: i32 = 7;
/// A required pointer argument was null.
pub const SPATIO_ERR_NULL_ARG: i32 = 8;
/// A `*const c_char` argument was not valid UTF-8.
pub const SPATIO_ERR_UTF8: i32 = 9;

/// Map a [`SpatioError`] onto its stable status code.
pub fn status_code(err: &SpatioError) -> i32 {
    match err {
        SpatioError::DatabaseClosed => SPATIO_ERR_CLOSED,
        SpatioError::SerializationError | SpatioError::SerializationErrorWithContext(_) => {
            SPATIO_ERR_SERIALIZATION
        }
        SpatioError::InvalidTimestamp => SPATIO_ERR_INVALID_TIMESTAMP,
        SpatioError::InvalidInput(_) => SPATIO_ERR_INVALID_INPUT,
        SpatioError::ObjectNotFound => SPATIO_ERR_NOT_FOUND,
        SpatioError::Io(_) => SPATIO_ERR_IO,
        _ => SPATIO_ERR_OTHER,
    }
}

/// Write a heap-allocated, null-terminated copy of `msg` into `*err` (if `err`
/// is non-null). The caller owns the string and must release it with
/// `spatio_string_free`. Interior NULs collapse the message to a null pointer.
///
/// # Safety
/// `err` must be null or a valid, writable `*mut *mut c_char`.
pub unsafe fn set_err(err: *mut *mut c_char, msg: &str) {
    if err.is_null() {
        return;
    }
    let ptr = match CString::new(msg) {
        Ok(c) => c.into_raw(),
        Err(_) => std::ptr::null_mut(),
    };
    unsafe { *err = ptr };
}

/// Record `e` into the error out-param and return its status code in one step.
///
/// # Safety
/// `err` must be null or a valid, writable `*mut *mut c_char`.
pub unsafe fn report(err: *mut *mut c_char, e: &SpatioError) -> i32 {
    unsafe { set_err(err, &e.to_string()) };
    status_code(e)
}

/// Borrow a required C string as `&str`.
///
/// # Safety
/// `ptr`, if non-null, must point to a valid null-terminated C string that
/// outlives the returned reference.
pub unsafe fn cstr<'a>(ptr: *const c_char) -> Result<&'a str, i32> {
    if ptr.is_null() {
        return Err(SPATIO_ERR_NULL_ARG);
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|_| SPATIO_ERR_UTF8)
}

/// Borrow an optional C string; a null pointer yields `None`.
///
/// # Safety
/// Same contract as [`cstr`] for non-null pointers.
pub unsafe fn cstr_opt<'a>(ptr: *const c_char) -> Result<Option<&'a str>, i32> {
    if ptr.is_null() {
        return Ok(None);
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(Some)
        .map_err(|_| SPATIO_ERR_UTF8)
}

/// Hand a `String` back to the caller through `out` as a heap C string the
/// caller frees with `spatio_string_free`.
///
/// # Safety
/// `out` must be null or a valid, writable `*mut *mut c_char`.
pub unsafe fn out_string(out: *mut *mut c_char, s: String) -> i32 {
    if out.is_null() {
        return SPATIO_ERR_NULL_ARG;
    }
    match CString::new(s) {
        Ok(c) => {
            unsafe { *out = c.into_raw() };
            SPATIO_OK
        }
        // A NUL inside serialized JSON would only arise from corrupt metadata.
        Err(_) => SPATIO_ERR_SERIALIZATION,
    }
}

/// Borrow the database behind an opaque handle.
///
/// # Safety
/// `handle` must be null or a pointer returned by `spatio_open*` that has not
/// been passed to `spatio_close`.
pub unsafe fn handle<'a>(handle: *mut c_void) -> Result<&'a Spatio, i32> {
    if handle.is_null() {
        return Err(SPATIO_ERR_NULL_ARG);
    }
    Ok(unsafe { &*(handle as *const Spatio) })
}

/// Serialize any value to JSON and emit it through `out`, mapping failures to a
/// serialization error.
///
/// # Safety
/// `out` must be null or a valid, writable `*mut *mut c_char`.
pub unsafe fn emit_json<T: serde::Serialize>(out: *mut *mut c_char, value: &T) -> i32 {
    match serde_json::to_string(value) {
        Ok(s) => unsafe { out_string(out, s) },
        Err(_) => SPATIO_ERR_SERIALIZATION,
    }
}
