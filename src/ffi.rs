//! C-compatible FFI for the Spatio database.
//!
//! These functions provide a minimal handle-based interface that can be
//! consumed from C or other languages that interoperate with a `cdylib`.
//!
//! The API follows a status-code pattern: `0` indicates success, negative
//! numbers indicate errors, and positive numbers are reserved for
//! non-error states such as “not found”.

use crate::{Config, Spatio};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_uchar};
use std::ptr;

/// Generic success status.
const SPATIO_OK: c_int = 0;
/// Returned when the provided arguments are null or otherwise invalid.
const SPATIO_ERR_INVALID_ARGUMENT: c_int = -1;
/// Returned when an internal database operation fails.
const SPATIO_ERR_OPERATION_FAILED: c_int = -2;
/// Returned when a key lookup succeeds but the key is absent.
pub const SPATIO_STATUS_NOT_FOUND: c_int = 1;

/// Opaque database handle exposed to C callers.
#[repr(C)]
pub struct SpatioHandle {
    db: Spatio,
}

/// Buffer returned to callers for value data.
#[repr(C)]
pub struct SpatioBuffer {
    pub data: *mut c_uchar,
    pub len: usize,
}

/// # Safety
/// `path` must point to a valid, null-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spatio_open(path: *const c_char) -> *mut SpatioHandle {
    if path.is_null() {
        return ptr::null_mut();
    }

    let path_cstr = unsafe { CStr::from_ptr(path) };
    let path_str = match path_cstr.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    match Spatio::open(path_str) {
        Ok(db) => Box::into_raw(Box::new(SpatioHandle { db })),
        Err(_) => ptr::null_mut(),
    }
}

/// # Safety
/// `path` and (if non-null) `config_json` must point to valid, null-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spatio_open_with_config(
    path: *const c_char,
    config_json: *const c_char,
) -> *mut SpatioHandle {
    if path.is_null() {
        return ptr::null_mut();
    }

    let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let config = if config_json.is_null() {
        Config::default()
    } else {
        match unsafe { CStr::from_ptr(config_json) }
            .to_str()
            .ok()
            .and_then(|json| Config::from_json(json).ok())
        {
            Some(cfg) => cfg,
            None => return ptr::null_mut(),
        }
    };

    match Spatio::open_with_config(path_str, config) {
        Ok(db) => Box::into_raw(Box::new(SpatioHandle { db })),
        Err(_) => ptr::null_mut(),
    }
}

/// # Safety
/// `handle` must be a valid pointer obtained from `spatio_open` or
/// `spatio_open_with_config`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spatio_close(handle: *mut SpatioHandle) {
    if handle.is_null() {
        return;
    }

    // Reconstruct the Box so it is dropped at the end of the scope.
    let mut boxed = unsafe { Box::from_raw(handle) };
    let _ = boxed.db.close();
}

/// # Safety
/// `handle` must be valid. `key` must be a null-terminated UTF-8 string. If
/// `value_len` is non-zero, `value` must point to a buffer of at least that
/// length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spatio_insert(
    handle: *mut SpatioHandle,
    key: *const c_char,
    value: *const c_uchar,
    value_len: usize,
) -> c_int {
    if handle.is_null() || key.is_null() || (value.is_null() && value_len != 0) {
        return SPATIO_ERR_INVALID_ARGUMENT;
    }

    let db = unsafe { &mut *handle };
    let key_str = match unsafe { CStr::from_ptr(key) }.to_str() {
        Ok(s) => s,
        Err(_) => return SPATIO_ERR_INVALID_ARGUMENT,
    };

    let value_slice = unsafe { std::slice::from_raw_parts(value, value_len) };

    match db.db.insert(key_str, value_slice, None) {
        Ok(_) => SPATIO_OK,
        Err(_) => SPATIO_ERR_OPERATION_FAILED,
    }
}

/// # Safety
/// `handle` and `out_buffer` must be valid pointers. `key` must be a
/// null-terminated UTF-8 string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spatio_get(
    handle: *mut SpatioHandle,
    key: *const c_char,
    out_buffer: *mut SpatioBuffer,
) -> c_int {
    if handle.is_null() || key.is_null() || out_buffer.is_null() {
        return SPATIO_ERR_INVALID_ARGUMENT;
    }

    let db = unsafe { &mut *handle };
    let key_bytes = unsafe { CStr::from_ptr(key) }.to_bytes();

    let result = match db.db.get(key_bytes) {
        Ok(opt) => opt,
        Err(_) => return SPATIO_ERR_OPERATION_FAILED,
    };

    match result {
        Some(bytes) => {
            let mut vec = bytes.to_vec();
            vec.shrink_to_fit();
            let len = vec.len();
            let ptr = vec.as_mut_ptr();
            std::mem::forget(vec);

            unsafe {
                (*out_buffer).data = ptr;
                (*out_buffer).len = len;
            }

            SPATIO_OK
        }
        None => SPATIO_STATUS_NOT_FOUND,
    }
}

/// # Safety
/// `buffer` must be a value previously produced by `spatio_get` and not yet
/// freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spatio_free_buffer(buffer: SpatioBuffer) {
    if buffer.data.is_null() {
        return;
    }
    unsafe {
        Vec::from_raw_parts(buffer.data, buffer.len, buffer.len);
    }
}

/// # Safety
/// `message_out` must be a valid pointer to receive the error message pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spatio_last_error_message(message_out: *mut *const c_char) -> c_int {
    if message_out.is_null() {
        return SPATIO_ERR_INVALID_ARGUMENT;
    }
    unsafe {
        *message_out = ptr::null();
    }
    SPATIO_OK
}
