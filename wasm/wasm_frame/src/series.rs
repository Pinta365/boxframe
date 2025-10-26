//! Series operations: accessors, conversions, and scalar operations
//! 
//! This module provides functions for accessing series data, converting
//! between formats, and performing scalar operations on registered series.

use wasm_bindgen::prelude::*;
use crate::core::ENGINE;

// Series pointer and length accessors
#[wasm_bindgen]
pub fn engine_series_ptr_f64(series_id: u32) -> usize {
    ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, _)) = eng.series_store.get(&series_id) {
            *ptr as usize
        } else {
            0
        }
    })
}

#[wasm_bindgen]
pub fn engine_series_len_f64(series_id: u32) -> usize {
    ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((_, len)) = eng.series_store.get(&series_id) {
            *len
        } else {
            0
        }
    })
}

#[wasm_bindgen]
pub fn engine_series_ptr_i32(series_id: u32) -> usize {
    ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, _)) = eng.series_store_i32.get(&series_id) {
            *ptr as usize
        } else { 0 }
    })
}

#[wasm_bindgen]
pub fn engine_series_len_i32(series_id: u32) -> usize {
    ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((_, len)) = eng.series_store_i32.get(&series_id) { *len } else { 0 }
    })
}

// Series conversion functions
#[wasm_bindgen]
pub fn engine_series_to_vec_f64(series_id: u32) -> Vec<f64> {
    ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) {
            if ptr.is_null() || *len == 0 {
                return Vec::new();
            }
            unsafe {
                let slice = std::slice::from_raw_parts(*ptr, *len);
                return slice.to_vec();
            }
        }
        Vec::new()
    })
}

#[wasm_bindgen]
pub fn engine_series_to_vec_i32(series_id: u32) -> Vec<i32> {
    ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store_i32.get(&series_id) {
            if ptr.is_null() || *len == 0 { return Vec::new(); }
            unsafe {
                let slice = std::slice::from_raw_parts(*ptr, *len);
                return slice.to_vec();
            }
        }
        Vec::new()
    })
}

// Scalar operations on registered f64 series
#[wasm_bindgen]
pub fn engine_series_sum_f64(series_id: u32) -> f64 {
    let (ptr, len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((p, l)) = eng.series_store.get(&series_id) { (*p, *l) } else { (std::ptr::null_mut(), 0) }
    });
    if ptr.is_null() || len == 0 { return 0.0; }
    let mut sum = 0.0;
    unsafe {
        for i in 0..len {
            let v = *ptr.add(i);
            if !v.is_nan() { sum += v; }
        }
    }
    sum
}

#[wasm_bindgen]
pub fn engine_series_mean_f64(series_id: u32) -> f64 {
    let (ptr, len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((p, l)) = eng.series_store.get(&series_id) { (*p, *l) } else { (std::ptr::null_mut(), 0) }
    });
    if ptr.is_null() || len == 0 { return f64::NAN; }
    let mut sum = 0.0; let mut cnt: usize = 0;
    unsafe {
        for i in 0..len {
            let v = *ptr.add(i);
            if !v.is_nan() { sum += v; cnt += 1; }
        }
    }
    if cnt == 0 { f64::NAN } else { sum / (cnt as f64) }
}

#[wasm_bindgen]
pub fn engine_series_std_f64(series_id: u32) -> f64 {
    let (ptr, len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((p, l)) = eng.series_store.get(&series_id) { (*p, *l) } else { (std::ptr::null_mut(), 0) }
    });
    if ptr.is_null() { return f64::NAN; }
    let mut sum = 0.0; let mut cnt: usize = 0;
    unsafe {
        for i in 0..len {
            let v = *ptr.add(i);
            if !v.is_nan() { sum += v; cnt += 1; }
        }
    }
    if cnt <= 1 { return f64::NAN; }
    let mean = sum / (cnt as f64);
    let mut sumsq = 0.0;
    unsafe {
        for i in 0..len {
            let v = *ptr.add(i);
            if !v.is_nan() { let d = v - mean; sumsq += d*d; }
        }
    }
    (sumsq / ((cnt - 1) as f64)).sqrt()
}

#[wasm_bindgen]
pub fn engine_series_min_f64(series_id: u32) -> f64 {
    let (ptr, len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((p, l)) = eng.series_store.get(&series_id) { (*p, *l) } else { (std::ptr::null_mut(), 0) }
    });
    if ptr.is_null() || len == 0 { return f64::NAN; }
    let mut m = f64::INFINITY; let mut seen = false;
    unsafe {
        for i in 0..len {
            let v = *ptr.add(i);
            if !v.is_nan() { if v < m { m = v; } seen = true; }
        }
    }
    if seen { m } else { f64::NAN }
}

#[wasm_bindgen]
pub fn engine_series_max_f64(series_id: u32) -> f64 {
    let (ptr, len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((p, l)) = eng.series_store.get(&series_id) { (*p, *l) } else { (std::ptr::null_mut(), 0) }
    });
    if ptr.is_null() || len == 0 { return f64::NAN; }
    let mut m = f64::NEG_INFINITY; let mut seen = false;
    unsafe {
        for i in 0..len {
            let v = *ptr.add(i);
            if !v.is_nan() { if v > m { m = v; } seen = true; }
        }
    }
    if seen { m } else { f64::NAN }
}

#[wasm_bindgen]
pub fn engine_series_count_f64(series_id: u32) -> u32 {
    let (ptr, len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((p, l)) = eng.series_store.get(&series_id) { (*p, *l) } else { (std::ptr::null_mut(), 0) }
    });
    if ptr.is_null() { return 0; }
    let mut cnt: u32 = 0;
    unsafe {
        for i in 0..len {
            let v = *ptr.add(i);
            if !v.is_nan() { cnt += 1; }
        }
    }
    cnt
}
