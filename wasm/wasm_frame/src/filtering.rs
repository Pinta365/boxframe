//! Filtering operations: engine-based and direct filtering functions
//! 
//! This module provides functions for filtering data using boolean masks,
//! both through the engine (using registered series) and directly on arrays.

use wasm_bindgen::prelude::*;
use crate::core::ENGINE;

/// Filter float64 series using a boolean mask (1=true, 0=false)
#[wasm_bindgen]
pub fn engine_filter_f64(series_id: u32, mask: &[u8]) -> u32 {
    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) {
            (*ptr, *len)
        } else {
            (std::ptr::null_mut(), 0)
        }
    });
    if src_ptr.is_null() || src_len == 0 || mask.len() != src_len {
        return u32::MAX;
    }
    let mut out: Vec<f64> = Vec::new();
    unsafe {
        for i in 0..src_len {
            if mask[i] != 0 {
                out.push(*src_ptr.add(i));
            }
        }
    }
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let id = eng.next_series_id;
        eng.next_series_id = eng.next_series_id.wrapping_add(1);
        let len = out.len();
        let dst_ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(
                len * std::mem::size_of::<f64>(),
                std::mem::align_of::<f64>(),
            )
            .unwrap();
            let raw = std::alloc::alloc(layout) as *mut f64;
            if !raw.is_null() && len > 0 {
                std::ptr::copy_nonoverlapping(out.as_ptr(), raw, len);
            }
            raw
        };
        eng.series_store.insert(id, (dst_ptr, len));
        id
    })
}

/// High-performance filtering with boolean mask (using u8 array for WASM compatibility)
#[wasm_bindgen]
pub fn filter_f64(data: &[f64], mask: &[u8]) -> Vec<f64> {
    if data.len() != mask.len() {
        return Vec::new();
    }
    
    data.iter()
        .zip(mask.iter())
        .filter(|(_, &keep)| keep != 0)
        .map(|(&val, _)| val)
        .collect()
}
