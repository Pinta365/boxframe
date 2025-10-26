//! Sorting operations: engine-based and direct sorting functions
//! 
//! This module provides functions for sorting data, both through the engine
//! (using registered series) and directly on arrays.

use std::cmp::Ordering;
use wasm_bindgen::prelude::*;
use crate::core::ENGINE;

// Engine-based sorting functions

/// Sort values (float64) ascending/descending, nulls last flag applies to NaN
#[wasm_bindgen]
pub fn engine_sort_values_f64(series_id: u32, ascending: u8, nulls_last: u8) -> u32 {
    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) {
            (*ptr, *len)
        } else {
            (std::ptr::null_mut(), 0)
        }
    });
    if src_ptr.is_null() || src_len == 0 {
        return u32::MAX;
    }

    // Copy into a temporary Vec for sorting (we avoid reallocating inside engine storage)
    let mut values: Vec<f64> = Vec::with_capacity(src_len);
    unsafe {
        for i in 0..src_len {
            values.push(*src_ptr.add(i));
        }
    }

    let idx = sort_single_column_f64(&values, ascending != 0, nulls_last != 0);
    let mut sorted: Vec<f64> = Vec::with_capacity(idx.len());
    for i in idx {
        sorted.push(values[i]);
    }

    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let id = eng.next_series_id;
        eng.next_series_id = eng.next_series_id.wrapping_add(1);
        let len = sorted.len();
        let dst_ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(
                len * std::mem::size_of::<f64>(),
                std::mem::align_of::<f64>(),
            )
            .unwrap();
            let raw = std::alloc::alloc(layout) as *mut f64;
            if !raw.is_null() && len > 0 {
                std::ptr::copy_nonoverlapping(sorted.as_ptr(), raw, len);
            }
            raw
        };
        eng.series_store.insert(id, (dst_ptr, len));
        id
    })
}

/// Return sort indices (float64) for a registered series (no materialization)
#[wasm_bindgen]
pub fn engine_sort_indices_f64(series_id: u32, ascending: u8, nulls_last: u8) -> Box<[u32]> {
    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) { (*ptr, *len) } else { (std::ptr::null_mut(), 0) }
    });
    if src_ptr.is_null() || src_len == 0 {
        return Box::new([]);
    }

    // Copy into temporary Vec for sorting
    let mut values: Vec<f64> = Vec::with_capacity(src_len);
    unsafe {
        for i in 0..src_len { values.push(*src_ptr.add(i)); }
    }
    let idx = sort_single_column_f64(&values, ascending != 0, nulls_last != 0);
    let idx_u32: Vec<u32> = idx.into_iter().map(|i| i as u32).collect();
    idx_u32.into_boxed_slice()
}

/// Return sort indices by two registered f64 series (provided as two series ids)
#[wasm_bindgen]
pub fn engine_sort_two_columns_indices_f64(series1_id: u32, series2_id: u32, asc1: u8, asc2: u8, nulls_last: u8) -> Box<[u32]> {
    let (ptr1, len1) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((p, l)) = eng.series_store.get(&series1_id) { (*p, *l) } else { (std::ptr::null_mut(), 0) }
    });
    let (ptr2, len2) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((p, l)) = eng.series_store.get(&series2_id) { (*p, *l) } else { (std::ptr::null_mut(), 0) }
    });
    if ptr1.is_null() || ptr2.is_null() || len1 == 0 || len1 != len2 { return Box::new([]); }
    let mut col1: Vec<f64> = Vec::with_capacity(len1);
    let mut col2: Vec<f64> = Vec::with_capacity(len1);
    unsafe {
        for i in 0..len1 { col1.push(*ptr1.add(i)); col2.push(*ptr2.add(i)); }
    }
    let idx = sort_two_columns_f64(&col1, &col2, asc1, asc2, nulls_last);
    let idx_u32: Vec<u32> = idx.into_iter().map(|i| i as u32).collect();
    idx_u32.into_boxed_slice()
}

/// Return sort indices (int32) for a registered i32 series
#[wasm_bindgen]
pub fn engine_sort_indices_i32(series_id: u32, ascending: u8, nulls_last: u8) -> Box<[u32]> {
    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store_i32.get(&series_id) { (*ptr, *len) } else { (std::ptr::null_mut(), 0) }
    });
    if src_ptr.is_null() || src_len == 0 { return Box::new([]); }
    let mut values: Vec<i32> = Vec::with_capacity(src_len);
    unsafe { for i in 0..src_len { values.push(*src_ptr.add(i)); } }
    let idx = sort_single_column_i32(&values, ascending != 0, nulls_last != 0);
    let idx_u32: Vec<u32> = idx.into_iter().map(|i| i as u32).collect();
    idx_u32.into_boxed_slice()
}

/// Return sort indices by two registered i32 series
#[wasm_bindgen]
pub fn engine_sort_two_columns_indices_i32(series1_id: u32, series2_id: u32, asc1: u8, asc2: u8, nulls_last: u8) -> Box<[u32]> {
    let (ptr1, len1) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((p, l)) = eng.series_store_i32.get(&series1_id) { (*p, *l) } else { (std::ptr::null_mut(), 0) }
    });
    let (ptr2, len2) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((p, l)) = eng.series_store_i32.get(&series2_id) { (*p, *l) } else { (std::ptr::null_mut(), 0) }
    });
    if ptr1.is_null() || ptr2.is_null() || len1 == 0 || len1 != len2 { return Box::new([]); }
    let mut col1: Vec<i32> = Vec::with_capacity(len1);
    let mut col2: Vec<i32> = Vec::with_capacity(len1);
    unsafe { for i in 0..len1 { col1.push(*ptr1.add(i)); col2.push(*ptr2.add(i)); } }
    let idx = sort_two_columns_i32(&col1, &col2, asc1, asc2, nulls_last);
    let idx_u32: Vec<u32> = idx.into_iter().map(|i| i as u32).collect();
    idx_u32.into_boxed_slice()
}

// Direct sorting functions

/// Sort indices by two float64 columns (most common multi-column case)
/// 
/// # Arguments
/// * `col1` - First column to sort by
/// * `col2` - Second column to sort by
/// * `asc1` - Whether first column should be ascending (1) or descending (0)
/// * `asc2` - Whether second column should be ascending (1) or descending (0)
/// * `nulls_last` - Whether to put null values at the end (1) or beginning (0)
/// 
/// # Returns
/// * Array of indices sorted according to the multi-column criteria
#[wasm_bindgen]
pub fn sort_two_columns_f64(
    col1: &[f64], 
    col2: &[f64], 
    asc1: u8, 
    asc2: u8, 
    nulls_last: u8
) -> Vec<usize> {
    if col1.len() != col2.len() {
        return vec![];
    }
    
    let num_rows = col1.len();
    let mut indices: Vec<usize> = (0..num_rows).collect();
    let nulls_last_bool = nulls_last == 1;
    
    // Create a stable sort comparator
    indices.sort_by(|&a, &b| {
        // Compare first column
        let val_a1 = col1[a];
        let val_b1 = col1[b];
        let a1_is_nan = val_a1.is_nan();
        let b1_is_nan = val_b1.is_nan();
        
        let comparison1 = match (a1_is_nan, b1_is_nan) {
            (true, true) => Ordering::Equal,
            (true, false) => if nulls_last_bool { Ordering::Greater } else { Ordering::Less },
            (false, true) => if nulls_last_bool { Ordering::Less } else { Ordering::Greater },
            (false, false) => {
                if val_a1 < val_b1 {
                    Ordering::Less
                } else if val_a1 > val_b1 {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        };
        
        let result1 = if asc1 == 1 {
            comparison1
        } else {
            comparison1.reverse()
        };
        
        if result1 != Ordering::Equal {
            return result1;
        }
        
        // Compare second column if first column is equal
        let val_a2 = col2[a];
        let val_b2 = col2[b];
        let a2_is_nan = val_a2.is_nan();
        let b2_is_nan = val_b2.is_nan();
        
        let comparison2 = match (a2_is_nan, b2_is_nan) {
            (true, true) => Ordering::Equal,
            (true, false) => if nulls_last_bool { Ordering::Greater } else { Ordering::Less },
            (false, true) => if nulls_last_bool { Ordering::Less } else { Ordering::Greater },
            (false, false) => {
                if val_a2 < val_b2 {
                    Ordering::Less
                } else if val_a2 > val_b2 {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        };
        
        if asc2 == 1 {
            comparison2
        } else {
            comparison2.reverse()
        }
    });
    
    indices
}

/// Sort indices by two int32 columns
/// 
/// # Arguments
/// * `col1` - First column to sort by
/// * `col2` - Second column to sort by
/// * `asc1` - Whether first column should be ascending (1) or descending (0)
/// * `asc2` - Whether second column should be ascending (1) or descending (0)
/// * `nulls_last` - Whether to put null values at the end (1) or beginning (0)
/// 
/// # Returns
/// * Array of indices sorted according to the multi-column criteria
#[wasm_bindgen]
pub fn sort_two_columns_i32(
    col1: &[i32], 
    col2: &[i32], 
    asc1: u8, 
    asc2: u8, 
    nulls_last: u8
) -> Vec<usize> {
    if col1.len() != col2.len() {
        return vec![];
    }
    
    let num_rows = col1.len();
    let mut indices: Vec<usize> = (0..num_rows).collect();
    let nulls_last_bool = nulls_last == 1;
    
    // Create a stable sort comparator
    indices.sort_by(|&a, &b| {
        // Compare first column
        let val_a1 = col1[a];
        let val_b1 = col1[b];
        let a1_is_null = val_a1 == i32::MIN;
        let b1_is_null = val_b1 == i32::MIN;
        
        let comparison1 = match (a1_is_null, b1_is_null) {
            (true, true) => Ordering::Equal,
            (true, false) => if nulls_last_bool { Ordering::Greater } else { Ordering::Less },
            (false, true) => if nulls_last_bool { Ordering::Less } else { Ordering::Greater },
            (false, false) => val_a1.cmp(&val_b1)
        };
        
        let result1 = if asc1 == 1 {
            comparison1
        } else {
            comparison1.reverse()
        };
        
        if result1 != Ordering::Equal {
            return result1;
        }
        
        // Compare second column if first column is equal
        let val_a2 = col2[a];
        let val_b2 = col2[b];
        let a2_is_null = val_a2 == i32::MIN;
        let b2_is_null = val_b2 == i32::MIN;
        
        let comparison2 = match (a2_is_null, b2_is_null) {
            (true, true) => Ordering::Equal,
            (true, false) => if nulls_last_bool { Ordering::Greater } else { Ordering::Less },
            (false, true) => if nulls_last_bool { Ordering::Less } else { Ordering::Greater },
            (false, false) => val_a2.cmp(&val_b2)
        };
        
        if asc2 == 1 {
            comparison2
        } else {
            comparison2.reverse()
        }
    });
    
    indices
}

/// Sort indices by a single float64 column (optimized single-column version)
/// 
/// # Arguments
/// * `data` - Float64 array to sort by
/// * `ascending` - Whether to sort in ascending order
/// * `nulls_last` - Whether to put null values at the end
/// 
/// # Returns
/// * Array of indices sorted according to the column
#[wasm_bindgen]
pub fn sort_single_column_f64(data: &[f64], ascending: bool, nulls_last: bool) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..data.len()).collect();
    
    indices.sort_by(|&a, &b| {
        let val_a = data[a];
        let val_b = data[b];
        
        // Handle NaN values (treat as null)
        let a_is_nan = val_a.is_nan();
        let b_is_nan = val_b.is_nan();
        
        let comparison = match (a_is_nan, b_is_nan) {
            (true, true) => Ordering::Equal,
            (true, false) => if nulls_last { Ordering::Greater } else { Ordering::Less },
            (false, true) => if nulls_last { Ordering::Less } else { Ordering::Greater },
            (false, false) => {
                if val_a < val_b {
                    Ordering::Less
                } else if val_a > val_b {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
        };
        
        if ascending {
            comparison
        } else {
            comparison.reverse()
        }
    });
    
    indices
}

/// Sort indices by a single int32 column (optimized single-column version)
/// 
/// # Arguments
/// * `data` - Int32 array to sort by
/// * `ascending` - Whether to sort in ascending order
/// * `nulls_last` - Whether to put null values at the end
/// 
/// # Returns
/// * Array of indices sorted according to the column
#[wasm_bindgen]
pub fn sort_single_column_i32(data: &[i32], ascending: bool, nulls_last: bool) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..data.len()).collect();
    
    indices.sort_by(|&a, &b| {
        let val_a = data[a];
        let val_b = data[b];
        
        // Using i32::MIN as null sentinel
        let a_is_null = val_a == i32::MIN;
        let b_is_null = val_b == i32::MIN;
        
        let comparison = match (a_is_null, b_is_null) {
            (true, true) => Ordering::Equal,
            (true, false) => if nulls_last { Ordering::Greater } else { Ordering::Less },
            (false, true) => if nulls_last { Ordering::Less } else { Ordering::Greater },
            (false, false) => val_a.cmp(&val_b)
        };
        
        if ascending {
            comparison
        } else {
            comparison.reverse()
        }
    });
    
    indices
}
