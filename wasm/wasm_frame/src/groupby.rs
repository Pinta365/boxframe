//! GroupBy operations: aggregations on grouped data
//! 
//! This module provides functions for performing various aggregations
//! on grouped data using registered series and group keys.

use std::collections::HashMap;
use serde_json;
use wasm_bindgen::prelude::*;
use crate::core::ENGINE;

/// GroupBy sum using an existing registered f64 series and JSON keys
/// Returns a new series_id for the aggregated result (values sorted by key)
#[wasm_bindgen]
pub fn engine_groupby_sum_f64(series_id: u32, group_keys_json: &str) -> u32 {
    let keys: Vec<String> = serde_json::from_str(group_keys_json).unwrap_or_default();

    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) {
            (*ptr, *len)
        } else {
            (std::ptr::null_mut(), 0)
        }
    });
    if src_ptr.is_null() {
        return u32::MAX;
    }

    if keys.len() != src_len || src_ptr.is_null() {
        return u32::MAX;
    }

    // Build groups
    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, key) in keys.iter().enumerate() {
        groups.entry(key.clone()).or_insert_with(Vec::new).push(i);
    }

    // Sort keys to maintain deterministic order
    let mut sorted_keys: Vec<String> = groups.keys().cloned().collect();
    sorted_keys.sort();

    // Compute sums in a temporary Vec
    let mut results: Vec<f64> = Vec::with_capacity(sorted_keys.len());
    unsafe {
        for k in sorted_keys.iter() {
            if let Some(ixs) = groups.get(k) {
                let mut sum = 0.0;
                for &idx in ixs {
                    let v = *src_ptr.add(idx);
                    if !v.is_nan() {
                        sum += v;
                    }
                }
                results.push(sum);
            }
        }
    }

    // Register result as a new series in engine
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let id = eng.next_series_id;
        eng.next_series_id = eng.next_series_id.wrapping_add(1);

        let len = results.len();
        let dst_ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(
                len * std::mem::size_of::<f64>(),
                std::mem::align_of::<f64>(),
            )
            .unwrap();
            let raw = std::alloc::alloc(layout) as *mut f64;
            if !raw.is_null() && len > 0 {
                std::ptr::copy_nonoverlapping(results.as_ptr(), raw, len);
            }
            raw
        };
        eng.series_store.insert(id, (dst_ptr, len));
        id
    })
}

/// GroupBy mean using an existing registered f64 series and JSON keys
#[wasm_bindgen]
pub fn engine_groupby_mean_f64(series_id: u32, group_keys_json: &str) -> u32 {
    let keys: Vec<String> = serde_json::from_str(group_keys_json).unwrap_or_default();

    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) {
            (*ptr, *len)
        } else {
            (std::ptr::null_mut(), 0)
        }
    });
    if src_ptr.is_null() || keys.len() != src_len {
        return u32::MAX;
    }

    let mut groups: HashMap<String, (f64, usize)> = HashMap::new();
    unsafe {
        for (i, key) in keys.iter().enumerate() {
            let v = *src_ptr.add(i);
            if !v.is_nan() {
                let entry = groups.entry(key.clone()).or_insert((0.0, 0));
                entry.0 += v;
                entry.1 += 1;
            }
        }
    }

    let mut sorted_keys: Vec<String> = groups.keys().cloned().collect();
    sorted_keys.sort();
    let results: Vec<f64> = sorted_keys
        .into_iter()
        .map(|k| {
            let (sum, cnt) = groups.get(&k).cloned().unwrap_or((0.0, 0));
            if cnt > 0 { sum / (cnt as f64) } else { f64::NAN }
        })
        .collect();

    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let id = eng.next_series_id;
        eng.next_series_id = eng.next_series_id.wrapping_add(1);
        let len = results.len();
        let dst_ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(
                len * std::mem::size_of::<f64>(),
                std::mem::align_of::<f64>(),
            )
            .unwrap();
            let raw = std::alloc::alloc(layout) as *mut f64;
            if !raw.is_null() && len > 0 {
                std::ptr::copy_nonoverlapping(results.as_ptr(), raw, len);
            }
            raw
        };
        eng.series_store.insert(id, (dst_ptr, len));
        id
    })
}

/// GroupBy count (non-null) using an existing registered f64 series and JSON keys
#[wasm_bindgen]
pub fn engine_groupby_count_f64(series_id: u32, group_keys_json: &str) -> u32 {
    let keys: Vec<String> = serde_json::from_str(group_keys_json).unwrap_or_default();

    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) {
            (*ptr, *len)
        } else {
            (std::ptr::null_mut(), 0)
        }
    });
    if src_ptr.is_null() || keys.len() != src_len {
        return u32::MAX;
    }

    // First, identify all unique group keys
    let unique_keys: std::collections::HashSet<String> = keys.iter().cloned().collect();
    
    // Then, count non-null values for each group
    let mut groups: HashMap<String, usize> = HashMap::new();
    unsafe {
        for (i, key) in keys.iter().enumerate() {
            let v = *src_ptr.add(i);
            // For count, we count non-null values (filter out NaN)
            if !v.is_nan() {
                *groups.entry(key.clone()).or_insert(0) += 1;
            }
        }
    }

    // Return results for all unique group keys, even if count is 0
    let mut sorted_keys: Vec<String> = unique_keys.into_iter().collect();
    sorted_keys.sort();
    let results: Vec<f64> = sorted_keys
        .into_iter()
        .map(|k| groups.get(&k).cloned().unwrap_or(0) as f64)
        .collect();

    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let id = eng.next_series_id;
        eng.next_series_id = eng.next_series_id.wrapping_add(1);
        let len = results.len();
        let dst_ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(
                len * std::mem::size_of::<f64>(),
                std::mem::align_of::<f64>(),
            )
            .unwrap();
            let raw = std::alloc::alloc(layout) as *mut f64;
            if !raw.is_null() && len > 0 {
                std::ptr::copy_nonoverlapping(results.as_ptr(), raw, len);
            }
            raw
        };
        eng.series_store.insert(id, (dst_ptr, len));
        id
    })
}

/// GroupBy min using an existing registered f64 series and JSON keys
#[wasm_bindgen]
pub fn engine_groupby_min_f64(series_id: u32, group_keys_json: &str) -> u32 {
    let keys: Vec<String> = serde_json::from_str(group_keys_json).unwrap_or_default();
    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) { (*ptr, *len) } else { (std::ptr::null_mut(), 0) }
    });
    if src_ptr.is_null() || keys.len() != src_len { return u32::MAX; }
    let mut groups: HashMap<String, f64> = HashMap::new();
    unsafe {
        for (i, key) in keys.iter().enumerate() {
            let v = *src_ptr.add(i);
            if !v.is_nan() {
                groups.entry(key.clone()).and_modify(|m| { if v < *m { *m = v; } }).or_insert(v);
            }
        }
    }
    let mut sorted_keys: Vec<String> = groups.keys().cloned().collect();
    sorted_keys.sort();
    let results: Vec<f64> = sorted_keys.into_iter().map(|k| *groups.get(&k).unwrap_or(&f64::NAN)).collect();
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let id = eng.next_series_id; eng.next_series_id = eng.next_series_id.wrapping_add(1);
        let len = results.len();
        let dst_ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(len * std::mem::size_of::<f64>(), std::mem::align_of::<f64>()).unwrap();
            let raw = std::alloc::alloc(layout) as *mut f64;
            if !raw.is_null() && len > 0 { std::ptr::copy_nonoverlapping(results.as_ptr(), raw, len); }
            raw
        };
        eng.series_store.insert(id, (dst_ptr, len)); id
    })
}

/// GroupBy max using an existing registered f64 series and JSON keys
#[wasm_bindgen]
pub fn engine_groupby_max_f64(series_id: u32, group_keys_json: &str) -> u32 {
    let keys: Vec<String> = serde_json::from_str(group_keys_json).unwrap_or_default();
    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) { (*ptr, *len) } else { (std::ptr::null_mut(), 0) }
    });
    if src_ptr.is_null() || keys.len() != src_len { return u32::MAX; }
    let mut groups: HashMap<String, f64> = HashMap::new();
    unsafe {
        for (i, key) in keys.iter().enumerate() {
            let v = *src_ptr.add(i);
            if !v.is_nan() {
                groups.entry(key.clone()).and_modify(|m| { if v > *m { *m = v; } }).or_insert(v);
            }
        }
    }
    let mut sorted_keys: Vec<String> = groups.keys().cloned().collect();
    sorted_keys.sort();
    let results: Vec<f64> = sorted_keys.into_iter().map(|k| *groups.get(&k).unwrap_or(&f64::NAN)).collect();
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let id = eng.next_series_id; eng.next_series_id = eng.next_series_id.wrapping_add(1);
        let len = results.len();
        let dst_ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(len * std::mem::size_of::<f64>(), std::mem::align_of::<f64>()).unwrap();
            let raw = std::alloc::alloc(layout) as *mut f64;
            if !raw.is_null() && len > 0 { std::ptr::copy_nonoverlapping(results.as_ptr(), raw, len); }
            raw
        };
        eng.series_store.insert(id, (dst_ptr, len)); id
    })
}

/// GroupBy std using an existing registered f64 series and JSON keys (sample std, N-1)
#[wasm_bindgen]
pub fn engine_groupby_std_f64(series_id: u32, group_keys_json: &str) -> u32 {
    let keys: Vec<String> = serde_json::from_str(group_keys_json).unwrap_or_default();
    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) { (*ptr, *len) } else { (std::ptr::null_mut(), 0) }
    });
    if src_ptr.is_null() || keys.len() != src_len { return u32::MAX; }
    let mut sums: HashMap<String, f64> = HashMap::new();
    let mut counts: HashMap<String, usize> = HashMap::new();
    unsafe {
        for (i, key) in keys.iter().enumerate() {
            let v = *src_ptr.add(i);
            if !v.is_nan() {
                *sums.entry(key.clone()).or_insert(0.0) += v;
                *counts.entry(key.clone()).or_insert(0) += 1;
            }
        }
    }
    let mut means: HashMap<String, f64> = HashMap::new();
    for (k, c) in counts.iter() { let s = sums.get(k).cloned().unwrap_or(0.0); means.insert(k.clone(), if *c>0 { s/(*c as f64) } else { f64::NAN }); }
    let mut sumsqdiff: HashMap<String, f64> = HashMap::new();
    unsafe {
        for (i, key) in keys.iter().enumerate() {
            let v = *src_ptr.add(i);
            if !v.is_nan() {
                let m = means.get(key).cloned().unwrap_or(f64::NAN);
                if !m.is_nan() { *sumsqdiff.entry(key.clone()).or_insert(0.0) += (v - m)*(v - m); }
            }
        }
    }
    let mut sorted_keys: Vec<String> = counts.keys().cloned().collect();
    sorted_keys.sort();
    let results: Vec<f64> = sorted_keys.into_iter().map(|k| {
        let c = counts.get(&k).cloned().unwrap_or(0);
        if c>1 { let ss = sumsqdiff.get(&k).cloned().unwrap_or(0.0); (ss/((c-1) as f64)).sqrt() } else { f64::NAN }
    }).collect();
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let id = eng.next_series_id; eng.next_series_id = eng.next_series_id.wrapping_add(1);
        let len = results.len();
        let dst_ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(len * std::mem::size_of::<f64>(), std::mem::align_of::<f64>()).unwrap();
            let raw = std::alloc::alloc(layout) as *mut f64;
            if !raw.is_null() && len > 0 { std::ptr::copy_nonoverlapping(results.as_ptr(), raw, len); }
            raw
        };
        eng.series_store.insert(id, (dst_ptr, len)); id
    })
}

/// GroupBy var using an existing registered f64 series and JSON keys (sample var, N-1)
#[wasm_bindgen]
pub fn engine_groupby_var_f64(series_id: u32, group_keys_json: &str) -> u32 {
    let keys: Vec<String> = serde_json::from_str(group_keys_json).unwrap_or_default();
    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) { (*ptr, *len) } else { (std::ptr::null_mut(), 0) }
    });
    if src_ptr.is_null() || keys.len() != src_len { return u32::MAX; }
    let mut sums: HashMap<String, f64> = HashMap::new();
    let mut counts: HashMap<String, usize> = HashMap::new();
    unsafe {
        for (i, key) in keys.iter().enumerate() {
            let v = *src_ptr.add(i);
            if !v.is_nan() {
                *sums.entry(key.clone()).or_insert(0.0) += v;
                *counts.entry(key.clone()).or_insert(0) += 1;
            }
        }
    }
    let mut means: HashMap<String, f64> = HashMap::new();
    for (k, c) in counts.iter() { let s = sums.get(k).cloned().unwrap_or(0.0); means.insert(k.clone(), if *c>0 { s/(*c as f64) } else { f64::NAN }); }
    let mut sumsqdiff: HashMap<String, f64> = HashMap::new();
    unsafe {
        for (i, key) in keys.iter().enumerate() {
            let v = *src_ptr.add(i);
            if !v.is_nan() {
                let m = means.get(key).cloned().unwrap_or(f64::NAN);
                if !m.is_nan() { *sumsqdiff.entry(key.clone()).or_insert(0.0) += (v - m)*(v - m); }
            }
        }
    }
    let mut sorted_keys: Vec<String> = counts.keys().cloned().collect();
    sorted_keys.sort();
    let results: Vec<f64> = sorted_keys.into_iter().map(|k| {
        let c = counts.get(&k).cloned().unwrap_or(0);
        if c>1 { let ss = sumsqdiff.get(&k).cloned().unwrap_or(0.0); ss/((c-1) as f64) } else { f64::NAN }
    }).collect();
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let id = eng.next_series_id; eng.next_series_id = eng.next_series_id.wrapping_add(1);
        let len = results.len();
        let dst_ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(len * std::mem::size_of::<f64>(), std::mem::align_of::<f64>()).unwrap();
            let raw = std::alloc::alloc(layout) as *mut f64;
            if !raw.is_null() && len > 0 { std::ptr::copy_nonoverlapping(results.as_ptr(), raw, len); }
            raw
        };
        eng.series_store.insert(id, (dst_ptr, len)); id
    })
}

/// Batch multi-aggregation for groupby on f64 series.
/// agg_mask bit layout (LSB -> MSB):
/// 1=sum, 2=mean, 4=count, 8=min, 16=max, 32=std, 64=var
/// Returns array of series ids in the above order for bits that are set.
#[wasm_bindgen]
pub fn engine_groupby_multi_f64(series_id: u32, group_keys_json: &str, agg_mask: u32) -> Box<[u32]> {
    let keys: Vec<String> = serde_json::from_str(group_keys_json).unwrap_or_default();
    let (src_ptr, src_len) = ENGINE.with(|cell| {
        let eng = cell.borrow();
        if let Some((ptr, len)) = eng.series_store.get(&series_id) { (*ptr, *len) } else { (std::ptr::null_mut(), 0) }
    });
    if src_ptr.is_null() || keys.len() != src_len { return Box::new([]); }

    // Prepare maps
    let mut sums: HashMap<String, f64> = HashMap::new();
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut mins: HashMap<String, f64> = HashMap::new();
    let mut maxs: HashMap<String, f64> = HashMap::new();

    let need_sum = (agg_mask & 1) != 0 || (agg_mask & 2) != 0 || (agg_mask & 32) != 0 || (agg_mask & 64) != 0;
    let need_count = (agg_mask & 4) != 0 || (agg_mask & 2) != 0 || (agg_mask & 32) != 0 || (agg_mask & 64) != 0;
    let need_min = (agg_mask & 8) != 0;
    let need_max = (agg_mask & 16) != 0;

    unsafe {
        for (i, key) in keys.iter().enumerate() {
            let v = *src_ptr.add(i);
            if v.is_nan() { continue; }
            if need_sum { *sums.entry(key.clone()).or_insert(0.0) += v; }
            if need_count { *counts.entry(key.clone()).or_insert(0) += 1; }
            if need_min {
                mins.entry(key.clone()).and_modify(|m| { if v < *m { *m = v; } }).or_insert(v);
            }
            if need_max {
                maxs.entry(key.clone()).and_modify(|m| { if v > *m { *m = v; } }).or_insert(v);
            }
        }
    }

    let mut means: HashMap<String, f64> = HashMap::new();
    if (agg_mask & 2) != 0 || (agg_mask & 32) != 0 || (agg_mask & 64) != 0 {
        for (k, c) in counts.iter() {
            let s = sums.get(k).cloned().unwrap_or(0.0);
            means.insert(k.clone(), if *c > 0 { s / (*c as f64) } else { f64::NAN });
        }
    }
    let mut sumsqdiff: HashMap<String, f64> = HashMap::new();
    if (agg_mask & 32) != 0 || (agg_mask & 64) != 0 {
        unsafe {
            for (i, key) in keys.iter().enumerate() {
                let v = *src_ptr.add(i);
                if v.is_nan() { continue; }
                let m = means.get(key).cloned().unwrap_or(f64::NAN);
                if !m.is_nan() { *sumsqdiff.entry(key.clone()).or_insert(0.0) += (v - m) * (v - m); }
            }
        }
    }

    // Deterministic order
    let mut ordered_keys: Vec<String> = counts.keys().cloned().collect();
    if ordered_keys.is_empty() {
        // fallback to any keys seen in mins/maxs/sums
        for k in sums.keys() { if !ordered_keys.contains(k) { ordered_keys.push(k.clone()); } }
        for k in mins.keys() { if !ordered_keys.contains(k) { ordered_keys.push(k.clone()); } }
        for k in maxs.keys() { if !ordered_keys.contains(k) { ordered_keys.push(k.clone()); } }
    }
    ordered_keys.sort();

    // Helper to register a result vec and return id
    let mut out_ids: Vec<u32> = Vec::new();
    let register_vec = |vals: Vec<f64>| -> u32 {
        ENGINE.with(|cell| {
            let mut eng = cell.borrow_mut();
            let id = eng.next_series_id; eng.next_series_id = eng.next_series_id.wrapping_add(1);
            let len = vals.len();
            let dst_ptr = unsafe {
                let layout = std::alloc::Layout::from_size_align(len * std::mem::size_of::<f64>(), std::mem::align_of::<f64>()).unwrap();
                let raw = std::alloc::alloc(layout) as *mut f64;
                if !raw.is_null() && len > 0 { std::ptr::copy_nonoverlapping(vals.as_ptr(), raw, len); }
                raw
            };
            eng.series_store.insert(id, (dst_ptr, len)); id
        })
    };

    if (agg_mask & 1) != 0 {
        let vals: Vec<f64> = ordered_keys.iter().map(|k| sums.get(k).cloned().unwrap_or(0.0)).collect();
        out_ids.push(register_vec(vals));
    }
    if (agg_mask & 2) != 0 {
        let vals: Vec<f64> = ordered_keys.iter().map(|k| {
            let c = counts.get(k).cloned().unwrap_or(0);
            if c>0 { sums.get(k).cloned().unwrap_or(0.0) / (c as f64) } else { f64::NAN }
        }).collect();
        out_ids.push(register_vec(vals));
    }
    if (agg_mask & 4) != 0 {
        let vals: Vec<f64> = ordered_keys.iter().map(|k| counts.get(k).cloned().unwrap_or(0) as f64).collect();
        out_ids.push(register_vec(vals));
    }
    if (agg_mask & 8) != 0 {
        let vals: Vec<f64> = ordered_keys.iter().map(|k| mins.get(k).cloned().unwrap_or(f64::NAN)).collect();
        out_ids.push(register_vec(vals));
    }
    if (agg_mask & 16) != 0 {
        let vals: Vec<f64> = ordered_keys.iter().map(|k| maxs.get(k).cloned().unwrap_or(f64::NAN)).collect();
        out_ids.push(register_vec(vals));
    }
    if (agg_mask & 32) != 0 {
        let vals: Vec<f64> = ordered_keys.iter().map(|k| {
            let c = counts.get(k).cloned().unwrap_or(0);
            if c>1 { let ss = sumsqdiff.get(k).cloned().unwrap_or(0.0); (ss/((c-1) as f64)).sqrt() } else { f64::NAN }
        }).collect();
        out_ids.push(register_vec(vals));
    }
    if (agg_mask & 64) != 0 {
        let vals: Vec<f64> = ordered_keys.iter().map(|k| {
            let c = counts.get(k).cloned().unwrap_or(0);
            if c>1 { let ss = sumsqdiff.get(k).cloned().unwrap_or(0.0); ss/((c-1) as f64) } else { f64::NAN }
        }).collect();
        out_ids.push(register_vec(vals));
    }

    out_ids.into_boxed_slice()
}
