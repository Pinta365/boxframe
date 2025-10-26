//! Statistical functions: direct statistical operations on arrays
//! 
//! This module provides high-performance statistical functions that operate
//! directly on arrays without requiring engine registration.

use wasm_bindgen::prelude::*;

/// High-performance vectorized sum
#[wasm_bindgen]
pub fn sum_f64(data: &[f64]) -> f64 {
    data.iter().filter(|&&x| !x.is_nan()).sum()
}

/// High-performance vectorized mean
#[wasm_bindgen]
pub fn mean_f64(data: &[f64]) -> f64 {
    let valid_data: Vec<f64> = data.iter().filter(|&&x| !x.is_nan()).copied().collect();
    if valid_data.is_empty() {
        f64::NAN
    } else {
        valid_data.iter().sum::<f64>() / valid_data.len() as f64
    }
}

/// High-performance vectorized standard deviation (sample)
#[wasm_bindgen]
pub fn std_f64(data: &[f64]) -> f64 {
    let valid_data: Vec<f64> = data.iter().filter(|&&x| !x.is_nan()).copied().collect();
    if valid_data.is_empty() {
        return f64::NAN;
    }
    if valid_data.len() == 1 {
        return 0.0;
    }
    
    let mean = valid_data.iter().sum::<f64>() / valid_data.len() as f64;
    let variance = valid_data.iter()
        .map(|&x| (x - mean).powi(2))
        .sum::<f64>() / (valid_data.len() - 1) as f64;
    
    variance.sqrt()
}

/// High-performance vectorized min
#[wasm_bindgen]
pub fn min_f64(data: &[f64]) -> f64 {
    data.iter()
        .filter(|&&x| !x.is_nan())
        .fold(f64::INFINITY, |a, &b| a.min(b))
}

/// High-performance vectorized max
#[wasm_bindgen]
pub fn max_f64(data: &[f64]) -> f64 {
    data.iter()
        .filter(|&&x| !x.is_nan())
        .fold(f64::NEG_INFINITY, |a, &b| a.max(b))
}

/// Count non-null values
#[wasm_bindgen]
pub fn count_non_null_f64(data: &[f64]) -> usize {
    data.iter().filter(|&&x| !x.is_nan()).count()
}
