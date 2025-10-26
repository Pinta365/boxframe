//! Membership operations: checking if values are in a given set
//! 
//! This module provides functions for membership testing using hash sets
//! for O(1) lookup performance instead of O(n) linear search.

use std::collections::HashSet;
use wasm_bindgen::prelude::*;

/// Check if values in an array are members of a given set (i32)
/// 
/// # Arguments
/// * `data` - Array of i32 values to check
/// * `values` - Array of i32 values to check membership against
/// 
/// # Returns
/// * Array of u8 values (0 = false, 1 = true) indicating membership
#[wasm_bindgen]
pub fn isin_i32(data: &[i32], values: &[i32]) -> Vec<u8> {
    // Create hash set for O(1) lookups
    let value_set: HashSet<i32> = values.iter().copied().collect();
    
    // Check membership for each element
    data.iter()
        .map(|&val| if value_set.contains(&val) { 1 } else { 0 })
        .collect()
}

/// Check if values in an array are members of a given set (f64 with tolerance)
/// Note: For floating point numbers, we use a simple linear search with tolerance
/// since HashSet doesn't work well with floating point equality
/// 
/// # Arguments
/// * `data` - Array of f64 values to check
/// * `values` - Array of f64 values to check membership against
/// * `tolerance` - Tolerance for floating point comparison (default: 1e-9)
/// 
/// # Returns
/// * Array of u8 values (0 = false, 1 = true) indicating membership
#[wasm_bindgen]
pub fn isin_f64(data: &[f64], values: &[f64], tolerance: f64) -> Vec<u8> {
    let tol = if tolerance > 0.0 { tolerance } else { 1e-9 };
    
    // For floating point, we use linear search with tolerance
    // This is still faster than the JavaScript version for large datasets
    data.iter()
        .map(|&val| {
            values.iter().any(|&v| (val - v).abs() < tol) as u8
        })
        .collect()
}

/// Check if values in an array are members of a given set (strings)
/// Note: This function takes string arrays as Vec<String> since &[String] is not supported by wasm-bindgen
/// 
/// # Arguments
/// * `data` - Vector of string values to check
/// * `values` - Vector of string values to check membership against
/// 
/// # Returns
/// * Array of u8 values (0 = false, 1 = true) indicating membership
#[wasm_bindgen]
pub fn isin_string(data: Vec<String>, values: Vec<String>) -> Vec<u8> {
    // Create hash set for O(1) lookups
    let value_set: HashSet<String> = values.into_iter().collect();
    
    // Check membership for each element
    data.into_iter()
        .map(|val| if value_set.contains(&val) { 1 } else { 0 })
        .collect()
}