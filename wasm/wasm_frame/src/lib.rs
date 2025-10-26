//! wasm_frame - High-performance data operations for BoxFrame
//! 
//! This crate provides optimized Rust implementations of common data operations
//! that are compiled to WebAssembly for use in the BoxFrame TypeScript library.
//! The functionality is organized into logical modules for better maintainability.

// Core engine functionality
pub mod core;
pub use core::*;

// Series operations
pub mod series;
pub use series::*;

// GroupBy operations
pub mod groupby;
pub use groupby::*;

// Sorting operations
pub mod sorting;
pub use sorting::*;

// Filtering operations
pub mod filtering;
pub use filtering::*;

// Statistical functions
pub mod statistics;
pub use statistics::*;

// Membership operations
pub mod membership;
pub use membership::*;
