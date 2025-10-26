//! Core engine functionality: memory management and state
//! 
//! This module provides the foundational EngineState and memory management
//! functions for the WASM engine.

use std::cell::RefCell;
use std::collections::HashMap;
use wasm_bindgen::prelude::*;

// Simple ID generator and registries protected by a global mutex.
// This keeps design straightforward for single-threaded wasm; can be upgraded later.

#[derive(Default)]
pub struct EngineState {
    pub next_series_id: u32,
    // Store series as contiguous f64 buffers owned by WASM heap
    pub series_store: HashMap<u32, (*mut f64, usize)>,
    // Store series as contiguous i32 buffers owned by WASM heap
    pub series_store_i32: HashMap<u32, (*mut i32, usize)>,
}

impl EngineState {
    pub fn alloc_f64_buffer(&mut self, data: &[f64]) -> (*mut f64, usize) {
        let len = data.len();
        let ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(
                len * std::mem::size_of::<f64>(),
                std::mem::align_of::<f64>(),
            )
            .unwrap();
            let raw = std::alloc::alloc(layout) as *mut f64;
            if !raw.is_null() {
                std::ptr::copy_nonoverlapping(data.as_ptr(), raw, len);
            }
            raw
        };
        (ptr, len)
    }

    pub fn free_f64_buffer(&mut self, ptr: *mut f64, len: usize) {
        if !ptr.is_null() && len > 0 {
            unsafe {
                let layout = std::alloc::Layout::from_size_align(
                    len * std::mem::size_of::<f64>(),
                    std::mem::align_of::<f64>(),
                )
                .unwrap();
                std::alloc::dealloc(ptr as *mut u8, layout);
            }
        }
    }

    pub fn alloc_i32_buffer(&mut self, data: &[i32]) -> (*mut i32, usize) {
        let len = data.len();
        let ptr = unsafe {
            let layout = std::alloc::Layout::from_size_align(
                len * std::mem::size_of::<i32>(),
                std::mem::align_of::<i32>(),
            )
            .unwrap();
            let raw = std::alloc::alloc(layout) as *mut i32;
            if !raw.is_null() {
                std::ptr::copy_nonoverlapping(data.as_ptr(), raw, len);
            }
            raw
        };
        (ptr, len)
    }

    pub fn free_i32_buffer(&mut self, ptr: *mut i32, len: usize) {
        if !ptr.is_null() && len > 0 {
            unsafe {
                let layout = std::alloc::Layout::from_size_align(
                    len * std::mem::size_of::<i32>(),
                    std::mem::align_of::<i32>(),
                )
                .unwrap();
                std::alloc::dealloc(ptr as *mut u8, layout);
            }
        }
    }
}

thread_local! {
    pub static ENGINE: RefCell<EngineState> = RefCell::new(EngineState::default());
}

// Basic series creation and management functions
#[wasm_bindgen]
pub fn engine_create_series_f64(data: &[f64]) -> u32 {
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let (ptr, len) = eng.alloc_f64_buffer(data);
        let id = eng.next_series_id;
        eng.next_series_id = eng.next_series_id.wrapping_add(1);
        eng.series_store.insert(id, (ptr, len));
        id
    })
}

#[wasm_bindgen]
pub fn engine_create_series_i32(data: &[i32]) -> u32 {
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        let (ptr, len) = eng.alloc_i32_buffer(data);
        let id = eng.next_series_id;
        eng.next_series_id = eng.next_series_id.wrapping_add(1);
        eng.series_store_i32.insert(id, (ptr, len));
        id
    })
}

#[wasm_bindgen]
pub fn engine_free_series(series_id: u32) {
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        if let Some((ptr, len)) = eng.series_store.remove(&series_id) {
            eng.free_f64_buffer(ptr, len);
        }
    })
}

#[wasm_bindgen]
pub fn engine_free_series_i32(series_id: u32) {
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        if let Some((ptr, len)) = eng.series_store_i32.remove(&series_id) {
            eng.free_i32_buffer(ptr, len);
        }
    })
}

#[wasm_bindgen]
pub fn engine_flush() {
    ENGINE.with(|cell| {
        let mut eng = cell.borrow_mut();
        // Take the maps to avoid borrow issues, then free outside map
        let old_f64 = std::mem::take(&mut eng.series_store);
        for (_, (ptr, len)) in old_f64.into_iter() {
            eng.free_f64_buffer(ptr, len);
        }
        let old_i32 = std::mem::take(&mut eng.series_store_i32);
        for (_, (ptr, len)) in old_i32.into_iter() {
            eng.free_i32_buffer(ptr, len);
        }
        eng.next_series_id = 0;
    })
}

#[wasm_bindgen]
pub fn engine_memory_usage() -> usize {
    ENGINE.with(|cell| {
        let eng = cell.borrow();
        let mut total_bytes = 0;
        
        // Calculate f64 memory usage
        for (_, (_, len)) in eng.series_store.iter() {
            total_bytes += len * std::mem::size_of::<f64>();
        }
        
        // Calculate i32 memory usage
        for (_, (_, len)) in eng.series_store_i32.iter() {
            total_bytes += len * std::mem::size_of::<i32>();
        }
        
        total_bytes
    })
}

#[wasm_bindgen]
pub fn engine_series_count() -> usize {
    ENGINE.with(|cell| {
        let eng = cell.borrow();
        eng.series_store.len() + eng.series_store_i32.len()
    })
}