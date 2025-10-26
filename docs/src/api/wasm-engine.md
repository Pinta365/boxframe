---
title: "WasmEngine API"
nav_order: 3
parent: "API Reference"
description: "WASM engine for high-performance operations"
---

# WasmEngine API

The WASM engine provides high-performance operations and memory management.

## Memory Management

- `WasmEngine.getMemoryUsage()` - Get current WASM memory usage in bytes
- `WasmEngine.getSeriesCount()` - Get number of active series in WASM memory

## Performance Features

### WASM Acceleration

BoxFrame includes optional WASM acceleration for performance-critical operations:

- **Automatic Fallback**: Operations automatically fall back to JavaScript if WASM is unavailable
- **Memory Management**: WASM memory is temporary and automatically cleaned up
- **Performance Monitoring**: Query WASM memory usage and series count
