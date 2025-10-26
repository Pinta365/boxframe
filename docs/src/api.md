---
title: "API Reference"
nav_order: 3
description: "Complete API documentation for BoxFrame"
---

# API Reference

This section provides comprehensive documentation for all BoxFrame classes, methods, and utilities. The API is organized into logical sections for easy navigation.

## Core Classes

### [DataFrame API](dataframe)
The main class for working with two-dimensional data. Includes methods for data inspection, selection, manipulation, grouping, and cleaning.

### [Series API](series)
One-dimensional labeled arrays. Provides methods for data access, statistical operations, and data manipulation.

### [GroupBy API](groupby)
Grouping and aggregation operations for both Series and DataFrame.

### [WasmEngine API](wasm-engine)
High-performance WASM engine for memory management and accelerated operations.

## Utilities & Factory Methods

### [Static Methods](static-methods)
BoxFrame static factory methods for creating DataFrames and Series, plus data I/O operations and utility functions.

### [CSV Parser API](csv-parser)
CSV parsing functions for reading data from files, URLs, and strings.

### [GoogleSheets API](google-sheets)
Integration for reading public Google Sheets data.

### [Utility Functions](utilities)
Helper functions for data type handling, conversions, and validation.

### [Data Types](data-types)
Complete type definitions, interfaces, and type aliases used throughout the BoxFrame API.

### [Error Handling](error-handling)
Comprehensive error handling with specific error classes for different scenarios.

## Quick Reference

### Most Common Operations

```typescript
// Create DataFrame
const df = new DataFrame({
    name: ["Alice", "Bob", "Charlie"],
    age: [25, 30, 35],
    city: ["New York", "London", "Tokyo"]
});

// Basic operations
df.head()                    // First 5 rows
df.shape                     // [rows, columns]
df.get("age")                // Get column as Series
df.query("age > 30")         // Filter rows with query

// Data manipulation
df.assign({ salary: [50000, 60000, 70000] })  // Add column
df.sortValues("age", false)                   // Sort descending
df.groupBy("city").agg({ age: "mean" })       // Group and aggregate
```

### Performance Features

- **WASM Acceleration**: Automatic fallback to JavaScript if WASM unavailable
- **Memory Management**: Efficient memory usage with automatic cleanup
- **Performance Monitoring**: Query memory usage and active series count

### Error Handling

BoxFrame uses descriptive error messages for better debugging:
- **Column Errors**: "Column 'name' not found" when accessing missing columns
- **Index Errors**: "Index length mismatch" when data lengths don't align
- **Query Errors**: "Invalid condition" when query syntax is wrong
- **Validation Errors**: Specific messages for invalid parameters

