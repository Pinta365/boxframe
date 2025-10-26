---
title: "Utility Functions"
nav_order: 7
parent: "API Reference"
description: "Utility functions for data type handling and conversions"
---

# Utility Functions

Helper functions for data type inference, conversion, and validation.

## Data Type Functions

- `isDType(value: string)` - Check if a string is a valid DType
- `inferDType(value: DataValue)` - Infer DType from a JavaScript value
- `toTypedArray(data: T[], dtype: DType)` - Convert to typed array
- `fromTypedArray(data: DataArray<T>, dtype: DType)` - Convert from typed array
- `isWasmEngineEnabled()` - Check if WASM engine is enabled



## Examples

```typescript
import { inferDType, isDType, isWasmEngineEnabled } from "@pinta365/boxframe";

// Type inference
const dtype = inferDType(42);        // "int32"
const dtype2 = inferDType(3.14);     // "float64"
const dtype3 = inferDType("hello");  // "string"

// Type validation
const isValid = isDType("float64");  // true
const isInvalid = isDType("invalid"); // false

// WASM status
const wasmEnabled = isWasmEngineEnabled(); // true/false
```

## Notes

- These are utility functions for data type handling
- Used internally by BoxFrame classes
- Available for advanced use cases requiring direct type operations
