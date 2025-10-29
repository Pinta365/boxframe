---
title: "CSV Parser API"
nav_order: 6
parent: "API Reference"
description: "CSV parsing functions and utilities"
---

# CSV Parser API

Functions for parsing CSV data from various sources.

## Functions

- `parseCsv(content: string, options?)` - Parse CSV content from string
- `parseCsvFromFile(filePath: string, options?)` - Parse CSV from file or URL

## Options

```typescript
interface CsvParseOptions {
    hasHeader?: boolean;                    // Whether first row contains headers
    delimiter?: string;                     // Custom delimiter (default: ',')
    skipRows?: number;                      // Number of rows to skip
    columns?: string[];                     // Custom column names
    comment?: string;                       // Comment character to ignore lines
    dtypes?: Record<string, DType> | DType[]; // Column data types
    inferTypes?: boolean;                   // Enable automatic type inference (default: true)
    naValues?: string[];                    // Values to treat as null/NaN
}

type DType = "int32" | "float64" | "string" | "bool" | "datetime";
```

## Examples

### Basic Usage

```typescript
import { parseCsv, parseCsvFromFile } from "@pinta365/boxframe";

// Parse CSV string with automatic type inference
const df1 = parseCsv("name,age,salary,hire_date\nAlice,25,50000.50,2023-01-15\nBob,30,75000,2022-06-10");
// age: int32, salary: float64, hire_date: datetime, name: string

// Parse from file
const df2 = await parseCsvFromFile("data.csv");

// Parse from URL
const df3 = await parseCsvFromFile("https://example.com/data.csv");
```

### Data Type Control

```typescript
// Explicit data types by column name
const df4 = parseCsv(csvContent, {
    dtypes: {
        name: "string",
        age: "int32",
        salary: "float64",
        is_active: "bool",
        hire_date: "datetime"
    }
});

// Partial type specification (infer rest)
const df5 = parseCsv(csvContent, {
    dtypes: {
        name: "string",
        age: "int32"
        // salary, hire_date will be automatically inferred
    }
});

// Data types by column index (no headers)
const df6 = parseCsv(csvContent, {
    hasHeader: false,
    dtypes: ["string", "int32", "float64", "bool", "datetime"]
});

// Disable type inference (all strings)
const df7 = parseCsv(csvContent, {
    inferTypes: false
});
```

### Advanced Options

```typescript
// Custom NA values and delimiter
const df8 = parseCsv(csvContent, {
    delimiter: ";",
    naValues: ["N/A", "unknown", "NULL"],
    dtypes: {
        salary: "float64",
        is_active: "bool"
    }
});

// Skip rows and comments
const df9 = parseCsv(csvContent, {
    skipRows: 2,
    comment: "#",
    hasHeader: true
});
```

## Data Type Inference

The CSV parser intelligently infers data types from your data:

- **Numbers**: `int32` for integers, `float64` for decimals
- **Booleans**: `true`/`false` (case-insensitive) → `bool`
- **Dates**: `YYYY-MM-DD`, `MM/DD/YYYY`, `YYYY-MM-DDTHH:MM:SS` → `datetime`
- **Strings**: Everything else → `string`

### Supported Date Formats

- `YYYY-MM-DD` (e.g., `2023-01-15`)
- `YYYY-MM-DD HH:MM:SS` (e.g., `2023-01-15 10:30:00`)
- `YYYY-MM-DDTHH:MM:SS` (e.g., `2023-01-15T10:30:00Z`)
- `MM/DD/YYYY` (e.g., `01/15/2023`)
- `MM-DD-YYYY` (e.g., `01-15-2023`)
- `YYYY/MM/DD` (e.g., `2023/01/15`)
- `M/D/YYYY` (e.g., `1/15/2023`)


