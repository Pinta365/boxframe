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
- `analyzeCsv(filePath: string, options?)` - Sample the first N lines to infer columns, dtypes, and estimate row count
- `parseCsvBatchedStream(filePath: string, options)` - Stream a large CSV in batches and receive `DataFrame` batches via callback
- `parseCsvStream(filePath: string, options?)` - Stream a large CSV and return a single accumulated `DataFrame`

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

### Streaming Options

```typescript
interface ParseCsvStreamOptions extends CsvParseOptions {
    batchSize?: number; // Number of lines per batch (default: 25,000)
    onBatch: (df: DataFrame) => void | Promise<void>; // Required for batched streaming
    onProgress?: (progress: { bytesRead: number; rowsProcessed: number }) => void; // Optional progress callback
    
    // Worker-based parallel parsing options
    useWorkers?: boolean; // Enable worker-based parsing (default: false)
    workerCount?: number; // Number of workers (default: Math.max(1, cores - 1))
    preserveOrder?: boolean; // Preserve batch order (default: true for parseCsvStream, false for batched)
    schema?: { columns: string[]; dtypes: Record<string, DType> }; // Pre-computed schema to skip inference
    abortSignal?: AbortSignal; // Signal for cancellation/timeout
    workerModule?: string | URL; // Custom worker module URL (advanced)
}
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

### Streaming Large CSVs

```typescript
import { parseCsvBatchedStream, parseCsvStream } from "@pinta365/boxframe";

// 1) Batched streaming with callbacks
await parseCsvBatchedStream("/path/to/large.csv", {
    hasHeader: true,
    batchSize: 50000, // number of data lines per batch
    onBatch: async (df) => {
        // Process each batch DataFrame incrementally
        console.log("batch rows:", df.shape[0]);
        // e.g., write to DB or aggregate stats here
    },
    onProgress: ({ bytesRead, rowsProcessed }) => {
        console.log("progress:", { bytesRead, rowsProcessed });
    },
});

// 2) Streaming that returns a single accumulated DataFrame. Options are optional.
const df = await parseCsvStream("/path/to/large.csv", {
    hasHeader: true,
    batchSize: 25000, // affects internal batching; result is a single DataFrame
});

console.log("total rows:", df.shape[0]);
```

### Worker-Based Parallel Parsing

For very large CSV files, you can enable worker-based parallel parsing to leverage multiple CPU cores:

```typescript
import { parseCsvBatchedStream, parseCsvStream, analyzeCsv } from "@pinta365/boxframe";

// Enable workers for parallel processing (recommended for files > 100MB)
await parseCsvBatchedStream("/path/to/very-large.csv", {
    useWorkers: true, // Enable parallel processing
    workerCount: 4, // Optional: specify number of workers (default: cores - 1)
    batchSize: 50000,
    preserveOrder: false, // Faster but batches may arrive out of order
    onBatch: async (df) => {
        console.log("Received batch:", df.shape[0], "rows");
        // Process batch...
    },
});

// With order preservation (slower but maintains batch sequence)
await parseCsvBatchedStream("/path/to/very-large.csv", {
    useWorkers: true,
    preserveOrder: true, // Batches arrive in sequence (0, 1, 2, ...)
    batchSize: 50000,
    onBatch: async (df) => {
        // Process batches in order...
    },
});

// Pre-compute schema to skip inference in workers (faster startup)
const analysis = await analyzeCsv("/path/to/very-large.csv", { sampleLines: 1000 });
const df = await parseCsvStream("/path/to/very-large.csv", {
    useWorkers: true,
    schema: {
        columns: analysis.columns,
        dtypes: analysis.dtypes,
    },
    // Schema is known, so workers skip type inference
});

// With cancellation support
const controller = new AbortController();
setTimeout(() => controller.abort(), 60000); // Cancel after 1 minute

await parseCsvBatchedStream("/path/to/very-large.csv", {
    useWorkers: true,
    abortSignal: controller.signal, // Enables cancellation
    onBatch: async (df) => {
        // Process batches...
    },
}).catch((err) => {
    if (err.message.includes("aborted")) {
        console.log("Parsing cancelled");
    }
});
```

### Quick File Analysis (Sampling)

```typescript
import { analyzeCsv } from "@pinta365/boxframe";

const analysis = await analyzeCsv("/path/to/large.csv", { sampleLines: 1000 });
console.log(analysis.columns);      // inferred column names
console.log(analysis.dtypes);       // inferred dtypes per column
console.log(analysis.estimatedRows); // rough estimate of total rows
console.log(analysis.sampleRows);    // rows parsed from the sample
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


