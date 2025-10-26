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
    hasHeader?: boolean;        // Whether first row contains headers
    delimiter?: string;         // Custom delimiter (default: ',')
    skipRows?: number;         // Number of rows to skip
    columns?: string[];        // Custom column names
    comment?: string;          // Comment character to ignore lines
}
```

## Examples

```typescript
import { parseCsv, parseCsvFromFile } from "@pinta365/boxframe";

// Parse CSV string
const df1 = parseCsv("name,age\nAlice,25\nBob,30");

// Parse from file
const df2 = await parseCsvFromFile("data.csv");

// Parse from URL
const df3 = await parseCsvFromFile("https://example.com/data.csv");

// Custom delimiter
const df4 = parseCsv("name;age\nAlice;25", { delimiter: ";" });

// Advanced options
const df5 = parseCsv(content, {
    hasHeader: true,
    delimiter: ",",
    skipRows: 1,
    comment: "#"
});
```

## Notes

- Automatically detects data types
- Handles missing values gracefully
- Supports both local files and URLs
- Browser environments require URLs (no local file access)
- Streaming support for large files is planned for future releases
