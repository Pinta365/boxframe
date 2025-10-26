---
title: "DataFrame API"
nav_order: 1
parent: "API Reference"
description: "DataFrame class methods and properties"
---

# DataFrame API

The main class for working with two-dimensional data.

## Constructor

```typescript
new DataFrame(data: Record<string, any[]> | any[][], options?: DataFrameOptions)
```

**Parameters:**

- `data`: Object with column names as keys and arrays as values, or 2D array
- `options`: Optional configuration object

## Data Inspection

- `head(n?: number)` - Return first n rows (default: 5)
- `tail(n?: number)` - Return last n rows (default: 5)
- `info()` - Display DataFrame information
- `describe()` - Generate descriptive statistics
- `shape` - Get dimensions as [rows, columns]
- `columns` - Get column names
- `dtypes` - Get data types for each column

## Data Selection

- `get(columnName: string)` - Get a single column as Series
- `getColumns(columnNames: string[])` - Get multiple columns as DataFrame
- `filter(mask: boolean[])` - Filter rows using boolean mask
- `query(expression: string)` - Filter using string expressions (e.g., 'age > 25')
- `iloc(rowPositions: number[], columnPositions?: number[])` - Select by integer position
- `loc(rowLabels: Index[], columnLabels?: string[])` - Select by label

## Data Manipulation

- `assign(assignments: Record<string, any[] | ((row: RowData) => any)>)` - Add new columns
- `dropColumns(columns: string[])` - Remove columns
- `dropRows(labels: Index[])` - Remove rows by label
- `drop(positions: number[])` - Remove rows by position
- `rename(columns: Record<string, string>)` - Rename columns
- `sortValues(by: string | string[], ascending?: boolean)` - Sort by column values
- `sortIndex(ascending?: boolean)` - Sort by index
- `resetIndex()` - Reset the index

## Grouping

- `groupBy(by: string | string[], options?: GroupByOptions)` - Group by specified columns

**Multiple Function Aggregation:**
- `agg({ column: ["func1", "func2"] })` - Multiple functions per column

## Data Cleaning

- `dropna(axis?: "rows" | "columns")` - Remove rows/columns with missing values
- `fillna(value: DataValue | Record<string, DataValue>)` - Fill missing values
- `dropDuplicates(subset?: string[])` - Remove duplicate rows
- `isnull()` - Check for null values
- `notnull()` - Check for non-null values

## Memory Management

- `memoryUsage()` - Calculate memory usage in bytes (includes WASM memory when available)

## Export

- `toJSON()` - Convert to JSON
- `toString()` - Convert to string representation
