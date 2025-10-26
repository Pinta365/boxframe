---
title: "Data Types"
nav_order: 5
parent: "API Reference"
description: "Type definitions and interfaces"
---

# Data Types

## Core Interfaces

```typescript
interface DataFrameOptions {
    index?: Index[];
    columns?: string[];
}

interface SeriesOptions {
    name?: string;
    index?: Index[];
    dtype?: DType;
}

interface GroupByOptions {
    as_index?: boolean;
    sort?: boolean;
    dropna?: boolean;
}
```

## Core Types

```typescript
type DataValue = string | number | boolean | Date | null;
type Index = string | number;
type DType = "int32" | "float64" | "string" | "bool" | "datetime";
```

## Aggregation Types

```typescript
type AggFunc = "sum" | "mean" | "count" | "size" | "min" | "max" | "std" | "var" | "first" | "last";
type AggSpec = AggFunc | { [column: string]: AggFunc | AggFunc[] };
```
