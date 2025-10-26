---
title: "GroupBy API"
nav_order: 3
parent: "API Reference"
description: "GroupBy operations for Series and DataFrame aggregation"
---

# GroupBy API

GroupBy operations for aggregating data across groups. Available for both Series and DataFrame.

## Usage

### Series GroupBy
```typescript
const sales = new Series([100, 200, 150, 300, 250], { name: "sales" });
const regions = ["North", "North", "South", "South", "South"];
const grouped = sales.groupBy(regions); // Group by region labels
// This creates groups: "North" contains [100, 200], "South" contains [150, 300, 250]
```

**How Series GroupBy works:**
- You provide an array of group labels that matches the Series length
- Each position in the Series gets assigned to a group based on the label at that position
- Values with the same label are grouped together

### DataFrame GroupBy
```typescript
const df = new DataFrame({
    name: ["Alice", "Bob", "Charlie", "Alice", "Bob"],
    age: [25, 30, 35, 28, 32],
    salary: [50000, 60000, 70000, 55000, 65000]
});
const grouped = df.groupBy("name"); // Group by column
```

## Methods

### Data Access
- `groups()` - Get the groups as a Map
- `getGroup(name: string)` - Get a specific group (Series or DataFrame)
- `size()` - Get the size of each group

### Aggregation Methods
- `agg(aggregation: AggSpec)` - Apply aggregation function(s)
- `sum()` - Calculate sum for each group
- `mean()` - Calculate mean for each group
- `count()` - Calculate count for each group
- `min()` - Calculate minimum for each group
- `max()` - Calculate maximum for each group
- `std()` - Calculate standard deviation for each group
- `var()` - Calculate variance for each group
- `first()` - Get first value for each group
- `last()` - Get last value for each group

## Examples

### Series GroupBy
```typescript
const sales = new Series([100, 200, 150, 300, 250], { name: "sales" });
const regions = ["North", "North", "South", "South", "South"];
const grouped = sales.groupBy(regions);
const result = grouped.sum(); // Returns Series with total sales by region
```

### DataFrame GroupBy
```typescript
const df = new DataFrame({
    name: ["Alice", "Bob", "Charlie", "Alice", "Bob"],
    age: [25, 30, 35, 28, 32],
    salary: [50000, 60000, 70000, 55000, 65000]
});

// Single aggregation
const byName = df.groupBy("name");
const avgAge = byName.mean().get("age");

// Multiple aggregations
const result = df.groupBy("name").agg({
    age: "mean",
    salary: ["sum", "max"]
});
```

## Return Types

- **Series GroupBy** → Returns `Series` for single aggregations
- **DataFrame GroupBy** → Returns `DataFrame` for all aggregations
