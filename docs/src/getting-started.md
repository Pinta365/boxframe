---
title: "Getting Started"
nav_order: 2
description: "Learn how to install and use BoxFrame in your projects"
---

# Getting Started

This guide will help you get up and running with BoxFrame quickly.

## Installation

BoxFrame is available on JSR (JavaScript Registry) and can be installed with your preferred package manager:

#### Deno
```bash
deno add jsr:@pinta365/boxframe
```

#### Node.js
```bash
npx jsr add @pinta365/boxframe
```

#### Bun
```bash
bunx jsr add @pinta365/boxframe
```

#### Other Package Managers
```bash
# pnpm
pnpm i jsr:@pinta365/boxframe

# yarn
yarn add jsr:@pinta365/boxframe

# vlt
vlt install jsr:@pinta365/boxframe
```
## Your First DataFrame

Let's create a simple DataFrame to get started:

```typescript
import { DataFrame } from "@pinta365/boxframe";

// Create a DataFrame from an object
const df = new DataFrame({
    name: ["Alice", "Bob", "Charlie", "Diana"],
    age: [25, 30, 35, 28],
    city: ["New York", "London", "Tokyo", "Paris"],
    salary: [50000, 60000, 70000, 55000],
});

console.log(df);
/*
DataFrame {
  columns: [ "name", "age", "city", "salary" ],
  index: [ 0, 1, 2, 3 ],
  data: Map(4) {
    "name" => Series {
      data: [ "Alice", "Bob", "Charlie", "Diana" ],
      dtype: "string",
      index: [ 0, 1, 2, 3 ],
      name: "name"
    },
    "age" => Series {
      data: Int32Array(4) [ 25, 30, 35, 28 ],
      dtype: "int32",
      index: [ 0, 1, 2, 3 ],
      name: "age"
    },
    "city" => Series {
      data: [ "New York", "London", "Tokyo", "Paris" ],
      dtype: "string",
      index: [ 0, 1, 2, 3 ],
      name: "city"
    },
    "salary" => Series {
      data: Int32Array(4) [ 50000, 60000, 70000, 55000 ],
      dtype: "int32",
      index: [ 0, 1, 2, 3 ],
      name: "salary"
    }
  },
  shape: [ 4, 4 ]
}
*/
```

## Method Chaining

BoxFrame methods return new DataFrames, which allows you to chain operations together. This makes data manipulation more readable and efficient:

```typescript
// Chain multiple operations
const result = df
    .query("age > 25") // Filter by age > 25
    .getColumns(["name", "salary"]) // Get name and salary columns
    .sortValues("salary", false) // Sort by salary descending
    .head(2); // Get top 2 rows

console.log(result);
/*
DataFrame {
  columns: [ "name", "salary" ],
  index: [ 2, 1 ],
  data: Map(2) {
    "name" => Series {
      data: [ "Charlie", "Bob" ],
      dtype: "string",
      index: [ 2, 1 ],
      name: "name"
    },
    "salary" => Series {
      data: Int32Array(2) [ 70000, 60000 ],
      dtype: "int32",
      index: [ 2, 1 ],
      name: "salary"
    }
  },
  shape: [ 2, 2 ]
}
*/
```

## Basic Operations

### Viewing Data

```typescript
// Display first few rows
console.log(df.head());

// Display last few rows
console.log(df.tail());

// Get basic statistics
console.log(df.describe());

// Get DataFrame info
console.log(df.info());
```

### Filtering Data

```typescript
// Method 1: Using query() method (recommended for conditions)
const youngPeople = df.query("age < 30");
const highEarners = df.query("age > 30 and salary > 60000");

// Method 2: Using boolean mask with filter()
const mask = df.get("age").values.map((age) => age < 30);
const youngPeople = df.filter(mask);

// Method 3: Using loc() for label-based selection
const selectedRows = df.loc([0, 2], ["name", "age"]);

// Method 4: Using iloc() for position-based selection
const firstTwoRows = df.iloc([0, 1]);
```

### Selecting Columns

```typescript
// Select specific columns
const namesAndAges = df.getColumns(["name", "age"]);

// Select a single column (returns Series)
const names = df.get("name");
```

## Working with Series

A Series is a one-dimensional array with labels:

```typescript
import { Series } from "https://jsr.io/@pinta365/";

// Create a Series
const ages = new Series([25, 30, 35, 28], { name: "age" });

// Basic operations
console.log(ages.mean()); // 29.5
console.log(ages.max()); // 35
console.log(ages.min()); // 25
console.log(ages.sum()); // 118

console.log(ages.toString());
/*
Series: age
dtype: int32
length: 4

0    25
1    30
2    35
3    28
*/
```

## Grouping and Aggregation

```typescript
// Group by city and calculate statistics
const cityStats = df.groupBy("city").agg({
    age: "mean",
    salary: "max",
    name: "count",
});

console.log(cityStats.toString());

// Alternative: Use individual aggregation methods
const byCity = df.groupBy("city");
const avgAge = byCity.mean().get("age");
const maxSalary = byCity.max().get("salary");
```

## Reading Data

```typescript
// Read CSV file
const df = await BoxFrame.readCsv("data.csv");

// Parse CSV content from string
const df2 = BoxFrame.parseCsv("name,age\nAlice,25\nBob,30");

// Read JSON file
const df3 = await BoxFrame.readJson("data.json");

// Create from array of objects
const df4 = BoxFrame.fromObjects([
    { name: "Alice", age: 25, city: "New York" },
    { name: "Bob", age: 30, city: "London" },
]);

// Create from record of columns
const df5 = BoxFrame.fromRecord({
    name: ["Alice", "Bob"],
    age: [25, 30],
    city: ["New York", "London"],
});

// Create Series from array
const series = BoxFrame.Series([1, 2, 3, 4, 5], { name: "numbers" });

// Read from Google Sheets (public sheets only)
const df6 = await BoxFrame.readGoogleSheet("1INCdqc8IBQDkgj9szIfp4y6o6NYU0-VGnd668zoQK90");
const df7 = await BoxFrame.readGoogleSheetFromUrl(
    "https://docs.google.com/spreadsheets/d/1INCdqc8IBQDkgj9szIfp4y6o6NYU0-VGnd668zoQK90/edit",
);
```

## Next Steps

- Explore the [API Reference](/api) for detailed documentation
- Check out [Examples](/examples) for more complex use cases

This should get you started with BoxFrame!