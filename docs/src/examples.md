---
title: "Examples"
nav_order: 4
description: "Real-world examples and use cases for BoxFrame"
---

# Examples

This page contains practical examples using BoxFrame.

## Basic Data Analysis

### Sales Data Analysis

```typescript
import { DataFrame } from "@pinta365/boxframe";

// Sample sales data
const salesData = new DataFrame({
    date: ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
    product: ["Laptop", "Mouse", "Keyboard", "Laptop", "Monitor"],
    quantity: [2, 5, 3, 1, 2],
    price: [999.99, 29.99, 79.99, 999.99, 299.99],
    region: ["North", "South", "North", "East", "West"],
});

// Calculate total revenue
const withRevenue = salesData.assign({
    revenue: (row) => row.quantity * row.price,
});

// Group by product and calculate totals
const productSummary = withRevenue.groupBy("product").agg({
    quantity: "sum",
    revenue: "sum",
    price: "mean",
});

console.log(productSummary);
```

### Employee Data Processing

```typescript
import { DataFrame } from "@pinta365/boxframe";

// Employee dataset
const employees = new DataFrame({
    id: [1, 2, 3, 4, 5, 6],
    name: ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"],
    department: ["Engineering", "Sales", "Engineering", "HR", "Sales", "Engineering"],
    salary: [75000, 60000, 80000, 55000, 65000, 90000],
    years_experience: [5, 3, 7, 2, 4, 10],
    performance_score: [8.5, 7.2, 9.1, 6.8, 7.9, 9.5],
});

// Find high performers
const highPerformers = employees.query("performance_score > 8.0 and salary > 70000");

// Department statistics
const deptStats = employees.groupBy("department").agg({
    salary: "mean",
    performance_score: "mean",
    years_experience: "mean",
});

// Salary analysis
const salaryAnalysis = employees.assign({
    above_avg: (row) => row.salary > employees.get("salary").mean(),
});
```

## Data Cleaning and Transformation

### Cleaning Messy Data

```typescript
import { DataFrame } from "@pinta365/boxframe";

// Messy dataset with missing values and inconsistencies
const messyData = new DataFrame({
    name: ["John Doe", "Jane Smith", null, "Bob Johnson", ""],
    email: ["john@email.com", "jane@email.com", "invalid-email", "bob@email.com", null],
    age: [25, 30, null, 35, 0],
    city: ["New York", "Los Angeles", "Chicago", "New York", "Boston"],
    salary: [50000, 60000, null, 70000, 45000],
});

// Clean the data
const cleaned = messyData
    .query("name != null and name != '' and age > 0") // Remove invalid rows
    .assign({
        name: (row) => row.name ? row.name.trim().toLowerCase() : row.name,
        email_valid: (row) => row.email && row.email.includes("@") && row.email.includes("."),
    })
    .dropna(); // Remove rows with missing values

console.log("Original rows:", messyData.shape[0]);
console.log("Cleaned rows:", cleaned.shape[0]);
```

### Data Normalization

Data normalization is essential for comparing values across different scales and preparing data for machine learning algorithms.

**Min-Max Normalization (salary_normalized):**
- Scales values to a range between 0 and 1
- Formula: `(value - min) / (max - min)`
- Useful for: Machine learning algorithms, comparing features on the same scale
- Result: 0 = minimum value, 1 = maximum value

**Z-Score Standardization (salary_zscore):**
- Measures how many standard deviations a value is from the mean
- Formula: `(value - mean) / standard_deviation`
- Useful for: Identifying outliers, statistical analysis, comparing across different datasets
- Result: Positive = above average, Negative = below average, ±1 = within normal range

```typescript
// Normalize salary data
const salarySeries = employees.get("salary");
const normalized = employees.assign({
    salary_normalized: (row) =>
        (row.salary - salarySeries.min()) /
        (salarySeries.max() - salarySeries.min()),
    salary_zscore: (row) =>
        (row.salary - salarySeries.mean()) /
        salarySeries.std(),
});

console.log("Normalized data:");
console.log(normalized.toString());
```

## Analytics Examples

### Time Series Analysis

```typescript
import { DataFrame } from "@pinta365/boxframe";

// Stock price data
const stockData = new DataFrame({
    date: ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
    price: [100, 102, 98, 105, 103],
    volume: [1000, 1200, 800, 1500, 1100],
});

// Calculate daily returns
const prices = stockData.get("price").values;
const dailyReturns = prices.map((price, index) => {
    if (index === 0) return 0;
    const prevPrice = prices[index - 1];
    return (price - prevPrice) / prevPrice;
});

const withReturns = stockData.assign({
    daily_return: dailyReturns,
});

// Calculate moving average
const movingAverages = prices.map((_, index) => {
    if (index < 2) return null;
    const window = prices.slice(index - 2, index + 1);
    return window.reduce((sum, price) => sum + price, 0) / window.length;
});

const withMA = withReturns.assign({
    ma_3: movingAverages,
});
```

### Customer Segmentation

Customer segmentation helps identify distinct groups based on demographics and behavior patterns. This example creates age and income groups, then analyzes spending patterns across segments.

```typescript
import { DataFrame } from "@pinta365/boxframe";

// Customer data with diverse age/income combinations
const customers = new DataFrame({
    customer_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
    age: [25, 35, 45, 30, 55, 28, 40, 32, 22, 38, 52, 27, 48, 33, 29, 31, 53],
    income: [50000, 75000, 90000, 60000, 120000, 45000, 85000, 70000, 35000, 95000, 110000, 75000, 80000, 65000, 105000, 58000, 45000],
    spending: [2000, 5000, 8000, 3000, 12000, 1500, 7000, 4000, 1000, 9000, 10000, 3500, 7500, 4500, 9500, 2800, 2000],
    purchases: [5, 12, 20, 8, 25, 3, 15, 10, 2, 18, 22, 8, 16, 11, 19, 7, 4],
});

// Create customer segments
const segmented = customers.assign({
    age_group: (row) => {
        if (row.age < 30) return "Young";
        if (row.age < 50) return "Middle";
        return "Senior";
    },
    income_group: (row) => {
        if (row.income < 60000) return "Low";
        if (row.income < 100000) return "Medium";
        return "High";
    },
    value_score: (row) => row.spending * row.purchases,
});

// Analyze segments
const segmentAnalysis = segmented.groupBy(["age_group", "income_group"]).agg({
    spending: "mean",
    purchases: "mean",
    value_score: "mean",
});

console.log("Customer Segmentation Analysis:");
console.log(segmentAnalysis.toString());
/*
Customer Segmentation Analysis:
DataFrame
shape: [7, 3]

              spending_mean purchases_mean value_score_mean
Young|Low     1500          3.333333       5500
Middle|Medium 6000          13.75          90062.5
Senior|High   11000         23.5           260000
Young|Medium  3500          8              28000
Young|High    9500          19             180500
Middle|Low    2800          7              19600
Senior|Low    2000          4              8000
*/
```
## Performance Optimization

### Large Dataset Processing

```typescript
import { DataFrame } from "@pinta365/boxframe";

// For large datasets, use appropriate operations
const largeDataset = new DataFrame({
    id: Array.from({ length: 100000 }, (_, i) => i),
    value: Array.from({ length: 100000 }, () => Math.random() * 100),
    category: Array.from({ length: 100000 }, () => ["A", "B", "C", "D"][Math.floor(Math.random() * 4)]),
});

// Filtering and grouping
const results = largeDataset
    .query("value > 50")
    .groupBy("category")
    .agg({
        value: "mean",
        id: "count",
    });

console.log("Processed", results.shape[0], "groups");
```

### Streaming Large CSV Files

```typescript
import { parseCsvBatchedStream } from "@pinta365/boxframe";

// Stream a large CSV in batches and write to a database incrementally
await parseCsvBatchedStream("/data/huge.csv", {
    hasHeader: true,
    batchSize: 50000,
    onBatch: async (df) => {
        // Replace with your persistence/aggregation logic
        console.log("Inserting rows:", df.shape[0]);
        // await insertBatch(df);
    },
    onProgress: ({ bytesRead, rowsProcessed }) => {
        console.log("progress:", { bytesRead, rowsProcessed });
    },
});

```

## Integration Examples

### Working with APIs

```typescript
import { BoxFrame } from "@pinta365/boxframe";

// Fetch data from API and process
async function processAPIData() {
    const response = await fetch("https://api.example.com/data");
    const jsonData = await response.json();

    const df = BoxFrame.fromObjects(jsonData);

    // Process the data
    const processed = df
        .query("status == 'active'")
        .groupBy("category")
        .agg({
            value: "sum",
        });

    return processed;
}
```

### Export Results

```typescript
// Process data and export
const results = df.groupBy("category").agg({
    value: "sum",
});

// Export to different formats
const jsonOutput = results.toJSON();

// Save to files
await Deno.writeTextFile("results.json", JSON.stringify(jsonOutput, null, 2));
```

These examples show BoxFrame for various data analysis tasks. The library handles data exploration and analytical workflows.
