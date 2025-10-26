---
title: "Series API"
nav_order: 2
parent: "API Reference"
description: "Series class methods and properties"
---

# Series API

One-dimensional labeled array.

## Constructor

```typescript
new Series(data: DataValue[], options?: SeriesOptions)
```

## Properties

- `dtype` - Get the data type of the Series
- `name` - Get the name of the Series
- `length` - Get the number of elements
- `shape` - Get the shape as a tuple [number_of_elements]

## Data Access

- `get(label: Index)` - Get value by label
- `iloc(position: number)` - Get value by position
- `loc(labels: Index[])` - Get values by labels
- `ilocRange(positions: number[])` - Get values by positions

## Basic Operations

- `head(n?: number)` - Return first n elements
- `tail(n?: number)` - Return last n elements
- `unique()` - Return unique values
- `valueCounts()` - Count value frequencies
- `nunique(dropna?: boolean)` - Count unique values
- `isUnique()` - Check if all values are unique

## Statistical Methods

- `mean()` - Calculate mean
- `std()` - Calculate standard deviation
- `min()` - Find minimum value
- `max()` - Find maximum value
- `sum()` - Calculate sum
- `count()` - Count non-null values

## Data Manipulation

- `filter(mask: boolean[])` - Filter using boolean mask
- `drop(labels: Index[])` - Drop values by labels
- `rename(name: string)` - Rename the series
- `isin(values: T[])` - Check if values are in list
- `sortValues(ascending?: boolean)` - Sort values
- `sortIndex(ascending?: boolean)` - Sort by index
- `resetIndex()` - Reset the index

## Data Cleaning

- `isnull()` - Check for null values
- `notnull()` - Check for non-null values
- `dropna()` - Remove null values
- `fillna(value: T)` - Fill null values

## Grouping

- `groupBy(by?: GroupByKey, options?: GroupByOptions)` - Group for aggregation
