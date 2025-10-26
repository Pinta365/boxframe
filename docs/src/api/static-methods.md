---
title: "Static Methods"
nav_order: 4
parent: "API Reference"
description: "BoxFrame static factory methods and utilities"
---

# BoxFrame Static Methods

**BoxFrame is a static namespace** - it contains only static methods for creating DataFrames, Series, and performing data operations. You never instantiate BoxFrame. Instead, use it as a factory and utility namespace.

## DataFrame Creation

- `BoxFrame.DataFrame(data: DataFrameInput, options?)` - Create DataFrame
- `BoxFrame.fromRecord(data: Record<string, ColumnData>, options?)` - Create from record
- `BoxFrame.fromObjects(objects: RowData[], options?)` - Create from array of objects

## Series Creation

- `BoxFrame.Series(data: DataValue[], options?)` - Create Series
- `BoxFrame.fromArray(data: DataValue[], options?)` - Create Series from array

## Data I/O

- `BoxFrame.readCsv(pathOrUrl: string, options?)` - Read CSV file or URL
- `BoxFrame.parseCsv(content: string, options?)` - Parse CSV string
- `BoxFrame.readJson(pathOrUrl: string, options?)` - Read JSON file
- `BoxFrame.readGoogleSheet(spreadsheetId: string, sheetName?, options?)` - Read Google Sheets
- `BoxFrame.readGoogleSheetFromUrl(url: string, sheetName?, options?)` - Read Google Sheets from URL

## Data Manipulation

- `BoxFrame.concat(dataFrames: DataFrame[], axis?, options?)` - Concatenate DataFrames
- `BoxFrame.toNumeric(data: Series | DataValue[])` - Convert to numeric
- `BoxFrame.toDatetime(data: Series | DataValue[])` - Convert to datetime
- `BoxFrame.cut(data: Series, bins: number | number[], labels?)` - Cut into bins
- `BoxFrame.dateRange(options?)` - Generate date range
