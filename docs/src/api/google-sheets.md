---
title: "GoogleSheets API"
nav_order: 5
parent: "API Reference"
description: "GoogleSheets class for reading public Google Sheets"
---

# GoogleSheets API

Provides methods to read data from public Google Sheets.

## Static Methods

- `GoogleSheets.readSheet(spreadsheetId: string, sheetName?, options?)` - Read Google Sheets
- `GoogleSheets.readFromUrl(url: string, sheetName?, options?)` - Read from Google Sheets URL
- `GoogleSheets.extractId(url: string)` - Extract spreadsheet ID from URL

## Parameters

- `spreadsheetId` - The Google Sheets ID from the URL
- `sheetName` - Name of the sheet (default: "Sheet1")
- `options` - CSV parsing options
- `url` - Full Google Sheets URL

## Example

```typescript
import { GoogleSheets } from "@pinta365/boxframe";

// From spreadsheet ID
const df = await GoogleSheets.readSheet("1ABC123...");

// From full URL
const df2 = await GoogleSheets.readFromUrl(
    "https://docs.google.com/spreadsheets/d/1ABC123.../edit"
);

// Specific sheet with options
const df3 = await GoogleSheets.readSheet("1ABC123...", "Data", {
    delimiter: ",",
    hasHeader: true
});
```

## Notes

- Only works with **public** Google Sheets
- Sheets must be publicly accessible
- Uses Google's CSV export API
