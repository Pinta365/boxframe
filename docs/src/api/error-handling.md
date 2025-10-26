---
title: "Error Handling"
nav_order: 6
parent: "API Reference"
description: "Error classes and exception handling"
---

# Error Handling

BoxFrame uses standard JavaScript `Error` objects with descriptive messages for comprehensive error handling.

## Error Types

BoxFrame throws standard `Error` objects with specific messages for different scenarios:

### Column Errors
- `Column 'columnName' not found` - When accessing non-existent columns
- `Column 'columnName' length (X) must match index length (Y)` - When column lengths don't match

### Index Errors  
- `Index length (X) must match data length (Y)` - When index and data lengths don't match
- `Mask length (X) must match DataFrame length (Y)` - When filter masks don't match data length

### Query Errors
- `Column 'columnName' not found in DataFrame` - When querying non-existent columns
- `Invalid condition: missing value after operator` - When query syntax is invalid
- `Query evaluation failed: [details]` - When query parsing fails

### Data Validation Errors
- `At least one DataFrame must be provided` - When concatenating empty arrays
- `Axis must be 0 or 1` - When invalid axis is specified
- `Number of bins must be positive` - When invalid bin parameters are provided

## Error Handling Best Practices

```typescript
try {
    const df = new DataFrame(data);
    const result = df.get('nonExistentColumn');
} catch (error) {
    if (error instanceof Error) {
        if (error.message.includes('not found')) {
            console.log('Column not found:', error.message);
        } else if (error.message.includes('length')) {
            console.log('Length mismatch:', error.message);
        } else {
            console.log('Unexpected error:', error.message);
        }
    }
}
```

## Common Error Patterns

```typescript
// Check for specific error types by message content
if (error.message.includes('not found')) {
    // Handle missing column/index errors
} else if (error.message.includes('length')) {
    // Handle length mismatch errors  
} else if (error.message.includes('Invalid')) {
    // Handle validation errors
}
```
