---
title: "BoxFrame"
nav_order: 1
description: "A DataFrame library for JavaScript"
---

# BoxFrame

**DataFrames for JavaScript**

BoxFrame is a DataFrame library for JavaScript/TypeScript. Built with WebAssembly (WASM) compiled
from Rust for performance, it provides an intuitive API for data manipulation, analysis, and processing that works across different JavaScript environments. Inspired by Pandas.

## Quick Start

```typescript
import { DataFrame } from "@pinta365/boxframe";

// Sales data analysis
const sales = new DataFrame({
    product: ["Laptop", "Phone", "Tablet", "Laptop", "Phone"],
    price: [999, 699, 399, 1099, 799],
    region: ["North", "South", "North", "South", "West"],
    quantity: [5, 12, 8, 5, 15]
});

// Find best selling products by region
const topProducts = sales.groupBy("region").agg({
    price: "mean",
    quantity: "sum"
}).sortValues("quantity_sum", false);

console.log(topProducts.toString());
/*
Outputs:
DataFrame
shape: [3, 2]

      price_mean quantity_sum
South 899        17
West  799        15
North 699        13
*/
```

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

#### Browser (ESM)
For browser usage, you can import BoxFrame directly from esm.sh:

```html
<script type="module">
import { DataFrame } from "https://esm.sh/jsr/@pinta365/boxframe@0.0.1";

// Use BoxFrame in your browser application
const df = new DataFrame({
    name: ["Alice", "Bob", "Charlie"],
    age: [25, 30, 35]
});

console.log(df.toString());
</script>
```

Ready to get started? Check out our [Getting Started Guide](/getting-started) or explore the [API Reference](/api).
