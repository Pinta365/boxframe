# BoxFrame

**DataFrames for JavaScript** - A high-performance data analysis library with WebAssembly acceleration. Inspired by Pandas.

**Cross-Platform**: Works in Deno, Node.js, Bun, and browsers

## Quick Start

```typescript
import { DataFrame } from "@pinta365/boxframe";

const df = new DataFrame({
    name: ["Alice", "Bob", "Charlie"],
    age: [25, 30, 35],
    salary: [50000, 60000, 70000],
});

// Find high earners
const highEarners = df.query("salary > 55000");
console.log(highEarners.toString());
```

## Installation

```bash
# Deno
deno add jsr:@pinta365/boxframe

# Node.js
npx jsr add @pinta365/boxframe

# Bun
bunx jsr add @pinta365/boxframe
```

## Documentation

📚 **[Complete Documentation](https://boxframe.pinta.land)** - API reference, examples, and guides

## License

MIT License - see [LICENSE](LICENSE) file for details.
