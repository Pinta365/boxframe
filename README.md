# BoxFrame

> **⚠️ Notice: This repository has been moved to [cross-org/boxframe](https://github.com/cross-org/boxframe).**
> 
> **Development continues! The project is not being stopped—it has simply moved to a new home under the [cross-org](https://github.com/cross-org) organization. Please use [@cross/boxframe](https://jsr.io/@cross/boxframe) instead. This repository is being archived.**

**DataFrames for JavaScript** - A high-performance data analysis library with WebAssembly acceleration. Inspired by Pandas.

**Cross-Platform**: Works in Deno, Node.js, Bun, and browsers

## Quick Start

```typescript
import { DataFrame } from "@cross/boxframe";

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
deno add jsr:@cross/boxframe

# Node.js
npx jsr add @cross/boxframe

# Bun
bunx jsr add @cross/boxframe
```

### Browser

```html
<script type="module">
    import { DataFrame } from "https://esm.sh/jsr/@cross/boxframe@0.0.1";
    // Use DataFrame in your browser app
</script>
```

**Try it live:** [JSFiddle Demo](https://jsfiddle.net/pinta365/e9L8ynmr/)

## Documentation

📚 **[Complete Documentation](https://boxframe.pinta.land)** - API reference, examples, and guides

## License

MIT License - see [LICENSE](LICENSE) file for details.
