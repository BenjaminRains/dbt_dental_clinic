# Vite Chunking Strategy - Simplified & Scalable Approach

## Problem Statement

We were experiencing initialization errors:
- `Cannot read properties of undefined (reading 'useLayoutEffect')` - React not available
- `Cannot access 'Lc' before initialization` - Recharts not finding React

**Root Cause**: Splitting React-dependent libraries into separate chunks creates loading order issues. When chunks load asynchronously, dependencies aren't guaranteed to be available.

## Solution: Dependency-Based Chunking

### Core Principle
**Bundle by dependency type, not by library type.**

Instead of splitting libraries arbitrarily (React, MUI, Charts), we bundle by what they depend on:
- **React-dependent**: Bundle together (React DOM, Router, MUI, Emotion, Recharts)
- **Independent**: Can be separate (Axios, Zustand, Mermaid)

### Current Strategy

```
Entry Bundle (main.js)
├── React Core (~45KB) - NOT chunked, always available first
│
└── Chunks (loaded after entry):
    ├── react-vendor.js
    │   ├── React DOM
    │   ├── React Router
    │   ├── MUI Material
    │   ├── MUI Icons
    │   ├── Emotion
    │   └── Recharts (React-based, must be with React)
    │
    ├── mermaid-vendor.js (dynamically imported)
    ├── utils-vendor.js (axios, zustand)
    └── vendor.js (other independent libraries)
```

### Why This Works

1. **React Core in Entry**: 
   - Small (~45KB), always loads first
   - Guarantees React is available before any chunks

2. **All React-Dependent Code Together**:
   - Prevents initialization errors
   - Clear dependency boundary
   - Easier to maintain

3. **Independent Libraries Separate**:
   - Still allows code splitting
   - Can load in any order
   - Better caching for rarely-used libraries

## Implementation

```typescript
manualChunks: (id) => {
    if (id.includes('node_modules')) {
        // React core - keep in entry (small, must load first)
        if (id.includes('/react/') && 
            !id.includes('react-dom') && 
            !id.includes('react-router') &&
            !id.includes('react/jsx-runtime')) {
            return undefined; // Keep in entry bundle
        }

        // ALL React-dependent libraries together
        if (id.includes('react-dom') ||
            id.includes('react-router') ||
            id.includes('@mui/material') ||
            id.includes('@mui/icons-material') ||
            id.includes('@emotion') ||
            id.includes('recharts')) {  // ← Recharts is React-based!
            return 'react-vendor';
        }

        // Independent libraries - can be separate
        if (id.includes('mermaid')) return 'mermaid-vendor';
        if (id.includes('axios') || id.includes('zustand')) return 'utils-vendor';
        
        return 'vendor'; // Other independent libraries
    }
}
```

## Key Insights

### 1. Recharts is React-Dependent
- Recharts is a React component library
- It uses React hooks internally
- Must be bundled with React-dependent code
- **Mistake**: Separating it caused initialization errors

### 2. React Core Should Stay in Entry
- Small size (~45KB) - acceptable in entry bundle
- Must be available before any chunks load
- Prevents all initialization errors
- Trade-off: Slightly larger entry bundle, but much more reliable

### 3. Dependency Boundaries Matter
- **React-dependent**: Bundle together
- **Independent**: Can be separate
- Clear rules make maintenance easier

## Scaling Strategy

### As You Add Dependencies

**Rule**: If a library depends on React, add it to `react-vendor`:

```typescript
// Example: Adding a new React library
if (id.includes('react-dom') ||
    id.includes('react-router') ||
    id.includes('@mui/material') ||
    id.includes('@mui/icons-material') ||
    id.includes('@emotion') ||
    id.includes('recharts') ||
    id.includes('@tanstack/react-table') ||  // ← New React library
    id.includes('react-hook-form')) {       // ← Another React library
    return 'react-vendor';
}
```

**Rule**: If a library is independent, it can be separate:

```typescript
// Example: Adding an independent library
if (id.includes('lodash')) return 'utils-vendor';
if (id.includes('date-fns')) return 'utils-vendor';
```

### When to Split Further

Only split `react-vendor` if:
1. It grows > 1MB (gzipped)
2. You have clear usage patterns (e.g., charts only on certain pages)
3. You can lazy-load the split chunk

**Example**: If Recharts is only used on 2 pages, you could:
```typescript
// Lazy load Recharts only when needed
const RechartsChart = lazy(() => 
    import('recharts').then(m => ({ default: m.BarChart }))
);
```

But for now, bundling together is simpler and more reliable.

## Bundle Size Targets

- **Entry bundle**: < 100KB (gzipped) - includes React core
- **react-vendor**: < 500KB (gzipped) - all React-dependent code
- **Other chunks**: < 200KB each (gzipped)

## Monitoring

### Check Bundle Sizes
```bash
npm run build
# Look at dist/assets/ sizes
```

### Use Bundle Analyzer
```bash
npm install --save-dev rollup-plugin-visualizer
```

Add to vite.config.ts:
```typescript
import { visualizer } from 'rollup-plugin-visualizer';

plugins: [
    react(),
    visualizer({ open: true, filename: 'dist/stats.html' })
]
```

## Future Optimizations

### Phase 1: Tree-Shake MUI Icons (Quick Win)
- Create `src/icons/index.ts` with only used icons
- Saves ~800KB-1MB immediately
- See `CHUNKING_STRATEGY_ANALYSIS.md` for details

### Phase 2: Lazy Load Heavy Features
- Lazy load Recharts on pages that need it
- Lazy load MUI DataGrid (if you add it)
- Route-based code splitting (already implemented)

### Phase 3: Advanced Splitting (If Needed)
- Split MUI Icons from MUI Core (if icons grow)
- Split Recharts from MUI (if react-vendor > 1MB)
- Route-specific vendor chunks

## Troubleshooting

### If You Get Initialization Errors

1. **Check**: Is the library React-dependent?
   - If yes → Add to `react-vendor`
   - If no → Can be separate

2. **Check**: Is React core in entry bundle?
   - Should return `undefined` for React core
   - Check build output: React should be in main bundle

3. **Check**: Are all React-dependent libraries together?
   - React DOM, Router, MUI, Emotion, Recharts should all be in `react-vendor`

### Common Mistakes

❌ **Splitting Recharts**: It's React-based, must be with React
❌ **Splitting Emotion from MUI**: Emotion is MUI's dependency
❌ **Chunking React core**: Must stay in entry bundle
✅ **Bundling all React-dependent code**: Correct approach

## Conclusion

**Simpler is better.** Bundling all React-dependent code together:
- Prevents initialization errors
- Easier to maintain
- Clear dependency boundaries
- Still allows code splitting for independent libraries

**Trade-off**: Slightly larger `react-vendor` chunk, but much more reliable and maintainable.
