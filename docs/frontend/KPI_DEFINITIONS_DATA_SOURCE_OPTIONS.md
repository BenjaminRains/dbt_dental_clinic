# KPI Definitions Data Source Options - Detailed Explanation

This document provides detailed explanations of Options A and B for sourcing KPI definitions data for the frontend KPI Definitions component.

## Overview

Both options aim to make KPI definitions from `dbt_dental_models/models/marts/exposures.yml` accessible in the frontend, but they differ in implementation complexity, maintenance burden, and synchronization strategy.

---

## Option A: Extract from `exposures.yml` at Build Time

### Concept

Parse the YAML file (`exposures.yml`) during the frontend build process, extract KPI definitions, and convert them into a format the frontend can consume (TypeScript/JSON).

### Implementation Approach

#### 1. **Build-Time Processing**

You would need to:

**a. Install a YAML parser:**
```bash
npm install --save-dev js-yaml @types/js-yaml
```

**b. Create a build script or Vite plugin:**

**Option A1: Vite Plugin (Recommended)**
Create `frontend/vite-plugins/yaml-loader.ts`:
```typescript
import { Plugin } from 'vite';
import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';

export function kpiDefinitionsLoader(): Plugin {
  return {
    name: 'kpi-definitions-loader',
    buildStart() {
      // Read exposures.yml from dbt_dental_models directory
      const yamlPath = path.resolve(__dirname, '../../dbt_dental_models/models/marts/exposures.yml');
      const yamlContent = fs.readFileSync(yamlPath, 'utf-8');
      const exposures = yaml.load(yamlContent) as any;
      
      // Extract KPI definitions
      const kpiDefinitions = extractKPIDefinitions(exposures);
      
      // Write to TypeScript file
      const outputPath = path.resolve(__dirname, '../src/data/kpiDefinitions.generated.ts');
      const tsContent = generateTypeScript(kpiDefinitions);
      fs.writeFileSync(outputPath, tsContent);
    }
  };
}

function extractKPIDefinitions(exposures: any): KPIDefinitions {
  // Parse YAML structure and extract relevant fields
  // Transform dbt exposure format to frontend format
}

function generateTypeScript(definitions: KPIDefinitions): string {
  return `// Auto-generated from exposures.yml - DO NOT EDIT MANUALLY
export const kpiDefinitions = ${JSON.stringify(definitions, null, 2)} as const;
`;
}
```

**Option A2: Pre-build Script**
Create `frontend/scripts/generate-kpi-definitions.js`:
```javascript
const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

const yamlPath = path.join(__dirname, '../../dbt_dental_models/models/marts/exposures.yml');
const outputPath = path.join(__dirname, '../src/data/kpiDefinitions.generated.ts');

const yamlContent = fs.readFileSync(yamlPath, 'utf-8');
const exposures = yaml.load(yamlContent);

// Extract and transform
const kpiDefinitions = extractKPIDefinitions(exposures);

// Generate TypeScript file
const tsContent = `// Auto-generated from exposures.yml
export const kpiDefinitions = ${JSON.stringify(kpiDefinitions, null, 2)} as const;
`;

fs.writeFileSync(outputPath, tsContent);
```

Then add to `package.json`:
```json
{
  "scripts": {
    "prebuild": "node scripts/generate-kpi-definitions.js",
    "build": "tsc && vite build"
  }
}
```

#### 2. **YAML Structure Parsing**

The `exposures.yml` file has a structure like:
```yaml
exposures:
  - name: executive_dashboard
    description: >
      **Executive Dashboard - High-Level KPI Overview**
      
      ## Key Metrics Displayed:
      - **Revenue Lost**: Total revenue from...
      - **Recovery Potential**: Estimated recoverable...
      
      ## Quick Statistics Definitions:
      
      ### Total Production:
      **Definition**: Total billed revenue...
      **Calculation**: 
        - **Source**: `mart_provider_performance.total_production`
        - **Formula**: Sum of all `fact_procedure.actual_fee`...
```

You'd need to:
- Parse the YAML structure
- Extract exposure names, descriptions
- Parse markdown-formatted definitions
- Transform into a structured format for the frontend

#### 3. **Transformation Logic**

Convert from dbt exposure format to frontend format:
```typescript
interface ExposureDefinition {
  name: string;
  description: string; // Markdown-formatted
  // ... other dbt fields
}

interface FrontendKPIDefinition {
  dashboardName: string;
  dashboardPath: string;
  kpis: Array<{
    name: string;
    description: string;
    calculation?: string;
    dataSource?: string;
    businessContext?: string;
  }>;
}
```

### Advantages

1. **Single Source of Truth**
   - KPI definitions exist only in `exposures.yml`
   - No duplicate data to maintain
   - Changes to dbt docs automatically flow to frontend

2. **Consistency**
   - Frontend and dbt documentation stay in sync
   - No risk of definitions diverging

3. **Maintainability**
   - Update definitions in one place (exposures.yml)
   - Build process ensures frontend gets latest definitions

4. **Automation**
   - No manual copying/updating needed
   - Build process handles synchronization

### Disadvantages

1. **Complexity**
   - Requires YAML parser and build tooling
   - Need to write parsing/transformation logic
   - More moving parts in build process

2. **Build Dependencies**
   - Frontend build depends on dbt project structure
   - Path dependencies (must maintain relative paths)
   - Build fails if YAML structure changes unexpectedly

3. **Parsing Challenges**
   - YAML descriptions contain markdown
   - Need to parse/extract structured data from markdown
   - Complex nested structures in exposures.yml

4. **Development Overhead**
   - Longer initial setup time
   - Need to test build process
   - Debugging build-time errors

5. **Type Safety Challenges**
   - YAML parsing results in `any` types
   - Need runtime validation or type assertions
   - Less TypeScript type safety during development

### When to Use

- **Good for:** Long-term projects where you want automatic synchronization
- **Good for:** Teams that frequently update dbt documentation
- **Good for:** Projects where consistency is critical
- **Not ideal for:** Rapid prototyping or MVP
- **Not ideal for:** Projects where dbt structure changes frequently

---

## Option B: Create Separate TypeScript/JSON File

### Concept

Manually create a separate file (TypeScript or JSON) containing KPI definitions, structured for frontend consumption. This file is maintained separately from `exposures.yml`, though it may reference it as the source of truth.

### Implementation Approach

#### 1. **Create Data File**

**Option B1: TypeScript File (Recommended)**
Create `frontend/src/data/kpiDefinitions.ts`:
```typescript
export interface KPIDefinition {
  name: string;
  description: string;
  calculation?: string;
  dataSource?: string;
  businessContext?: string;
}

export interface DashboardKPIs {
  dashboardName: string;
  dashboardPath: string;
  kpis: KPIDefinition[];
}

export const kpiDefinitions: DashboardKPIs[] = [
  {
    dashboardName: 'Executive Dashboard',
    dashboardPath: '/dashboard',
    kpis: [
      {
        name: 'Revenue Lost',
        description: 'Total revenue from missed appointments, no-shows, and cancellations.',
        calculation: 'Sum of scheduled_production_amount for missed appointments',
        dataSource: 'mart_revenue_lost',
        businessContext: 'Represents potential revenue that was scheduled but not realized.',
      },
      // ... more KPIs
    ],
  },
  // ... more dashboards
];
```

**Option B2: JSON File**
Create `frontend/src/data/kpiDefinitions.json`:
```json
[
  {
    "dashboardName": "Executive Dashboard",
    "dashboardPath": "/dashboard",
    "kpis": [
      {
        "name": "Revenue Lost",
        "description": "Total revenue from missed appointments...",
        "calculation": "Sum of scheduled_production_amount...",
        "dataSource": "mart_revenue_lost",
        "businessContext": "Represents potential revenue..."
      }
    ]
  }
]
```

Then import in TypeScript:
```typescript
import kpiDefinitions from './data/kpiDefinitions.json';
```

#### 2. **Import and Use**

In your component:
```typescript
import { kpiDefinitions } from '../data/kpiDefinitions';

const KPIDefinitions: React.FC = () => {
  // Use kpiDefinitions directly
  return (
    // ... component code
  );
};
```

#### 3. **Manual Maintenance Process**

When `exposures.yml` is updated:
1. Review changes in `exposures.yml`
2. Manually update `kpiDefinitions.ts` or `.json`
3. Test frontend component
4. Commit both files

### Advantages

1. **Simplicity**
   - No build-time processing needed
   - No additional dependencies
   - Straightforward file structure

2. **Type Safety (TypeScript option)**
   - Full TypeScript type checking
   - IDE autocomplete and IntelliSense
   - Compile-time validation

3. **Fast Development**
   - Quick to set up and modify
   - Easy to iterate on structure
   - No build process complexity

4. **Independence**
   - Frontend build doesn't depend on dbt project
   - Can structure data optimally for frontend
   - No path dependencies

5. **Easier Debugging**
   - Clear, readable source file
   - Easy to inspect and modify
   - No hidden build-time transformations

6. **Flexibility**
   - Can format data exactly as needed for UI
   - Can add frontend-specific fields
   - Not constrained by dbt YAML structure

### Disadvantages

1. **Manual Synchronization**
   - Must update both `exposures.yml` and frontend file
   - Risk of definitions getting out of sync
   - Requires discipline to keep them aligned

2. **Duplicate Data**
   - Definitions exist in two places
   - More places to update when definitions change
   - Potential for inconsistencies

3. **Maintenance Burden**
   - Every change requires manual updates
   - Need to remember to update both files
   - Higher risk of forgetting to sync

4. **No Automatic Updates**
   - Changes to dbt docs don't flow automatically
   - Requires manual intervention for every update

### When to Use

- **Good for:** Rapid development and prototyping
- **Good for:** Projects with infrequent definition changes
- **Good for:** MVP or initial implementation
- **Good for:** Teams with strong code review processes
- **Not ideal for:** Projects with frequent definition updates
- **Not ideal for:** Large teams where sync might be missed

---

## Comparison Summary

| Aspect | Option A (Build-Time YAML) | Option B (Separate File) |
|--------|---------------------------|-------------------------|
| **Setup Complexity** | High (requires build tooling) | Low (just create file) |
| **Maintenance** | Automatic (single source) | Manual (duplicate data) |
| **Build Time** | Longer (parsing step) | Faster (no parsing) |
| **Type Safety** | Moderate (runtime parsing) | High (TypeScript) |
| **Sync Risk** | Low (automatic) | High (manual) |
| **Flexibility** | Limited (YAML structure) | High (custom structure) |
| **Development Speed** | Slower (complex setup) | Faster (simple setup) |
| **Long-term Maintenance** | Lower (automatic) | Higher (manual) |

---

## Recommendation

**Start with Option B, migrate to Option A later** (as noted in the investigation document).

### Why Start with Option B:

1. **Faster Time to Value**
   - Get Phase 1 working quickly
   - Validate the UI/UX approach
   - Establish data structure requirements

2. **Lower Risk**
   - Simple implementation reduces bugs
   - Easy to iterate on structure
   - No build process complexity to debug

3. **Learning Opportunity**
   - Understand what data structure works best
   - Identify which fields are most important
   - Refine the UI before investing in automation

### When to Migrate to Option A:

- After Phase 2 (documenting all KPIs) is complete
- When you have a stable data structure
- When you've identified synchronization issues
- When maintenance burden becomes noticeable
- When you have time for build tooling investment

### Hybrid Approach (Advanced):

You could also implement a **hybrid approach**:
- Use Option B for initial data structure
- Create a script (not build-time) to help sync from YAML
- Run script manually when needed: `npm run sync-kpi-definitions`
- Migrate to full build-time automation later if needed

---

## Implementation Notes for Current Project

Given that:
- You're using **Vite** as the build tool
- The frontend is in `frontend/` directory
- The dbt project is at `dbt_dental_models/models/marts/exposures.yml`
- You're in **Phase 1** (initial implementation)

**Recommended Path:**
1. **Now (Phase 1):** Use Option B - TypeScript file
   - Create `frontend/src/data/kpiDefinitions.ts`
   - Populate with initial structure (as started in KPIDefinitions.tsx)
   - Get the component working

2. **Phase 2:** Continue with Option B
   - Extract full definitions from `exposures.yml`
   - Populate the TypeScript file with complete data
   - Refine structure as needed

3. **Future (if needed):** Consider Option A
   - If sync issues arise
   - If definitions change frequently
   - When you have time for build tooling

This approach balances speed of implementation with long-term maintainability.
