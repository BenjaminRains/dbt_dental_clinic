# AR Aging Dashboard Development Plan

## Overview

Build a comprehensive Accounts Receivable Aging Dashboard that leverages the existing `mart_ar_summary` mart. This dashboard will provide collection management capabilities with aging analysis, risk categorization, and priority scoring.

**Target Timeline**: 4-6 hours  
**Status**: Mart exists (`mart_ar_summary`), frontend/API missing  
**Priority**: HIGH - Critical for billing team daily operations

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend Layer                             │
├─────────────────────────────────────────────────────────────┤
│  React Component: AR.tsx                                    │
│  ├── KPI Summary Cards                                       │
│  ├── AR Aging Chart (Stacked Bar)                           │
│  ├── Collection Priority Queue Table                        │
│  ├── Risk Distribution Pie Chart                             │
│  └── Filters (Date Range, Provider, Risk, Balance)          │
└───────────────────────────┬─────────────────────────────────┘
                            │ HTTP/REST
┌───────────────────────────▼─────────────────────────────────┐
│                    API Layer (FastAPI)                       │
├─────────────────────────────────────────────────────────────┤
│  Router: api/routers/ar.py                                   │
│  ├── GET /ar/kpi-summary                                     │
│  ├── GET /ar/aging-summary                                   │
│  ├── GET /ar/priority-queue                                  │
│  ├── GET /ar/risk-distribution                               │
│  └── GET /ar/aging-trends                                    │
│                                                               │
│  Service: api/services/ar_service.py                         │
│  ├── get_ar_kpi_summary()                                    │
│  ├── get_ar_aging_summary()                                  │
│  ├── get_ar_priority_queue()                                 │
│  ├── get_ar_risk_distribution()                              │
│  └── get_ar_aging_trends()                                   │
│                                                               │
│  Models: api/models/ar.py                                    │
│  ├── ARKPISummary                                            │
│  ├── ARAgingSummary                                          │
│  ├── ARPriorityQueueItem                                     │
│  ├── ARRiskDistribution                                      │
│  └── ARAgingTrend                                            │
└───────────────────────────┬─────────────────────────────────┘
                            │ SQLAlchemy
┌───────────────────────────▼─────────────────────────────────┐
│                    Data Layer                                │
├─────────────────────────────────────────────────────────────┤
│  Database: PostgreSQL                                        │
│  Schema: raw_marts                                           │
│  Table: mart_ar_summary                                      │
│                                                               │
│  Key Columns:                                                │
│  ├── total_balance, balance_0_30_days, etc.                 │
│  ├── pct_current, pct_over_90                               │
│  ├── aging_risk_category                                     │
│  ├── collection_priority_score                              │
│  └── payment_recency                                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Development Process

### Phase 1: Backend Development (2-3 hours)

#### Step 1.1: Create Pydantic Models (`api/models/ar.py`)

**Purpose**: Define data structures for API request/response validation

**Models to Create**:

```python
# api/models/ar.py
from pydantic import BaseModel
from typing import Optional, List
from datetime import date

class ARKPISummary(BaseModel):
    """KPI summary for AR dashboard"""
    total_ar_outstanding: float
    current_amount: float  # 0-30 days
    current_percentage: float
    over_90_amount: float
    over_90_percentage: float
    dso_days: float  # Days Sales Outstanding
    collection_rate: float
    high_risk_count: int
    high_risk_amount: float

class ARAgingSummary(BaseModel):
    """Aging summary by bucket"""
    aging_bucket: str  # "0-30", "31-60", "61-90", "90+"
    amount: float
    percentage: float
    patient_count: int

class ARPriorityQueueItem(BaseModel):
    """Individual AR item for priority queue"""
    patient_id: int
    provider_id: int
    patient_name: str  # From dim_patient join
    provider_name: str  # From dim_provider join
    total_balance: float
    balance_0_30_days: float
    balance_31_60_days: float
    balance_61_90_days: float
    balance_over_90_days: float
    aging_risk_category: str
    collection_priority_score: int
    days_since_last_payment: Optional[int]
    payment_recency: str
    collection_rate: Optional[float]

class ARRiskDistribution(BaseModel):
    """Risk category distribution"""
    risk_category: str
    patient_count: int
    total_amount: float
    percentage: float

class ARAgingTrend(BaseModel):
    """Aging trends over time"""
    date: date
    current_amount: float
    over_30_amount: float
    over_60_amount: float
    over_90_amount: float
    total_amount: float
```

**Files to Create**:
- `api/models/ar.py` (new file)

**Estimated Time**: 30 minutes

---

#### Step 1.2: Create Service Layer (`api/services/ar_service.py`)

**Purpose**: Business logic and database queries

**Functions to Implement**:

```python
# api/services/ar_service.py
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from datetime import date

def get_ar_kpi_summary(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> dict:
    """
    Get AR KPI summary from mart_ar_summary
    
    Returns:
    - Total AR outstanding
    - Current (0-30) amount and percentage
    - Over 90 days amount and percentage
    - DSO (Days Sales Outstanding)
    - Collection rate
    - High risk counts and amounts
    """
    # Query mart_ar_summary for latest snapshot_date per patient
    # Aggregate: SUM balances, COUNT patients, AVG collection_rate
    
def get_ar_aging_summary(
    db: Session,
    snapshot_date: Optional[date] = None
) -> List[dict]:
    """
    Get AR aging summary by bucket
    
    Returns list of aging buckets with amounts, percentages, patient counts
    """
    # Group by aging buckets, sum amounts
    
def get_ar_priority_queue(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    min_priority_score: Optional[int] = None,
    risk_category: Optional[str] = None,
    min_balance: Optional[float] = None,
    provider_id: Optional[int] = None
) -> List[dict]:
    """
    Get AR priority queue sorted by collection_priority_score
    
    Joins:
    - dim_patient for patient_name
    - dim_provider for provider_name
    
    Filters:
    - min_priority_score: Minimum priority score threshold
    - risk_category: Filter by aging_risk_category
    - min_balance: Minimum total_balance threshold
    - provider_id: Filter by specific provider
    """
    # JOIN dim_patient and dim_provider
    # Filter by parameters
    # ORDER BY collection_priority_score DESC
    # LIMIT/OFFSET for pagination
    
def get_ar_risk_distribution(
    db: Session,
    snapshot_date: Optional[date] = None
) -> List[dict]:
    """
    Get risk category distribution
    
    Groups by aging_risk_category, counts patients, sums amounts
    """
    
def get_ar_aging_trends(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[dict]:
    """
    Get AR aging trends over time
    
    Groups by snapshot_date, aggregates by aging buckets
    """
```

**Key Query Patterns**:

1. **Latest Snapshot per Patient**:
```sql
WITH latest_snapshots AS (
    SELECT DISTINCT ON (patient_id, provider_id)
        *
    FROM raw_marts.mart_ar_summary
    ORDER BY patient_id, provider_id, snapshot_date DESC
)
SELECT ...
```

2. **Aging Bucket Aggregation**:
```sql
SELECT 
    '0-30' as aging_bucket,
    SUM(balance_0_30_days) as amount,
    COUNT(*) as patient_count
FROM raw_marts.mart_ar_summary
WHERE snapshot_date = :snapshot_date
```

3. **Priority Queue with Joins**:
```sql
SELECT 
    mas.*,
    dp.first_name || ' ' || dp.last_name as patient_name,
    dpr.last_name || ', ' || dpr.first_name as provider_name
FROM raw_marts.mart_ar_summary mas
JOIN raw_marts.dim_patient dp ON mas.patient_id = dp.patient_id
JOIN raw_marts.dim_provider dpr ON mas.provider_id = dpr.provider_id
WHERE mas.collection_priority_score >= :min_score
ORDER BY mas.collection_priority_score DESC
LIMIT :limit OFFSET :skip
```

**Files to Create**:
- `api/services/ar_service.py` (new file)

**Estimated Time**: 1.5 hours

---

#### Step 1.3: Create Router (`api/routers/ar.py`)

**Purpose**: API endpoint definitions and request handling

**Endpoints to Create**:

```python
# api/routers/ar.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
from database import get_db

router = APIRouter(prefix="/ar", tags=["accounts-receivable"])

from services.ar_service import (
    get_ar_kpi_summary,
    get_ar_aging_summary,
    get_ar_priority_queue,
    get_ar_risk_distribution,
    get_ar_aging_trends
)
from models.ar import (
    ARKPISummary,
    ARAgingSummary,
    ARPriorityQueueItem,
    ARRiskDistribution,
    ARAgingTrend
)

@router.get("/kpi-summary", response_model=ARKPISummary)
async def get_ar_kpi_summary_endpoint(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: Session = Depends(get_db)
):
    """Get AR KPI summary for dashboard"""
    return get_ar_kpi_summary(db, start_date, end_date)

@router.get("/aging-summary", response_model=List[ARAgingSummary])
async def get_ar_aging_summary_endpoint(
    snapshot_date: Optional[date] = Query(None),
    db: Session = Depends(get_db)
):
    """Get AR aging summary by bucket"""
    return get_ar_aging_summary(db, snapshot_date)

@router.get("/priority-queue", response_model=List[ARPriorityQueueItem])
async def get_ar_priority_queue_endpoint(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    min_priority_score: Optional[int] = Query(None, ge=0, le=100),
    risk_category: Optional[str] = Query(None),
    min_balance: Optional[float] = Query(None, ge=0),
    provider_id: Optional[int] = Query(None),
    db: Session = Depends(get_db)
):
    """Get AR priority queue sorted by collection priority"""
    return get_ar_priority_queue(
        db, skip, limit, min_priority_score, 
        risk_category, min_balance, provider_id
    )

@router.get("/risk-distribution", response_model=List[ARRiskDistribution])
async def get_ar_risk_distribution_endpoint(
    snapshot_date: Optional[date] = Query(None),
    db: Session = Depends(get_db)
):
    """Get risk category distribution"""
    return get_ar_risk_distribution(db, snapshot_date)

@router.get("/aging-trends", response_model=List[ARAgingTrend])
async def get_ar_aging_trends_endpoint(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: Session = Depends(get_db)
):
    """Get AR aging trends over time"""
    return get_ar_aging_trends(db, start_date, end_date)
```

**Files to Create**:
- `api/routers/ar.py` (new file)

**Files to Modify**:
- `api/main.py` - Add router import and registration:
```python
from routers import ar
app.include_router(ar.router)
```

**Estimated Time**: 30 minutes

---

### Phase 2: Frontend Development (2-3 hours)

#### Step 2.1: Add TypeScript Types (`frontend/src/types/api.ts`)

**Purpose**: TypeScript interfaces for API responses

**Types to Add**:

```typescript
// Add to frontend/src/types/api.ts

export interface ARKPISummary {
    total_ar_outstanding: number;
    current_amount: number;
    current_percentage: number;
    over_90_amount: number;
    over_90_percentage: number;
    dso_days: number;
    collection_rate: number;
    high_risk_count: number;
    high_risk_amount: number;
}

export interface ARAgingSummary {
    aging_bucket: string;
    amount: number;
    percentage: number;
    patient_count: number;
}

export interface ARPriorityQueueItem {
    patient_id: number;
    provider_id: number;
    patient_name: string;
    provider_name: string;
    total_balance: number;
    balance_0_30_days: number;
    balance_31_60_days: number;
    balance_61_90_days: number;
    balance_over_90_days: number;
    aging_risk_category: string;
    collection_priority_score: number;
    days_since_last_payment: number | null;
    payment_recency: string;
    collection_rate: number | null;
}

export interface ARRiskDistribution {
    risk_category: string;
    patient_count: number;
    total_amount: number;
    percentage: number;
}

export interface ARAgingTrend {
    date: string;
    current_amount: number;
    over_30_amount: number;
    over_60_amount: number;
    over_90_amount: number;
    total_amount: number;
}
```

**Files to Modify**:
- `frontend/src/types/api.ts`

**Estimated Time**: 15 minutes

---

#### Step 2.2: Add API Service Methods (`frontend/src/services/api.ts`)

**Purpose**: API client functions for AR endpoints

**Methods to Add**:

```typescript
// Add to frontend/src/services/api.ts

// AR API calls
export const arApi = {
    getKPISummary: async (params: DateRange = {}): Promise<ApiResponse<ARKPISummary>> => {
        return apiCall(() => api.get('/ar/kpi-summary', { params }));
    },

    getAgingSummary: async (snapshotDate?: string): Promise<ApiResponse<ARAgingSummary[]>> => {
        const params = snapshotDate ? { snapshot_date: snapshotDate } : {};
        return apiCall(() => api.get('/ar/aging-summary', { params }));
    },

    getPriorityQueue: async (
        skip: number = 0,
        limit: number = 100,
        filters: {
            min_priority_score?: number;
            risk_category?: string;
            min_balance?: number;
            provider_id?: number;
        } = {}
    ): Promise<ApiResponse<ARPriorityQueueItem[]>> => {
        return apiCall(() => api.get('/ar/priority-queue', {
            params: { skip, limit, ...filters }
        }));
    },

    getRiskDistribution: async (snapshotDate?: string): Promise<ApiResponse<ARRiskDistribution[]>> => {
        const params = snapshotDate ? { snapshot_date: snapshotDate } : {};
        return apiCall(() => api.get('/ar/risk-distribution', { params }));
    },

    getAgingTrends: async (params: DateRange = {}): Promise<ApiResponse<ARAgingTrend[]>> => {
        return apiCall(() => api.get('/ar/aging-trends', { params }));
    },
};
```

**Files to Modify**:
- `frontend/src/services/api.ts`
  - Update existing `arApi` object (currently has `getSummary` only)
  - Add import for new types

**Estimated Time**: 20 minutes

---

#### Step 2.3: Create React Component (`frontend/src/pages/AR.tsx`)

**Purpose**: Main dashboard UI component

**Component Structure**:

```typescript
// frontend/src/pages/AR.tsx
import React, { useState, useEffect } from 'react';
import {
    Box, Typography, Card, CardContent, Grid,
    Table, TableBody, TableCell, TableContainer,
    TableHead, TableRow, Paper, Chip, CircularProgress,
    Alert, FormControl, InputLabel, Select, MenuItem,
    TextField
} from '@mui/material';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { arApi } from '../services/api';
import { ARKPISummary, ARPriorityQueueItem, ARRiskDistribution } from '../types/api';

const AR: React.FC = () => {
    // State management
    const [kpiSummary, setKpiSummary] = useState<ARKPISummary | null>(null);
    const [priorityQueue, setPriorityQueue] = useState<ARPriorityQueueItem[]>([]);
    const [riskDistribution, setRiskDistribution] = useState<ARRiskDistribution[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    
    // Filter state
    const [minPriorityScore, setMinPriorityScore] = useState<number>(50);
    const [riskCategory, setRiskCategory] = useState<string>('');
    const [minBalance, setMinBalance] = useState<number>(0);
    const [selectedProvider, setSelectedProvider] = useState<number | ''>('');

    // Load data functions
    const loadARData = async () => {
        // Implementation
    };

    // Component rendering
    return (
        <Box>
            {/* KPI Cards */}
            {/* AR Aging Chart */}
            {/* Priority Queue Table */}
            {/* Risk Distribution Pie Chart */}
            {/* Filters */}
        </Box>
    );
};

export default AR;
```

**UI Components to Include**:

1. **KPI Summary Cards** (Grid layout):
   - Total AR Outstanding
   - Current (0-30) Amount & %
   - Over 90 Days Amount & %
   - DSO (Days Sales Outstanding)
   - Collection Rate

2. **AR Aging Chart** (Stacked Bar Chart):
   - X-axis: Time buckets (0-30, 31-60, 61-90, 90+)
   - Y-axis: Amount ($)
   - Stacked bars showing aging distribution

3. **Collection Priority Queue Table**:
   - Sortable columns: Patient Name, Provider, Balance, Aging Buckets, Risk Category, Priority Score
   - Color-coded risk chips
   - Pagination

4. **Risk Distribution Pie Chart**:
   - Slices: High Risk, Medium Risk, Moderate Risk, Low Risk
   - Tooltip with patient count and amount

5. **Filters Panel**:
   - Date range selector
   - Provider dropdown
   - Risk category dropdown
   - Min balance input
   - Min priority score slider

**Styling Notes**:
- Use Material-UI theme consistent with other dashboards
- Color coding:
  - High Risk: Red (#d32f2f)
  - Medium Risk: Orange (#f57c00)
  - Moderate Risk: Yellow (#fbc02d)
  - Low Risk: Green (#388e3c)

**Files to Create**:
- `frontend/src/pages/AR.tsx` (new file)

**Estimated Time**: 1.5 hours

---

#### Step 2.4: Add Route (`frontend/src/App.tsx`)

**Purpose**: Register AR dashboard route

**Changes**:

```typescript
// frontend/src/App.tsx
import AR from './pages/AR';

// In Routes:
<Route path="/ar-aging" element={<AR />} />
```

**Files to Modify**:
- `frontend/src/App.tsx`

**Estimated Time**: 5 minutes

---

#### Step 2.5: Add Navigation Link (`frontend/src/components/layout/Layout.tsx`)

**Purpose**: Add AR Aging to navigation menu

**Changes**:
- Add menu item: "AR Aging" → `/ar-aging`

**Files to Modify**:
- `frontend/src/components/layout/Layout.tsx`

**Estimated Time**: 5 minutes

---

### Phase 3: Integration & Testing (30 minutes)

#### Step 3.1: Update dbt Exposure (`dbt_dental_models/models/marts/exposures.yml`)

**Purpose**: Mark AR Aging Dashboard as implemented

**Changes**:
```yaml
- name: accounts_receivable_aging_dashboard
  # ... existing config ...
  maturity: high  # Change from 'low' to 'high'
  url: https://analytics-demo.your-domain.com/ar-aging  # Update to production URL
  description: >
    **[IMPLEMENTED] Accounts Receivable Aging Dashboard**
    
    # ... update description to reflect implementation ...
```

**Files to Modify**:
- `dbt_dental_models/models/marts/exposures.yml`

**Estimated Time**: 10 minutes

---

#### Step 3.2: Test Endpoints

**Testing Checklist**:
- [ ] Test `/ar/kpi-summary` endpoint returns data
- [ ] Test `/ar/aging-summary` endpoint returns buckets
- [ ] Test `/ar/priority-queue` with filters
- [ ] Test `/ar/risk-distribution` returns categories
- [ ] Test `/ar/aging-trends` returns time series
- [ ] Verify error handling for invalid parameters
- [ ] Test pagination on priority queue

**Estimated Time**: 20 minutes

---

## File Structure Summary

### New Files to Create:
```
api/
├── models/
│   └── ar.py                          [NEW]
├── routers/
│   └── ar.py                          [NEW]
└── services/
    └── ar_service.py                  [NEW]

frontend/src/
├── pages/
│   └── AR.tsx                         [NEW]
```

### Files to Modify:
```
api/
└── main.py                            [MODIFY] - Add ar router

frontend/src/
├── App.tsx                            [MODIFY] - Add route
├── components/layout/Layout.tsx        [MODIFY] - Add nav link
├── services/api.ts                    [MODIFY] - Add arApi methods
└── types/api.ts                       [MODIFY] - Add AR types

dbt_dental_models/models/marts/
└── exposures.yml                      [MODIFY] - Update AR exposure
```

---

## Development Order

### Recommended Sequence:

1. **Backend First** (2-3 hours):
   - Create models (`api/models/ar.py`)
   - Create service (`api/services/ar_service.py`)
   - Create router (`api/routers/ar.py`)
   - Test endpoints with Postman/curl

2. **Frontend Second** (2-3 hours):
   - Add TypeScript types
   - Add API service methods
   - Create React component
   - Add route and navigation

3. **Integration** (30 minutes):
   - Update dbt exposure
   - Final testing
   - UI polish

---

## Key Considerations

### Database Queries:
- Use `DISTINCT ON` for latest snapshot per patient
- JOIN `dim_patient` and `dim_provider` for names
- Filter by `snapshot_date` for time-based analysis
- Index on `collection_priority_score` for sorting

### Performance:
- Paginate priority queue (default 100 items)
- Cache KPI summary (changes daily)
- Use aggregation queries instead of fetching all rows

### Business Logic:
- DSO calculation: `(total_ar / avg_daily_production) * 30`
- Risk categories: Use `aging_risk_category` from mart
- Priority scores: Use `collection_priority_score` (0-100)

### Error Handling:
- Handle null balances (credit balances)
- Handle missing payment history
- Validate date ranges
- Validate filter parameters

---

## Success Criteria

✅ All 5 API endpoints return data  
✅ Frontend displays KPI cards with correct values  
✅ AR Aging chart renders with stacked bars  
✅ Priority queue table is sortable and filterable  
✅ Risk distribution pie chart shows categories  
✅ Filters work correctly  
✅ Navigation link added  
✅ dbt exposure updated  
✅ No console errors  
✅ Mobile responsive  

---

## Next Steps After Completion

1. Test with demo data from synthetic generator
2. Deploy to AWS
3. Update exposure URL to production domain
4. Document API endpoints
5. Add unit tests for service layer
6. Consider adding export to CSV functionality

---

**Estimated Total Time**: 4-6 hours  
**Complexity**: Medium (mart exists, standard CRUD patterns)  
**Dependencies**: None (all required marts exist)

