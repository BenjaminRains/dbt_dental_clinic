# 🚀 Frontend Development Workflow

This guide provides a practical workflow for developing the dental analytics frontend, based on your roadmap and current setup.

## 🏃‍♂️ Quick Start

### 1. Development Environment Setup
```bash
# Navigate to frontend directory
cd frontend

# Install dependencies (if not already done)
npm install

# Start development server
npm run dev
```

### 2. Backend API Setup
Make sure your FastAPI backend is running:
```bash
# From project root
cd api
uvicorn main:app --reload --port 8000
```

### 3. Environment Configuration
Create `.env.local` in the frontend directory:
```env
VITE_API_URL=http://localhost:8000
```

## 📁 Current Project Structure

```
frontend/src/
├── components/
│   └── layout/
│       └── Layout.tsx          # ✅ Complete - Responsive sidebar navigation
├── pages/
│   ├── Dashboard.tsx           # ✅ Basic structure - needs charts
│   ├── Revenue.tsx             # 🔄 Needs implementation
│   ├── Providers.tsx           # 🔄 Needs implementation
│   ├── Patients.tsx            # 🔄 Needs implementation
│   └── Appointments.tsx        # 🔄 Needs implementation
├── services/
│   └── api.ts                  # ✅ Complete - Type-safe API layer
├── types/
│   └── api.ts                  # ✅ Complete - TypeScript interfaces
└── App.tsx                     # ✅ Complete - Routing setup
```

## 🎯 Development Workflow

### Phase 1: Core Dashboard Enhancement (Current Focus)

#### Step 1: Add Charts to Dashboard
1. **Create chart components** in `src/components/charts/`
2. **Add Recharts visualizations** to Dashboard.tsx
3. **Implement KPI cards** with trend indicators

#### Step 2: Build Revenue Page
1. **Create revenue trend chart** with date filtering
2. **Add AR aging visualization**
3. **Implement export functionality**

#### Step 3: Provider Performance Page
1. **Build provider comparison charts**
2. **Add performance metrics table**
3. **Create drill-down capabilities**

### Phase 2: State Management & Advanced Features

#### Step 1: Implement Zustand Store
1. **Create store structure** for different data domains
2. **Add caching and loading states**
3. **Implement optimistic updates**

#### Step 2: Add Interactive Features
1. **Date range filtering**
2. **Provider selection**
3. **Real-time data updates**

## 🛠️ Development Commands

### Daily Development
```bash
# Start development server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview

# Type checking
npx tsc --noEmit
```

### Code Quality
```bash
# Lint code (if ESLint is configured)
npm run lint

# Format code (if Prettier is configured)
npm run format
```

## 📊 Component Development Guidelines

### 1. Chart Components
- **Location**: `src/components/charts/`
- **Naming**: `{ChartType}Chart.tsx` (e.g., `RevenueTrendChart.tsx`)
- **Props**: Use TypeScript interfaces for all props
- **Responsive**: Ensure mobile-friendly design

### 2. Page Components
- **Location**: `src/pages/`
- **Structure**: Header, filters, charts, tables
- **Loading**: Always show loading states
- **Error**: Handle API errors gracefully

### 3. Layout Components
- **Location**: `src/components/layout/`
- **Reusable**: Design for multiple use cases
- **Accessible**: Include ARIA labels and keyboard navigation

## 🔄 API Integration Pattern

### 1. Data Fetching
```typescript
// Use the existing api.ts service
import { dashboardApi } from '../services/api';

const [data, setData] = useState<ApiResponse<DashboardKPIs>>({ loading: true });

useEffect(() => {
    const fetchData = async () => {
        const result = await dashboardApi.getKPIs();
        setData(result);
    };
    fetchData();
}, []);
```

### 2. Error Handling
```typescript
if (data.loading) {
    return <CircularProgress />;
}

if (data.error) {
    return <Alert severity="error">{data.error}</Alert>;
}

// Use data.data safely
```

## 🎨 Styling Guidelines

### 1. Material-UI Theme
- **Custom theme** already configured in `main.ts`
- **Consistent spacing** using theme.spacing()
- **Color palette** follows dental/medical theme

### 2. Responsive Design
- **Mobile-first** approach
- **Breakpoints**: xs, sm, md, lg, xl
- **Grid system** for layouts

### 3. Component Styling
```typescript
// Use sx prop for styling
<Box sx={{ 
    display: 'flex', 
    flexDirection: { xs: 'column', md: 'row' },
    gap: 2 
}}>
```

## 📈 Chart Development with Recharts

### 1. Basic Chart Structure
```typescript
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const RevenueTrendChart: React.FC<{ data: RevenueTrend[] }> = ({ data }) => {
    return (
        <ResponsiveContainer width="100%" height={400}>
            <LineChart data={data}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Line type="monotone" dataKey="revenue_lost" stroke="#8884d8" />
            </LineChart>
        </ResponsiveContainer>
    );
};
```

### 2. Chart Types to Implement
- **Line Chart**: Revenue trends over time
- **Bar Chart**: Provider performance comparison
- **Pie Chart**: Revenue breakdown by category
- **Area Chart**: AR aging visualization
- **Scatter Plot**: Provider efficiency matrix

## 🚀 Next Steps

### Immediate (This Week)
1. **Add charts to Dashboard.tsx**
2. **Implement Revenue page with trend chart**
3. **Create basic KPI cards with animations**

### Short Term (Next 2 Weeks)
1. **Complete all page implementations**
2. **Add filtering and date range selection**
3. **Implement Zustand state management**

### Medium Term (Next Month)
1. **Add export functionality**
2. **Implement mobile optimizations**
3. **Add advanced chart interactions**

## 🐛 Troubleshooting

### Common Issues
1. **API Connection**: Check if backend is running on port 8000
2. **CORS Errors**: Ensure backend has proper CORS configuration
3. **Type Errors**: Run `npx tsc --noEmit` to check TypeScript
4. **Build Errors**: Clear node_modules and reinstall if needed

### Development Tips
1. **Use React DevTools** for component debugging
2. **Check Network tab** for API call issues
3. **Use Material-UI theme** for consistent styling
4. **Test on mobile** regularly during development

## 📚 Resources

- **Material-UI Docs**: https://mui.com/
- **Recharts Docs**: https://recharts.org/
- **React Router**: https://reactrouter.com/
- **Zustand**: https://github.com/pmndrs/zustand
- **TypeScript**: https://www.typescriptlang.org/

---

**Ready to start?** Begin with adding charts to the Dashboard page, then move to implementing the Revenue page with interactive visualizations!
