# Dental Analytics Frontend

A modern React dashboard for dental practice analytics, built with TypeScript, Material-UI, and Recharts.

## 🚀 Getting Started

### Prerequisites
- Node.js 18+ 
- npm or yarn

### Installation

1. Install dependencies:
```bash
npm install
```

2. Create environment file:
```bash
cp .env.example .env
```

3. Update `.env` with your API URL:
```
VITE_API_URL=http://localhost:8000
```

4. Start development server:
```bash
npm run dev
```

The application will be available at `http://localhost:3000`

## 🏗️ Project Structure

```
src/
├── components/          # Reusable UI components
│   ├── charts/         # Custom chart components
│   ├── layout/         # Layout components
│   └── common/         # Shared components
├── pages/              # Route components
├── services/           # API service layer
├── store/              # Zustand state management
├── types/              # TypeScript type definitions
└── utils/              # Utility functions
```

## 🛠️ Tech Stack

- **React 18** - UI framework
- **TypeScript** - Type safety
- **Material-UI** - Component library
- **Recharts** - Data visualization
- **Zustand** - State management
- **Axios** - HTTP client
- **React Router** - Navigation
- **Vite** - Build tool

## 📊 Features

- **Executive Dashboard** - KPI overview and key metrics
- **Revenue Analytics** - Financial performance tracking
- **Provider Performance** - Individual provider metrics
- **Patient Analytics** - Retention and acquisition insights
- **Appointment Analytics** - Scheduling and utilization metrics

## 🔧 Development

### Available Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run preview` - Preview production build

### API Integration

The frontend communicates with the FastAPI backend through the service layer in `src/services/api.ts`. All API calls are type-safe and include error handling.

### State Management

Uses Zustand for lightweight state management. Stores are organized by domain (revenue, providers, etc.).

## 🎨 Styling

- Material-UI theme system
- Responsive design with mobile-first approach
- Custom color palette for dental practice branding
- Consistent spacing and typography

## 📱 Responsive Design

- Mobile-first approach
- Collapsible sidebar navigation
- Responsive charts and tables
- Touch-friendly interactions

## 🚀 Deployment

The application can be deployed to any static hosting service:

- Vercel
- Netlify
- AWS S3 + CloudFront
- GitHub Pages

Build the application:
```bash
npm run build
```

The `dist` folder contains the production build.
