# Frontend-API Integration Guide

## Overview
The frontend is now connected to the Patient API endpoints. Here's how to test and use the integration.

## Setup

### 1. Start the API Server
```bash
# From project root
api-init
api-run
```
The API will be available at `http://localhost:8000`

### 2. Start the Frontend
```bash
# From frontend directory
npm run dev
```
The frontend will be available at `http://localhost:3000`

## API Integration Details

### Proxy Configuration
The frontend uses Vite's proxy feature to connect to the API:
- **Development**: Frontend calls `/api/*` which proxies to `http://localhost:8000/*`
- **Production**: Can be configured with `VITE_API_URL` environment variable

### Patient API Endpoints
- `GET /patients/` - List patients with pagination
- `GET /patients/{id}` - Get specific patient by ID

### Frontend Features
- **Patient List**: Displays patients in a table with pagination
- **Patient Details**: Shows demographics, status, financial info, insurance
- **Error Handling**: Displays errors with retry functionality
- **Loading States**: Shows loading indicators during API calls

## Testing the Integration

### 1. Test API Directly
```bash
# Test API endpoints directly
curl http://localhost:8000/
curl http://localhost:8000/patients/
curl http://localhost:8000/docs
```

### 2. Test Frontend
1. Navigate to `http://localhost:3000`
2. Click on "Patients" in the navigation
3. You should see a table with patient data
4. Check browser dev tools for API calls

### 3. Test Error Handling
- Stop the API server to test error states
- Check that error messages appear with retry buttons

## Troubleshooting

### Common Issues

1. **CORS Errors**: The API has CORS enabled for all origins in development
2. **Proxy Issues**: Ensure Vite dev server is running on port 3000
3. **API Connection**: Verify API server is running on port 8000
4. **Data Format**: Check that API returns data in expected format

### Debug Steps
1. Check browser Network tab for API calls
2. Check API server logs for incoming requests
3. Verify environment variables are set correctly
4. Check that both servers are running

## Next Steps

1. **Add Patient Detail View**: Create a detailed patient view page
2. **Add Search/Filter**: Implement patient search and filtering
3. **Add Patient Creation**: Create forms to add new patients
4. **Add Charts**: Display patient analytics with charts
5. **Add Authentication**: Implement user authentication
