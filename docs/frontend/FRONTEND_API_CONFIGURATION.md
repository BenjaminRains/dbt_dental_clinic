# Frontend API Configuration for Demo/Clinic Deployment

## Problem

The frontend dashboard is not loading data because:
1. The frontend build doesn't include the demo/clinic API URL (`https://api.dbtdentalclinic.com` or `https://api-clinic.dbtdentalclinic.com`)
2. The frontend build doesn't include the API key for authentication
3. The build defaults to `http://localhost:8000` which doesn't work from the deployed site

## Solution

The frontend needs to be built with environment variables:
- **Demo**: `VITE_API_URL=https://api.dbtdentalclinic.com`, `VITE_API_KEY=<demo-api-key>`
- **Clinic**: `VITE_API_URL=https://api-clinic.dbtdentalclinic.com`, `VITE_API_KEY=<clinic-api-key>`

## Current Build Process

The `frontend-deploy` command in `environment_manager.ps1` runs:
```powershell
npm run build
```

This doesn't set environment variables, so the build uses defaults.

## Fix Options

### Option 1: Update Build Script to Use Environment Variables (Recommended)

Modify `scripts/environment_manager.ps1` to set environment variables before building:

```powershell
# Step 1: Build
Write-Host "`n1. Building frontend..." -ForegroundColor Yellow
$frontendPath = "$ProjectPath\frontend"
Push-Location $frontendPath
try {
    # Set demo API configuration
    $env:VITE_API_URL = "https://api.dbtdentalclinic.com"
    $env:VITE_API_KEY = "<DEMO_API_KEY>"  # Need to retrieve from EC2 or use demo key
    
    npm run build
    # ... rest of build process
}
```

### Option 2: Create Demo/Clinic .env Files

Create `frontend/.env.demo` for demo deployment:
```
VITE_API_URL=https://api.dbtdentalclinic.com
VITE_API_KEY=<demo-api-key>
```

Create `frontend/.env.clinic` for clinic deployment:
```
VITE_API_URL=https://api-clinic.dbtdentalclinic.com
VITE_API_KEY=<clinic-api-key>
```

Vite will automatically use `.env.demo` or `.env.clinic` during build (depending on build target).

### Option 3: Make Demo Endpoints Public (For Portfolio)

For a portfolio/demo site, you might want to make certain endpoints public (no API key required). This would allow the dashboards to work without exposing the API key in the frontend build.

## Getting the Demo/Clinic API Key

The demo API key is stored on the EC2 instance at `/opt/dbt_dental_clinic/api/.env` as `DEMO_API_KEY`.

For clinic deployment, the clinic API key will be stored as `CLINIC_API_KEY`.

To retrieve it:
```powershell
# Via Systems Manager Session Manager
aws ssm start-session --target <INSTANCE_ID>
# Then on EC2:
cat /opt/dbt_dental_clinic/api/.env | grep API_KEY
```

Or check `deployment_credentials.json` if it's stored there.

## Security Considerations

**⚠️ Important:** Including the API key in the frontend build means it will be visible in the browser's JavaScript bundle. This is acceptable for:
- Demo/portfolio sites
- Public-facing dashboards with read-only access
- Non-sensitive data

**Not recommended for:**
- Production systems with sensitive data
- Systems requiring write access
- Systems with strict security requirements

## Recommended Approach for Portfolio

1. **Use a separate demo API key** that has limited permissions
2. **Make demo endpoints public** (no API key required) for portfolio showcase
3. **Or** include the API key in the build (acceptable for read-only demo data)

## Implementation Steps

1. **Retrieve API key from EC2** (or use a demo key)
2. **Update frontend build process** to include `VITE_API_URL` and `VITE_API_KEY`
3. **Rebuild and redeploy** the frontend
4. **Test** that dashboards load data correctly

## Testing

After deploying, test:
```powershell
# Test API is accessible
Invoke-WebRequest -Uri "https://api.dbtdentalclinic.com/docs"

# Test frontend can call API (check browser console)
# Visit https://dbtdentalclinic.com/demo
# Open browser DevTools → Network tab
# Check if API calls succeed
```

