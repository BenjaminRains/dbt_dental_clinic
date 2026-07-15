# Frontend Network Error Troubleshooting

## Problem: "Unable to connect to server" or Network Errors

If you're seeing network errors when loading the demo dashboards, check these issues:

## 1. Frontend Not Rebuilt with API Key

**Issue:** The frontend was deployed before the API key was added to `deployment_credentials.json`.

**Solution:** Rebuild and redeploy the frontend:
```powershell
frontend-deploy
```

The deployment script will now automatically:
- Read API key from `deployment_credentials.json`
- Build frontend with `VITE_API_URL=https://api.dbtdentalclinic.com`
- Build frontend with `VITE_API_KEY=<your-api-key>`

## 2. CORS Configuration

**Check CORS is configured correctly:**

The API should allow requests from:
- `https://dbtdentalclinic.com`
- `https://www.dbtdentalclinic.com`

**Verify CORS:**
```powershell
# Test CORS preflight
Invoke-WebRequest -Uri "https://api.dbtdentalclinic.com/reports/dashboard/kpis" `
    -Method OPTIONS `
    -Headers @{
        "Origin" = "https://dbtdentalclinic.com"
        "Access-Control-Request-Method" = "GET"
        "Access-Control-Request-Headers" = "X-API-Key"
    }
```

**Check CORS headers in response:**
- `Access-Control-Allow-Origin: https://dbtdentalclinic.com`
- `Access-Control-Allow-Methods: GET, POST, OPTIONS`
- `Access-Control-Allow-Headers: Content-Type, X-API-Key, Accept`
- `Access-Control-Allow-Credentials: true`

## 3. API Key in Frontend Build

**Check if API key is in the build:**

The API key is embedded in the JavaScript bundle during build. To verify:
1. Visit `https://dbtdentalclinic.com/demo`
2. Open browser DevTools (F12)
3. Go to Network tab
4. Look for API requests
5. Check Request Headers - should include `X-API-Key: 6mmQoSTPD_PLfs4QTXf2M6WUOMNinxu_`

**If API key is missing:**
- The build didn't include `VITE_API_KEY`
- Rebuild with: `frontend-deploy`

## 4. API Endpoint Errors

**Check API is responding:**

```powershell
# Test API root
Invoke-WebRequest -Uri "https://api.dbtdentalclinic.com/" -Method GET

# Test with API key
Invoke-WebRequest -Uri "https://api.dbtdentalclinic.com/reports/dashboard/kpis" `
    -Method GET `
    -Headers @{"X-API-Key" = "6mmQoSTPD_PLfs4QTXf2M6WUOMNinxu_"}
```

**Common API errors:**
- **500 Internal Server Error**: Database connection issue or API error
- **401 Unauthorized**: Missing or invalid API key
- **403 Forbidden**: CORS issue or rate limiting
- **404 Not Found**: Endpoint doesn't exist

## 5. Browser Console Errors

**Check browser console for specific errors:**

1. Visit `https://dbtdentalclinic.com/demo`
2. Open DevTools (F12) → Console tab
3. Look for:
   - CORS errors: `Access to fetch at '...' from origin '...' has been blocked by CORS policy`
   - Network errors: `Failed to fetch` or `Network request failed`
   - API errors: `401 Unauthorized` or `500 Internal Server Error`

## 6. CloudFront Cache

**Issue:** Old frontend build cached in CloudFront.

**Solution:** The deployment script automatically invalidates CloudFront, but you can manually invalidate:
```powershell
aws cloudfront create-invalidation --distribution-id E2VD20WF0IB7QE --paths "/*"
```

Wait 5-10 minutes for invalidation to complete.

## 7. Quick Fix Checklist

1. ✅ **API key in deployment_credentials.json**: `backend_api.api_key`
2. ✅ **Rebuild frontend**: Run `frontend-deploy`
3. ✅ **Wait for CloudFront invalidation**: 5-10 minutes
4. ✅ **Clear browser cache**: Ctrl+Shift+Delete or hard refresh (Ctrl+F5)
5. ✅ **Check browser console**: Look for specific error messages
6. ✅ **Test API directly**: Verify API is accessible with API key

## 8. Testing After Fix

**Test the full flow:**
1. Visit `https://dbtdentalclinic.com/demo`
2. Open DevTools → Network tab
3. Check API requests:
   - Should go to `https://api.dbtdentalclinic.com`
   - Should include `X-API-Key` header
   - Should return 200 status (not 401, 403, or 500)
4. Check CORS headers in response
5. Verify data loads in dashboards

## Common Issues and Solutions

### Issue: "Unable to connect to server"
**Cause:** Frontend doesn't have API key or wrong API URL
**Fix:** Rebuild with `frontend-deploy`

### Issue: CORS error in browser console
**Cause:** API CORS not configured for frontend domain
**Fix:** Check `API_CORS_ORIGINS` on EC2 includes `https://dbtdentalclinic.com`

### Issue: 401 Unauthorized
**Cause:** API key missing or incorrect in frontend build
**Fix:** Rebuild frontend with correct API key

### Issue: 500 Internal Server Error
**Cause:** API or database issue
**Fix:** Check EC2 logs: `aws ssm start-session --target i-0b7013339cf648e0f` then `sudo journalctl -u dental-clinic-api -n 50`

### Issue: Network timeout
**Cause:** API not responding or security group blocking
**Fix:** Verify API is accessible and security groups allow traffic

