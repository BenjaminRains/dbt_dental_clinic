# Frontend Public Access Troubleshooting Guide

This guide helps diagnose why `https://dbtdentalclinic.com` is not publicly accessible.

## Quick Diagnostic Commands

Run these commands to check your deployment status:

### 1. Check CloudFront Distribution Status

```powershell
# Get distribution details
aws cloudfront get-distribution --id E2VD20WF0IB7QE --query 'Distribution.[Status,DistributionConfig.Enabled,DistributionConfig.State,DomainName]' --output table
```

**What to look for:**
- **Status**: Should be `Deployed` (not `InProgress`)
- **Enabled**: Should be `true`
- **State**: Should be `Enabled`
- **DomainName**: This is your CloudFront domain (e.g., `d1234567890.cloudfront.net`)

### 2. Check CloudFront Custom Domain (CNAME)

```powershell
aws cloudfront get-distribution --id E2VD20WF0IB7QE --query 'Distribution.DistributionConfig.Aliases.Items' --output table
```

**What to look for:**
- Should include `dbtdentalclinic.com` and optionally `www.dbtdentalclinic.com`
- If empty, custom domain is not configured

### 3. Check SSL Certificate in CloudFront

```powershell
aws cloudfront get-distribution --id E2VD20WF0IB7QE --query 'Distribution.DistributionConfig.ViewerCertificate.[ACMCertificateArn,MinimumProtocolVersion,SSLSupportMethod]' --output table
```

**What to look for:**
- **ACMCertificateArn**: Should have a certificate ARN (not empty)
- **SSLSupportMethod**: Should be `sni-only` or `vip`
- **MinimumProtocolVersion**: Should be `TLSv1.2_2021` or similar

### 4. Check Route 53 DNS Records

```powershell
# First, find your hosted zone ID
aws route53 list-hosted-zones --query "HostedZones[?contains(Name, 'dbtdentalclinic.com')].[Id,Name]" --output table

# Then check records (replace ZONE_ID with your actual zone ID)
aws route53 list-resource-record-sets --hosted-zone-id ZONE_ID --query "ResourceRecordSets[?Name=='dbtdentalclinic.com.' || Name=='www.dbtdentalclinic.com.']" --output table
```

**What to look for:**
- Should have an A record (Alias) pointing to CloudFront
- Alias Target should match your CloudFront distribution domain

### 5. Test DNS Resolution

```powershell
Resolve-DnsName -Name dbtdentalclinic.com -Type A
```

**What to look for:**
- Should resolve to CloudFront IP addresses
- If it doesn't resolve, DNS hasn't propagated or record is missing

### 6. Test CloudFront Directly

```powershell
# Get CloudFront domain first
$cfDomain = (aws cloudfront get-distribution --id E2VD20WF0IB7QE --query 'Distribution.DomainName' --output text)
Write-Host "Testing: https://$cfDomain"
Invoke-WebRequest -Uri "https://$cfDomain" -Method GET
```

**What to look for:**
- Should return HTTP 200
- If this works but custom domain doesn't, it's a DNS/SSL issue

### 7. Test Custom Domain

```powershell
Invoke-WebRequest -Uri "https://dbtdentalclinic.com" -Method GET
```

**What to look for:**
- Should return HTTP 200
- If it fails, note the error message

### 8. Check S3 Bucket Contents

```powershell
aws s3 ls s3://dbtdentalclinic-frontend-3345/ --recursive
```

**What to look for:**
- Should have `index.html` and other frontend files
- If empty, files haven't been uploaded

## Common Issues and Solutions

### Issue 1: CloudFront Distribution Not Deployed

**Symptoms:**
- Status shows `InProgress`
- Distribution is not accessible

**Solution:**
1. Wait for deployment to complete (can take 15-30 minutes)
2. Check CloudFront console for any errors
3. If stuck, you may need to update the distribution to trigger redeployment

### Issue 2: CloudFront Distribution Disabled

**Symptoms:**
- `Enabled` is `false`
- `State` is `Disabled`

**Solution:**
```powershell
# Get current config
aws cloudfront get-distribution-config --id E2VD20WF0IB7QE > dist-config.json

# Edit dist-config.json to set Enabled: true
# Then update (you'll need the ETag from the get command)
aws cloudfront update-distribution --id E2VD20WF0IB7QE --if-match ETAG --distribution-config file://dist-config.json
```

**Note:** It's easier to do this in the AWS Console:
1. Go to CloudFront → Distributions → Your Distribution
2. Click "Edit"
3. Set "Enabled" to "Yes"
4. Save changes

### Issue 3: Custom Domain Not Configured

**Symptoms:**
- `Aliases.Items` is empty
- CloudFront works via `*.cloudfront.net` but not custom domain

**Solution:**
1. Go to CloudFront console
2. Edit your distribution
3. Under "Alternate domain names (CNAMEs)", add:
   - `dbtdentalclinic.com`
   - `www.dbtdentalclinic.com` (optional)
4. Select your SSL certificate
5. Save and wait for deployment

### Issue 4: SSL Certificate Not Configured

**Symptoms:**
- `ACMCertificateArn` is empty
- SSL errors when accessing custom domain

**Solution:**
1. Ensure you have a certificate in ACM (us-east-1 region)
2. Certificate must be for `dbtdentalclinic.com` (and `*.dbtdentalclinic.com` if using wildcard)
3. Certificate status must be `ISSUED`
4. In CloudFront, edit distribution and select the certificate

### Issue 5: Route 53 DNS Not Configured

**Symptoms:**
- DNS doesn't resolve
- `Resolve-DnsName` fails

**Solution:**
1. Go to Route 53 console
2. Find your hosted zone for `dbtdentalclinic.com`
3. Create/update A record:
   - Type: A (Alias)
   - Name: `dbtdentalclinic.com` (or leave blank for root)
   - Alias: Yes
   - Alias Target: Select your CloudFront distribution
4. Save

### Issue 6: S3 Bucket Empty

**Symptoms:**
- `aws s3 ls` shows no files
- CloudFront returns 403 or 404

**Solution:**
```powershell
# Rebuild and deploy
cd frontend
npm run build
cd ..
frontend-deploy
```

### Issue 7: DNS Propagation Delay

**Symptoms:**
- Everything looks correct but site still not accessible
- DNS resolves differently in different locations

**Solution:**
- Wait up to 48 hours for full DNS propagation
- Use `dig` or `nslookup` from different locations to test
- Clear your local DNS cache: `ipconfig /flushdns` (Windows)

## Step-by-Step Fix Checklist

If your site is not accessible, follow these steps:

1. ✅ **Verify CloudFront is Deployed and Enabled**
   - Status: `Deployed`
   - Enabled: `true`
   - State: `Enabled`

2. ✅ **Verify Custom Domain is Configured**
   - CNAME includes `dbtdentalclinic.com`
   - SSL certificate is selected

3. ✅ **Verify SSL Certificate**
   - Certificate exists in ACM (us-east-1)
   - Certificate status: `ISSUED`
   - Certificate covers `dbtdentalclinic.com`

4. ✅ **Verify Route 53 DNS**
   - A record (Alias) exists
   - Points to CloudFront distribution
   - DNS resolves correctly

5. ✅ **Verify S3 Bucket Has Files**
   - `index.html` exists
   - All frontend assets uploaded

6. ✅ **Test Direct CloudFront Access**
   - `https://[cloudfront-domain]` works
   - If this works, issue is DNS/SSL related

7. ✅ **Wait for Propagation**
   - CloudFront changes: 15-30 minutes
   - DNS changes: Up to 48 hours

## Quick Fix: Re-enable CloudFront Distribution

If your distribution was accidentally disabled:

```powershell
# This is complex via CLI - use AWS Console instead:
# 1. Go to: https://console.aws.amazon.com/cloudfront/v3/home#/distributions/E2VD20WF0IB7QE
# 2. Click "Edit"
# 3. Under "General", set "Enabled" to "Yes"
# 4. Save changes
# 5. Wait 15-30 minutes for deployment
```

## Testing After Fixes

After making changes, test with:

```powershell
# Test CloudFront directly
$cfDomain = (aws cloudfront get-distribution --id E2VD20WF0IB7QE --query 'Distribution.DomainName' --output text)
Invoke-WebRequest -Uri "https://$cfDomain" -Method GET

# Test custom domain
Invoke-WebRequest -Uri "https://dbtdentalclinic.com" -Method GET

# Check DNS
Resolve-DnsName -Name dbtdentalclinic.com
```

## Need Help?

If none of these solutions work:
1. Check CloudFront error logs (if configured)
2. Check browser console for specific errors
3. Verify AWS credentials have necessary permissions
4. Check if there are any WAF rules blocking traffic
5. Verify security groups/firewall rules (though CloudFront doesn't use these)

