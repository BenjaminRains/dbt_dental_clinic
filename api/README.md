# Dental Clinic API - FastAPI Backend Service

**Production-Ready REST API** connecting analytics data to business users.

## 🚀 Live Deployment

- **Production URL**: [https://api.dbtdentalclinic.com](https://api.dbtdentalclinic.com)
- **Hosting**: AWS EC2 + Application Load Balancer (ALB)
- **SSL Certificate**: AWS Certificate Manager (ACM)
- **DNS**: Route 53
- **Status**: ✅ Live and accessible

## 📋 API Endpoints

### Patient Management
- `GET /patients/` - List all patients with pagination
- `GET /patients/{patient_id}` - Get patient details
- `GET /patients/search?query={name}` - Search patients by name

### Revenue Analytics
- `GET /reports/revenue/` - Revenue trends, KPIs, and financial insights
- `GET /reports/revenue/trends?start_date={date}&end_date={date}` - Revenue trends over time

### Provider Performance
- `GET /reports/providers/` - Provider metrics and performance analysis
- `GET /reports/providers/{provider_id}` - Individual provider performance

### Dashboard KPIs
- `GET /reports/dashboard/` - Executive-level key performance indicators

### AR Management
- `GET /reports/ar/` - Accounts receivable analysis and collection insights

### Appointment Management
- `GET /appointments/` - List appointments with filtering
- `POST /appointments/` - Create new appointment

## 🏗️ AWS Architecture Overview

This API is deployed on AWS using a **defense-in-depth security model** that balances security best practices with cost-effectiveness. The architecture leverages multiple AWS services working together to provide a secure, scalable, and maintainable solution.

### AWS Services Used

**Compute & Networking:**
- **EC2 (Elastic Compute Cloud)**: Hosts the FastAPI application on Amazon Linux 2023
- **Application Load Balancer (ALB)**: Distributes traffic, handles SSL/TLS termination, and provides health checks
- **VPC (Virtual Private Cloud)**: Isolated network environment with public and private subnets
- **Security Groups**: Stateful firewalls controlling network traffic at the instance level

**Database:**
- **RDS (Relational Database Service)**: Managed PostgreSQL database in a private subnet
- **DB Subnet Group**: Ensures database is deployed across multiple availability zones

**Security & Access:**
- **AWS Certificate Manager (ACM)**: Manages SSL/TLS certificates for HTTPS
- **AWS Secrets Manager**: Securely stores database credentials
- **Systems Manager Session Manager**: Provides secure shell access to instances without SSH keys or open port 22
- **IAM (Identity and Access Management)**: Manages roles and permissions for EC2 instance

**DNS & Routing:**
- **Route 53**: DNS service managing the `api.dbtdentalclinic.com` domain
- **Target Groups**: Routes ALB traffic to healthy EC2 instances

**Monitoring (Optional):**
- **CloudWatch Logs**: Centralized logging for application and system logs
- **CloudWatch Metrics**: Performance and health monitoring

### Network Architecture

```
Internet
   ↓
Route 53 (api.dbtdentalclinic.com)
   ├─ DNS resolution to ALB
   ↓
Application Load Balancer (Public Subnet)
   ├─ Listener 443 (HTTPS)
   │  ├─ SSL/TLS termination via ACM certificate
   │  ├─ Default action: Forward to target group (EC2)
   │  └─ Decrypts HTTPS → forwards HTTP to EC2
   │
   ├─ Listener 80 (HTTP)
   │  ├─ Default action: Redirect to HTTPS:443
   │  ├─ Status code: HTTP 301 (permanent redirect)
   │  └─ ⚠️ HTTP requests NEVER reach EC2 (redirected at edge)
   │
   ├─ Health checks to EC2 instances
   └─ Security Group: Allows HTTPS/HTTP from internet
   ↓
EC2 Instance (Public Subnet, but secured)
   ├─ Receives ONLY HTTP traffic (SSL terminated at ALB)
   ├─ Nginx reverse proxy (listens on port 80, receives traffic from ALB)
   ├─ FastAPI application (port 8000, internal only)
   ├─ Systemd service management
   └─ Security Group: Only allows port 80 from ALB security group
   ↓
RDS PostgreSQL (Private Subnet)
   ├─ No internet gateway route
   ├─ No public IP address
   └─ Security Group: Only allows port 5432 from EC2 security group
```

### Traffic Flow

**HTTPS Request Flow:**
```
Client (HTTPS) 
  → Route 53 (DNS)
  → ALB Listener 443 (HTTPS)
  → ALB terminates SSL/TLS
  → ALB forwards HTTP to EC2:80 (Nginx)
  → Nginx (listens on port 80, receives HTTP from ALB)
  → Nginx proxies to FastAPI:8000 (internal)
  → FastAPI application
```

**HTTP Request Flow (Redirected at Edge):**
```
Client (HTTP)
  → Route 53 (DNS)
  → ALB Listener 80 (HTTP)
  → ALB redirects to HTTPS:443 (HTTP 301)
  → Client receives redirect, makes new HTTPS request
  → ⚠️ HTTP request NEVER reaches EC2
```

**Key Points:**
- ✅ **HTTP→HTTPS redirect happens at ALB edge** (before reaching EC2)
- ✅ **EC2 only receives HTTP traffic** (SSL terminated at ALB)
- ✅ **ALB targets Nginx on port 80** (Nginx is the entry point on EC2)
- ✅ **Nginx proxies to FastAPI on port 8000** (internal communication)
- ✅ **Nginx does NOT handle redirects** (ALB handles all redirects)
- ✅ **Simpler Nginx configuration** (only needs to proxy HTTP to FastAPI)

**Complete Request Path:**
```
ALB (HTTPS → HTTP) → Nginx:80 (adds security headers, logs) → FastAPI:8000 → RDS:5432
```

### Why Each AWS Service?

**Application Load Balancer (ALB):**
- **SSL/TLS Termination**: Handles HTTPS encryption/decryption at the edge, reducing load on EC2
- **HTTP→HTTPS Redirect**: Listener on port 80 automatically redirects all HTTP traffic to HTTPS (HTTP 301) at the edge
  - **Listener 443 (HTTPS)**: Default action forwards decrypted HTTP traffic to target group
  - **Listener 80 (HTTP)**: Default action redirects to HTTPS:443 with HTTP 301 status code
  - **Result**: HTTP requests never reach EC2 instances (redirected before leaving ALB)
- **Health Checks**: Automatically routes traffic away from unhealthy instances
- **High Availability**: Distributes traffic across multiple availability zones
- **Request Routing**: Single entry point for all API traffic
- **Edge-Level Security**: All redirects and SSL termination happen at AWS edge, simplifying backend configuration

**Why Handle Redirects at ALB Edge?**
- ✅ **Performance**: Redirects handled by AWS edge infrastructure (faster than EC2)
- ✅ **Reduced Load**: HTTP requests never reach EC2 (saves compute resources)
- ✅ **Simpler Backend**: Nginx doesn't need redirect logic (only proxies HTTP to FastAPI)
- ✅ **Better Security**: All HTTP traffic redirected before reaching application servers
- ✅ **Cost Efficiency**: Less processing on EC2 instances
- ✅ **Best Practice**: Industry standard to handle redirects at load balancer level

**EC2 (Elastic Compute Cloud):**
- **Flexibility**: Full control over the application environment
- **Cost-Effective**: Pay only for compute resources used
- **Scalability**: Can easily scale up/down based on demand
- **Amazon Linux 2023**: Optimized for AWS services with long-term support

**RDS (Relational Database Service):**
- **Managed Service**: Automated backups, patching, and monitoring
- **High Availability**: Multi-AZ deployment options available
- **Security**: Encryption at rest and in transit
- **Private Subnet**: Complete network isolation from internet

**Route 53:**
- **DNS Management**: Reliable domain name resolution
- **Health Checks**: Can route traffic based on endpoint health
- **Alias Records**: Direct routing to ALB without IP address management

**AWS Certificate Manager (ACM):**
- **Free SSL Certificates**: No cost for public certificates
- **Automatic Renewal**: Certificates automatically renew before expiration
- **Easy Integration**: Seamless integration with ALB

**AWS Secrets Manager:**
- **Secure Storage**: Encrypted storage of database credentials
- **Rotation**: Automatic credential rotation capabilities
- **IAM Integration**: Fine-grained access control via IAM policies

**Systems Manager Session Manager:**
- **Secure Shell Access**: Provides shell access via SSM channel (not traditional SSH)
- **No SSH Keys**: Eliminates SSH key management overhead
- **IAM-Based Access**: Access controlled through IAM policies
- **Audit Trail**: All sessions are logged for security auditing
- **No Open Ports**: No need to expose SSH port (22) to internet
- **Note**: Functionally replaces SSH but uses AWS Systems Manager protocol instead

## 🔒 Security Architecture

### Defense-in-Depth Model

The architecture implements multiple layers of security, ensuring that even if one layer is compromised, others provide protection.

### Security Layers

#### 1. Network Isolation

**RDS Database (Private Subnet):**
- **Private Subnet**: Located in a subnet with no route to an internet gateway
- **No Public IP**: Database has no public IP address
- **Security Group Restriction**: Only allows PostgreSQL (port 5432) from EC2 security group
- **Result**: Database is completely isolated from the internet and cannot be reached directly

**EC2 Instance (Public Subnet with Restrictions):**
- **Public Subnet**: Allows direct internet access for updates and AWS API calls
- **Security Group Protection**: Nginx port (80) is blocked from internet, only accessible from ALB
- **Internal Communication**: FastAPI runs on port 8000 (only accessible from Nginx on localhost)
- **No SSH Exposure**: No SSH port (22) open to internet
- **Result**: EC2 can access internet for updates but API is protected

#### 2. Security Groups (Stateful Firewalls)

Security groups act as virtual firewalls at the instance level, controlling inbound and outbound traffic.

**ALB Security Group:**
- ✅ Allows HTTPS (443) from internet (0.0.0.0/0)
- ✅ Allows HTTP (80) from internet (redirects to HTTPS)
- ❌ Blocks all other inbound traffic
- **Purpose**: ALB is the only public-facing component

**API Security Group (EC2):**
- ✅ Allows port 80 **only from ALB security group** (not from internet)
- ❌ Blocks all direct internet access to Nginx port
- ❌ No SSH port (22) open to internet
- **Purpose**: Even though EC2 is in a public subnet, Nginx is not directly accessible from internet
- **Result**: Even if someone knows the EC2 IP address, they cannot reach port 80 (Nginx)
- **Note**: FastAPI runs on port 8000 internally (only accessible from Nginx on localhost)

**RDS Security Group:**
- ✅ Allows PostgreSQL (5432) **only from API security group**
- ❌ Blocks all internet access
- ❌ No other ports open
- **Purpose**: Database is only accessible from the EC2 instance
- **Result**: Database is completely isolated and only reachable from the application server

#### 3. Access Control

**EC2 Instance Access:**
- ❌ **No SSH Keys**: Traditional SSH key management eliminated
- ✅ **Systems Manager Session Manager**: Secure shell access via AWS Systems Manager (SSM channel, not traditional SSH)
- ✅ **IAM-Based Authentication**: Access controlled through IAM policies
- ✅ **No Open Ports**: No SSH port (22) exposed to internet
- ✅ **Audit Trail**: All access attempts are logged in CloudTrail
- **Benefits**: 
  - No SSH key rotation needed
  - Centralized access control
  - All sessions are auditable

**API Access:**
- ✅ **Single Entry Point**: All traffic must go through the ALB
- ✅ **SSL/TLS Encryption**: HTTPS only (HTTP redirects to HTTPS)
- ✅ **API Key Authentication**: All business endpoints require valid API key
- ✅ **Health Check Exception**: The `/health` endpoint is unauthenticated so that ALB health checks can succeed without a key
- ✅ **CORS Protection**: Only allows requests from approved origins
- ❌ **No Direct Access**: EC2 Nginx port (80) is not accessible from internet

**Database Access:**
- ✅ **Private Subnet Only**: No internet route to database
- ✅ **Security Group Restriction**: Only EC2 instance can connect
- ✅ **Encrypted Credentials**: Database password stored in AWS Secrets Manager
- ✅ **IAM Role**: EC2 uses IAM role to retrieve credentials (no hardcoded passwords)

#### 4. Application Security

**FastAPI Application:**
- ✅ **API Key Authentication**: All business endpoints require `X-API-Key` header
- ✅ **Health Check Exception**: The `/health` endpoint is excluded from API key authentication to allow ALB health checks
- ✅ **CORS Configuration**: Restricts requests to approved frontend domains
- ✅ **Rate Limiting**: IP-based rate limiting (300 requests/minute, 5000 requests/hour)
- ✅ **Request Logging**: Comprehensive logging of all requests with IP, method, path, auth status, and response time
- ✅ **Input Validation**: Pydantic models validate all request data
- ✅ **Error Sanitization**: Error messages don't expose sensitive information
- ✅ **SQL Injection Protection**: Parameterized queries via SQLAlchemy

**Nginx Reverse Proxy:**
- ✅ **Security Headers**: 
  - `X-Content-Type-Options: nosniff`
  - `X-Frame-Options: DENY`
  - `X-XSS-Protection: 1; mode=block`
  - `Strict-Transport-Security: max-age=31536000; includeSubDomains`
- ✅ **Request Filtering**: Can filter malicious requests before reaching application

### Why This Architecture Works

**Even though EC2 is in a public subnet**, the security is robust because:

1. **Security Groups Act as Firewalls:**
   - Security groups are stateful firewalls at the instance level
   - The API security group explicitly blocks all traffic except from the ALB
   - Even if someone knows the EC2 IP address, they cannot reach port 80 (Nginx)
   - The ALB is the only component that can communicate with Nginx on EC2

2. **RDS is Fully Isolated:**
   - Located in a private subnet (no internet gateway route)
   - Security group only allows connections from the EC2 instance
   - No public IP address
   - Cannot be reached from the internet under any circumstances
   - This is the most critical component and has the strongest protection

3. **No Direct SSH Exposure:**
   - Systems Manager Session Manager provides secure shell access via SSM channel (not traditional SSH)
   - Eliminates the need for open SSH ports
   - IAM-based access control (no SSH keys to manage or rotate)
   - All access is logged and auditable via CloudTrail
   - More secure than traditional SSH key management

4. **Single Entry Point:**
   - ALB is the only public-facing component
   - All traffic is encrypted (HTTPS)
   - SSL/TLS certificates managed by AWS Certificate Manager
   - Health checks ensure only healthy instances receive traffic

5. **Edge-Level Redirects:**
   - HTTP→HTTPS redirects happen at ALB edge (before reaching EC2)
   - HTTP requests never reach EC2 instances (reduces load)
   - Nginx configuration is simpler (no redirect logic needed)
   - Better performance (redirects handled by AWS edge infrastructure)

### Cost vs Security Trade-off

**This architecture provides:**
- ✅ Enterprise-grade security for critical components (RDS fully isolated)
- ✅ Strong security for application layer (security groups + ALB)
- ✅ Cost-effective (~$51/month vs ~$85-90 with NAT Gateway)
- ✅ Suitable for portfolio projects demonstrating security awareness
- ✅ Practical balance between security and cost

**Architecture Decision Rationale:**
- **EC2 in Public Subnet**: Allows direct internet access for updates and AWS API calls without NAT Gateway
- **Security Groups**: Provide defense-in-depth protection even in public subnet
- **RDS in Private Subnet**: Most critical component gets strongest isolation
- **No NAT Gateway**: Saves ~$32/month while maintaining strong security

**For production enterprise environments**, you might consider:
- Moving EC2 to private subnet with NAT Gateway for additional isolation
- Additional WAF (Web Application Firewall) rules for DDoS protection
- Enhanced monitoring and intrusion detection (GuardDuty, CloudWatch Alarms)
- Multi-AZ deployment for high availability
- Automated scaling based on load (Auto Scaling Groups)

### Security Verification

You can verify the security configuration using AWS CLI:

```bash
# Check API Security Group (should only allow ALB)
aws ec2 describe-security-groups --group-ids <API_SECURITY_GROUP_ID>

# Check RDS Security Group (should only allow EC2)
aws ec2 describe-security-groups --group-ids <RDS_SECURITY_GROUP_ID>

# Verify RDS is not publicly accessible
aws rds describe-db-instances --db-instance-identifier <DB_INSTANCE_ID> \
    --query 'DBInstances[0].PubliclyAccessible'

# Check ALB security group (should allow HTTPS/HTTP from internet)
aws ec2 describe-security-groups --group-ids <ALB_SECURITY_GROUP_ID>
```

## 🛠️ Technical Implementation

### Technology Stack

**Application Layer:**
- **FastAPI**: Modern Python web framework with automatic OpenAPI documentation
- **Uvicorn**: ASGI server for running FastAPI application
- **SQLAlchemy**: ORM for database interactions
- **Pydantic**: Data validation and settings management

**Infrastructure:**
- **Amazon Linux 2023**: Operating system on EC2 instances
- **Python 3.11**: Runtime environment
- **Nginx**: Reverse proxy and security header management
- **Systemd**: Service management for FastAPI application

**AWS Services:**
- **EC2 (t3.small)**: Compute instance for application hosting
- **Application Load Balancer**: Traffic distribution and SSL termination
- **RDS PostgreSQL (db.t3.micro)**: Managed database service
- **Route 53**: DNS management
- **ACM**: SSL/TLS certificate management
- **Secrets Manager**: Secure credential storage
- **Systems Manager**: Secure instance access

### Environment Variables

The API uses environment variables configured in `/opt/dbt_dental_clinic/api/.env` on the EC2 instance. These are loaded at runtime by the application.

**Required Variables:**
```bash
# Environment
API_ENVIRONMENT=production

# Database Connection (retrieved from Secrets Manager)
POSTGRES_ANALYTICS_HOST=<RDS endpoint>
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_DB=opendental_analytics
POSTGRES_ANALYTICS_USER=analytics_user
POSTGRES_ANALYTICS_PASSWORD=<retrieved from Secrets Manager>

# API Configuration
API_PORT=8000
API_HOST=0.0.0.0
API_CORS_ORIGINS=https://dbtdentalclinic.com,https://www.dbtdentalclinic.com

# Security
DEMO_API_KEY=<generated API key>
```

**Security Notes:**
- Database password is retrieved from AWS Secrets Manager at startup
- API key is generated during deployment and stored securely
- Environment file has restricted permissions (600) on EC2
- No credentials are hardcoded in application code

### Service Management

The API runs as a systemd service on the EC2 instance for automatic startup and process management.

**Service Management Commands:**
```bash
# Check service status
sudo systemctl status dental-clinic-api

# View recent logs
sudo journalctl -u dental-clinic-api -n 50

# View live logs
sudo journalctl -u dental-clinic-api -f

# Restart service
sudo systemctl restart dental-clinic-api

# Stop service
sudo systemctl stop dental-clinic-api

# Start service
sudo systemctl start dental-clinic-api

# Enable service (start on boot)
sudo systemctl enable dental-clinic-api
```

**Service Configuration:**
- **User**: Runs as `ec2-user` (non-root)
- **Working Directory**: `/opt/dbt_dental_clinic/api`
- **Virtual Environment**: Uses Python virtual environment for dependencies
- **Auto-restart**: Service automatically restarts on failure
- **Environment Variables**: Loaded from `.env` file

### Nginx Configuration

Nginx acts as a reverse proxy, adding security headers and routing traffic to the FastAPI application.

**Important:** Nginx receives **HTTP traffic only** (not HTTPS). This is because:
- ALB terminates SSL/TLS at the edge (before traffic reaches EC2)
- ALB forwards decrypted HTTP traffic to EC2 on port 80 (Nginx)
- Nginx listens on port 80 to receive HTTP traffic from ALB
- Nginx proxies requests to FastAPI on port 8000 (internal, localhost only)
- Nginx does **NOT** handle HTTP→HTTPS redirects (ALB handles all redirects at the edge)

**Architecture:**
- **ALB Target**: Port 80 (Nginx)
- **Nginx Listens**: Port 80 (receives traffic from ALB)
- **Nginx Proxies To**: FastAPI on port 8000 (internal communication)
- **FastAPI Runs**: Port 8000 (only accessible from Nginx on localhost)

**Key Features:**
- **Reverse Proxy**: Routes HTTP requests from ALB to FastAPI on port 8000 (internal)
- **Security Headers**: Adds security headers to all responses
  - `X-Content-Type-Options: nosniff`
  - `X-Frame-Options: DENY`
  - `X-XSS-Protection: 1; mode=block`
  - `Strict-Transport-Security: max-age=31536000; includeSubDomains`
- **Health Check Endpoint**: Nginx can handle `/health` requests directly (returns 200) or proxy to FastAPI
  - **Note**: `/health` endpoint is unauthenticated (no API key required) so ALB health checks succeed
- **Request Logging**: Logs all requests for monitoring
- **Simplified Configuration**: No redirect logic needed (handled by ALB)

**Nginx Management:**
```bash
# Test configuration
sudo nginx -t

# Reload configuration
sudo systemctl reload nginx

# Check status
sudo systemctl status nginx

# View access logs
sudo tail -f /var/log/nginx/access.log

# View error logs
sudo tail -f /var/log/nginx/error.log
```

### Deployment

For complete deployment instructions, see `docs/API_DEPLOYMENT_OPTION2_EC2_ALB.md`.

**Deployment Overview:**
1. **Network Infrastructure**: VPC, subnets, security groups
2. **SSL Certificate**: Request and validate ACM certificate
3. **Database**: Create RDS instance in private subnet
4. **Load Balancer**: Create ALB with:
   - **Listener 443 (HTTPS)**: Forwards to target group (default action)
   - **Listener 80 (HTTP)**: Redirects to HTTPS:443 (default action, HTTP 301)
   - **Target Group**: HTTP protocol, port 80 (targets Nginx on EC2)
5. **EC2 Instance**: Launch and configure application server
   - **Security Group**: Allow port 80 from ALB security group only
   - **Nginx**: Listens on port 80, proxies to FastAPI on port 8000
6. **DNS**: Configure Route 53 records
7. **Verification**: Test API endpoints and health checks

**ALB Listener Configuration:**
- **Port 443 (HTTPS)**: 
  - Protocol: HTTPS
  - Certificate: ACM certificate
  - Default action: Forward to target group (EC2 instances)
- **Port 80 (HTTP)**:
  - Protocol: HTTP
  - Default action: Redirect to HTTPS:443
  - Redirect status code: HTTP 301 (permanent redirect)
  - **Result**: All HTTP traffic redirected at ALB edge, never reaches EC2

## 📚 API Documentation

The API includes automatic OpenAPI documentation generated by FastAPI:

- **Swagger UI**: `https://api.dbtdentalclinic.com/docs`
  - Interactive API documentation
  - Test endpoints directly from browser
  - View request/response schemas

- **ReDoc**: `https://api.dbtdentalclinic.com/redoc`
  - Alternative documentation interface
  - Clean, readable format

- **OpenAPI JSON**: `https://api.dbtdentalclinic.com/openapi.json`
  - Machine-readable API specification
  - Can be imported into API testing tools

## 🔐 Application Security Features

### API Key Authentication

**Overview:**
All business endpoints require a valid API key in the `X-API-Key` header. This provides a simple but effective authentication mechanism for portfolio projects.

**How It Works:**
1. API key is stored in `.ssh/dbt-dental-clinic-api-key.pem` file (development) or environment variable (production)
2. Client includes API key in request header: `X-API-Key: <your-api-key>`
3. FastAPI validates the key against the configured `DEMO_API_KEY`
4. Invalid or missing keys return `401 Unauthorized`

**Public Endpoints (No API Key Required):**
- `GET /` - Root endpoint for API discovery
- `GET /health` - Health check endpoint (for ALB health checks)
- `GET /docs` - OpenAPI documentation
- `GET /openapi.json` - OpenAPI specification

**Protected Endpoints (API Key Required):**
- All `/patients/*` endpoints
- All `/revenue/*` endpoints
- All `/ar/*` endpoints
- All `/providers/*` endpoints
- All `/appointments/*` endpoints
- All `/reports/*` endpoints
- All `/treatment-acceptance/*` endpoints
- All `/hygiene/*` endpoints
- All `/dbt/*` endpoints

**Error Responses:**
- `401 Unauthorized`: Missing or invalid API key
  ```json
  {
    "detail": "API Key required"
  }
  ```
  or
  ```json
  {
    "detail": "Invalid API Key"
  }
  ```

**Frontend Integration:**
The frontend automatically includes the API key in all requests via axios interceptors. The API key is configured via the `VITE_API_KEY` environment variable.

### Rate Limiting

**Overview:**
IP-based rate limiting prevents abuse and ensures fair resource usage. Rate limits are enforced per IP address.

**Limits:**
- **300 requests per minute** per IP address
- **5000 requests per hour** per IP address

**Implementation:**
- In-memory storage (suitable for single-instance deployments)
- Automatic cleanup of old request records
- Separate tracking for minute and hour windows

**Exempt Endpoints:**
Rate limiting is skipped for:
- `/` (root endpoint)
- `/health` (health checks)
- `/docs`, `/openapi.json`, `/redoc` (documentation)
- `OPTIONS` requests (CORS preflight)

**Rate Limit Headers:**
All responses include rate limit information:
- `X-RateLimit-Limit-Minute`: Maximum requests per minute (300)
- `X-RateLimit-Remaining-Minute`: Remaining requests in current minute window
- `X-RateLimit-Limit-Hour`: Maximum requests per hour (5000)
- `X-RateLimit-Remaining-Hour`: Remaining requests in current hour window

**Error Response:**
When rate limit is exceeded:
- **Status Code**: `429 Too Many Requests`
- **Response Body**:
  ```json
  {
    "detail": "Rate limit exceeded. Maximum 300 requests per minute."
  }
  ```
  or
  ```json
  {
    "detail": "Rate limit exceeded. Maximum 5000 requests per hour."
  }
  ```

**Production Considerations:**
For multi-instance deployments, consider using Redis-based rate limiting to share limits across instances.

### Request Logging

**Overview:**
All HTTP requests are logged with comprehensive details for security monitoring and troubleshooting.

**Log Format:**
```
[timestamp] [IP] [METHOD] [PATH] [AUTH_STATUS] [STATUS_CODE] [RESPONSE_TIME_MS]
```

**Example Log Entry:**
```
2025-01-05 19:45:23,456 - api.middleware.request_logger - INFO - [127.0.0.1] [GET] [/patients/] [authenticated] [200] [45.23ms]
```

**Logged Information:**
- **Client IP Address**: Original IP or first IP from `X-Forwarded-For` header (for proxy scenarios)
- **HTTP Method**: GET, POST, OPTIONS, etc.
- **Endpoint Path**: Requested URL path
- **Authentication Status**: `authenticated` or `unauthenticated` (based on presence of API key)
- **Response Status Code**: HTTP status code (200, 401, 404, 429, etc.)
- **Response Time**: Request processing time in milliseconds

**Why This Matters:**
- **Security Monitoring**: Identify suspicious patterns or unauthorized access attempts
- **Performance Analysis**: Track slow endpoints and optimize accordingly
- **Audit Trail**: Maintain records of all API access for compliance
- **Troubleshooting**: Quickly identify issues with specific endpoints or clients

**Log Location:**
- Development: Console output (stdout)
- Production: Systemd journal (`journalctl -u dental-clinic-api`)

### CORS Configuration

**Overview:**
Cross-Origin Resource Sharing (CORS) is configured to restrict API access to approved frontend domains only.

**Allowed Origins:**
- **Development**: `http://localhost:3000`, `http://127.0.0.1:3000`
- **Production**: `https://dbtdentalclinic.com`, `https://www.dbtdentalclinic.com`

**Configuration:**
- **Allowed Methods**: `GET`, `POST`, `OPTIONS`
- **Allowed Headers**: `Content-Type`, `X-API-Key`, `Accept`
- **Exposed Headers**: Rate limit headers (`X-RateLimit-*`)
- **Credentials**: Allowed (`Access-Control-Allow-Credentials: true`)
- **Preflight Cache**: 3600 seconds (1 hour)

**CORS Headers in Responses:**
- `Access-Control-Allow-Origin`: The allowed origin (matches request origin)
- `Access-Control-Allow-Methods`: `GET, POST, OPTIONS`
- `Access-Control-Allow-Headers`: `Accept, Accept-Language, Content-Language, Content-Type, X-API-Key`
- `Access-Control-Allow-Credentials`: `true`
- `Access-Control-Expose-Headers`: Rate limit headers
- `Access-Control-Max-Age`: `3600` (for preflight requests)

**Error Response:**
When origin is not allowed:
- **Status Code**: `400 Bad Request`
- **Response Body**: `Disallowed CORS origin`

**Why This Matters:**
- **Prevents Unauthorized Access**: Only approved frontend domains can access the API
- **Reduces Attack Surface**: Blocks requests from unknown origins
- **Browser Security**: Enforces same-origin policy for web applications

### Security Best Practices

**API Key Management:**
- ✅ API keys are stored securely (not in version control)
- ✅ Different keys for development and production
- ✅ Keys can be rotated by updating environment variables
- ⚠️ For production, consider using AWS Secrets Manager or similar service

**Rate Limiting:**
- ✅ Prevents abuse and DDoS attacks
- ✅ Ensures fair resource usage
- ✅ Headers provide transparency to clients
- ⚠️ For high-traffic applications, consider Redis-based distributed rate limiting

**Request Logging:**
- ✅ Comprehensive audit trail
- ✅ Helps identify security incidents
- ✅ Useful for performance monitoring
- ⚠️ Consider log rotation and retention policies for production

**CORS:**
- ✅ Explicit origin allowlist (no wildcards in production)
- ✅ Minimal required headers only
- ✅ Credentials properly configured
- ⚠️ Regularly review and update allowed origins

**Error Handling:**
- ✅ Error messages don't expose sensitive information
- ✅ Consistent error format for client handling
- ✅ Detailed errors logged server-side for debugging

## 🔍 Monitoring & Troubleshooting

### Health Checks

**Application Health Endpoint:**
- **URL**: `GET /` or `GET /health`
- **Response**: JSON with API status and version
- **Purpose**: Verify API is running and responsive

**ALB Health Check:**
- **Path**: `/health`
- **Protocol**: HTTP
- **Port**: 80 (Nginx)
- **Interval**: 30 seconds
- **Timeout**: 5 seconds
- **Healthy Threshold**: 2 consecutive successes
- **Unhealthy Threshold**: 3 consecutive failures
- **Authentication**: No API key required (health check endpoint is unauthenticated)
- **Note**: Nginx can handle `/health` directly (returns 200) or proxy to FastAPI. The endpoint is excluded from API key authentication to allow ALB health checks to succeed.

**Target Health Status:**
- **Healthy**: Instance is receiving traffic
- **Unhealthy**: Instance is removed from rotation
- **Initial**: Health check in progress

### Logs

**Application Logs:**
```bash
# View systemd service logs
sudo journalctl -u dental-clinic-api -f

# View last 100 lines
sudo journalctl -u dental-clinic-api -n 100

# View logs since boot
sudo journalctl -u dental-clinic-api -b
```

**Nginx Logs:**
```bash
# Access logs (all requests)
sudo tail -f /var/log/nginx/access.log

# Error logs (errors and warnings)
sudo tail -f /var/log/nginx/error.log
```

**CloudWatch Logs (if configured):**
```bash
# View logs via AWS CLI
aws logs tail /dental-clinic/api --follow

# View specific log stream
aws logs get-log-events \
    --log-group-name /dental-clinic/api \
    --log-stream-name <stream-name>
```

### Common Issues & Solutions

**503 Service Unavailable:**
- **Cause**: FastAPI service is not running
- **Solution**: 
  ```bash
  sudo systemctl status dental-clinic-api
  sudo systemctl start dental-clinic-api
  ```

**502 Bad Gateway:**
- **Cause**: Nginx cannot connect to FastAPI or service is down
- **Solution**: 
  ```bash
  # Check FastAPI service
  sudo systemctl status dental-clinic-api
  
  # Check Nginx configuration
  sudo nginx -t
  
  # Restart both services
  sudo systemctl restart dental-clinic-api
  sudo systemctl restart nginx
  ```

**Connection Timeout:**
- **Cause**: Security group rules blocking traffic or target unhealthy
- **Solution**:
  ```bash
  # Check target group health
  aws elbv2 describe-target-health --target-group-arn <TG_ARN>
  
  # Check security group rules
  aws ec2 describe-security-groups --group-ids <SECURITY_GROUP_ID>
  ```

**Database Connection Errors:**
- **Cause**: RDS security group not allowing EC2 or credentials incorrect
- **Solution**:
  ```bash
  # Verify RDS security group allows EC2
  aws ec2 describe-security-groups --group-ids <RDS_SG_ID>
  
  # Test connection from EC2
  psql -h <RDS_ENDPOINT> -U analytics_user -d opendental_analytics
  ```

**SSL Certificate Issues:**
- **Cause**: Certificate not issued or DNS not configured
- **Solution**:
  ```bash
  # Check certificate status
  aws acm describe-certificate --certificate-arn <CERT_ARN> --region us-east-1
  
  # Verify DNS records
  dig api.dbtdentalclinic.com
  ```

### Accessing EC2 Instance

**Using Systems Manager Session Manager (Recommended):**
```bash
# Connect to EC2 instance (provides shell access via SSM channel)
aws ssm start-session --target <INSTANCE_ID>

# Run commands via Systems Manager
aws ssm send-command \
    --instance-ids <INSTANCE_ID> \
    --document-name "AWS-RunShellScript" \
    --parameters commands="sudo systemctl status dental-clinic-api"
```

**Benefits:**
- No SSH keys needed
- IAM-based access control
- All sessions logged
- No open SSH ports
- Uses AWS Systems Manager protocol (not traditional SSH)

## 💰 Cost Breakdown

**Monthly Estimated Costs:**
- **EC2 t3.small**: ~$15/month
- **Application Load Balancer**: ~$16/month
- **RDS db.t3.micro**: ~$15/month
- **Data Transfer**: ~$5/month
- **Route 53**: ~$0.50/month (hosted zone)
- **ACM**: Free (public certificates)
- **Secrets Manager**: ~$0.40/month
- **Total**: ~$51/month

**Cost Optimization:**
- No NAT Gateway needed (saves ~$32/month)
- Single EC2 instance (can scale if needed)
- Single RDS instance (can enable Multi-AZ for HA)
- Reserved Instances available for 1-3 year commitments (30-60% savings)

## 📖 Additional Resources

- **Deployment Guide**: `docs/API_DEPLOYMENT_OPTION2_EC2_ALB.md` - Complete step-by-step deployment instructions
- **Deployment Notes**: `DEPLOYMENT_NOTES.md` - Deployment history and configuration details
- **Main Project README**: `../README.md` - Overall project documentation

## 🔗 Related Documentation

- **AWS EC2 Documentation**: [EC2 User Guide](https://docs.aws.amazon.com/ec2/)
- **AWS ALB Documentation**: [Application Load Balancer Guide](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/)
- **AWS RDS Documentation**: [RDS User Guide](https://docs.aws.amazon.com/rds/)
- **FastAPI Documentation**: [FastAPI Docs](https://fastapi.tiangolo.com/)
- **Systems Manager Session Manager**: [Session Manager Guide](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager.html)
