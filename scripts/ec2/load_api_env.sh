#!/bin/bash
# Load KEY=VALUE env files without shell-evaluating values.
# Safe for passwords containing ?, !, :, spaces, etc.
# Also strips CR so Windows CRLF files work when sourced on Linux.
#
# Usage:
#   source /opt/dbt_dental_clinic/scripts/ec2/load_api_env.sh
#   load_clinic_dbt_env   # prefers api/.env, else setup_ec2_dbt_env.sh
#   overlay_live_clinic_rds_password  # optional: replace password from Secrets Manager

load_dotenv_file() {
    local env_file="$1"
    local line key val
    [ -f "$env_file" ] || return 1
    while IFS= read -r line || [ -n "$line" ]; do
        line="${line%$'\r'}"
        case "$line" in
            ""|\#*) continue ;;
        esac
        case "$line" in
            *=*)
                key="${line%%=*}"
                val="${line#*=}"
                case "$key" in
                    *[!A-Za-z0-9_]*|"") continue ;;
                esac
                export "$key=$val"
                ;;
        esac
    done < "$env_file"
    return 0
}

# Fetch current RDS master password from Secrets Manager (rotation-safe).
# Prefer API venv boto3 (system aws CLI on Amazon Linux may be broken).
# Requires instance IAM: secretsmanager:GetSecretValue on rds!db-*, rds:DescribeDBInstances.
# Set API_CLINIC_RDS_LIVE_PASSWORD=0 to skip. Falls back to file password on failure.
overlay_live_clinic_rds_password() {
    case "${API_CLINIC_RDS_LIVE_PASSWORD:-1}" in
        0|false|FALSE|no|NO|off|OFF) return 0 ;;
    esac

    local root="${MDC_PROJECT_ROOT:-/opt/dbt_dental_clinic}"
    local py=""
    if [ -x "$root/api/venv/bin/python" ]; then
        py="$root/api/venv/bin/python"
    elif command -v python3 >/dev/null 2>&1; then
        py="python3"
    else
        echo "WARNING: python not found; using file password for dbt (may be stale after rotation)" >&2
        return 0
    fi

    local region="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
    local instance_id="${CLINIC_RDS_INSTANCE_ID:-dental-clinic-analytics}"
    local secret_id="${CLINIC_RDS_MASTER_SECRET_ARN:-}"
    local out=""

    out=$(
        CLINIC_RDS_MASTER_SECRET_ARN="$secret_id" \
        CLINIC_RDS_INSTANCE_ID="$instance_id" \
        AWS_DEFAULT_REGION="$region" \
        AWS_REGION="$region" \
        "$py" - <<'PY'
import json
import os
import sys

try:
    import boto3
except ImportError:
    print("NO_BOTO3", file=sys.stderr)
    sys.exit(2)

region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
instance_id = os.environ.get("CLINIC_RDS_INSTANCE_ID") or "dental-clinic-analytics"
secret_id = (os.environ.get("CLINIC_RDS_MASTER_SECRET_ARN") or "").strip()

try:
    if not secret_id:
        rds = boto3.client("rds", region_name=region)
        resp = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
        instances = resp.get("DBInstances") or []
        if not instances:
            print("NO_INSTANCE", file=sys.stderr)
            sys.exit(3)
        master = instances[0].get("MasterUserSecret") or {}
        secret_id = (master.get("SecretArn") or "").strip()
        if not secret_id:
            print("NO_MASTER_SECRET", file=sys.stderr)
            sys.exit(3)

    sm = boto3.client("secretsmanager", region_name=region)
    response = sm.get_secret_value(SecretId=secret_id)
    raw = (response.get("SecretString") or "").strip()
    if not raw and response.get("SecretBinary"):
        import base64
        raw = base64.b64decode(response["SecretBinary"]).decode("utf-8").strip()
    if not raw:
        print("EMPTY_SECRET", file=sys.stderr)
        sys.exit(4)

    if raw.startswith("{"):
        payload = json.loads(raw)
        pw = str(payload.get("password") or "").strip()
        if pw.startswith("{"):
            try:
                inner = json.loads(pw)
                if isinstance(inner, dict) and inner.get("password"):
                    pw = str(inner["password"]).strip()
            except Exception:
                pass
    else:
        pw = raw

    if not pw:
        print("NO_PASSWORD_FIELD", file=sys.stderr)
        sys.exit(4)

    # stdout: password only; stderr: source label for logs
    print(f"secrets_manager:{secret_id}", file=sys.stderr)
    print(pw, end="")
except Exception as exc:
    print(f"FETCH_FAIL:{type(exc).__name__}:{exc}", file=sys.stderr)
    sys.exit(5)
PY
    ) || true

    if [ -n "$out" ]; then
        export POSTGRES_ANALYTICS_PASSWORD="$out"
        echo "Loaded clinic RDS password from Secrets Manager (boto3)" >&2
    else
        echo "WARNING: Secrets Manager password fetch failed; using file password for dbt" >&2
    fi
    return 0
}

load_clinic_dbt_env() {
    local root="${MDC_PROJECT_ROOT:-/opt/dbt_dental_clinic}"
    if [ -f "$root/api/.env" ]; then
        load_dotenv_file "$root/api/.env"
    elif [ -f "$root/scripts/ec2/setup_ec2_dbt_env.sh" ]; then
        # shellcheck source=/dev/null
        . "$root/scripts/ec2/setup_ec2_dbt_env.sh"
    elif [ -f "$root/scripts/setup_ec2_dbt_env.sh" ]; then
        # shellcheck source=/dev/null
        . "$root/scripts/setup_ec2_dbt_env.sh"
    else
        echo "ERROR: no api/.env or setup_ec2_dbt_env.sh found under $root" >&2
        return 1
    fi
    overlay_live_clinic_rds_password
    return 0
}
