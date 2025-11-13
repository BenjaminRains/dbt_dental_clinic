from database import get_db
from sqlalchemy import text

db = next(get_db())

# Check what claim_status values exist
print("\n=== Checking Claim Status Values ===")
result = db.execute(text("""
    SELECT 
        claim_status, 
        COUNT(*) as count,
        COUNT(CASE WHEN claim_date >= CURRENT_DATE - INTERVAL '1 year' THEN 1 END) as last_year,
        COUNT(CASE WHEN billed_amount > 0 AND (billed_amount - COALESCE(paid_amount, 0)) > 0 THEN 1 END) as with_unpaid
    FROM raw_marts.fact_claim 
    GROUP BY claim_status 
    ORDER BY count DESC
""")).fetchall()

for r in result:
    print(f"{r[0]}: Total={r[1]}, Last Year={r[2]}, With Unpaid={r[3]}")

# Check specific claim rejections
print("\n=== Checking Claim Rejections (Denied/Rejected/Pending) ===")
result = db.execute(text("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN claim_date >= CURRENT_DATE - INTERVAL '1 year' THEN 1 END) as last_year,
        COUNT(CASE WHEN billed_amount > 0 AND (billed_amount - COALESCE(paid_amount, 0)) > 0 THEN 1 END) as with_unpaid,
        COUNT(CASE WHEN claim_date >= CURRENT_DATE - INTERVAL '1 year' 
                   AND billed_amount > 0 
                   AND (billed_amount - COALESCE(paid_amount, 0)) > 0 THEN 1 END) as matching_criteria
    FROM raw_marts.fact_claim
    WHERE claim_status IN ('Denied', 'Rejected', 'Pending')
        AND claim_date IS NOT NULL
""")).fetchall()

for r in result:
    print(f"Total: {r[0]}, Last Year: {r[1]}, With Unpaid: {r[2]}, Matching All Criteria: {r[3]}")

# Sample some records
print("\n=== Sample Claim Rejection Records ===")
result = db.execute(text("""
    SELECT 
        claim_id,
        claim_date,
        claim_status,
        billed_amount,
        paid_amount,
        (billed_amount - COALESCE(paid_amount, 0)) as unpaid_amount
    FROM raw_marts.fact_claim
    WHERE claim_status IN ('Denied', 'Rejected', 'Pending')
        AND claim_date IS NOT NULL
    LIMIT 10
""")).fetchall()

for r in result:
    print(f"ID: {r[0]}, Date: {r[1]}, Status: {r[2]}, Billed: {r[3]}, Paid: {r[4]}, Unpaid: {r[5]}")

