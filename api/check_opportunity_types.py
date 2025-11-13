from database import get_db
from sqlalchemy import text

db = next(get_db())
result = db.execute(text("""
    SELECT 
        opportunity_type, 
        COUNT(*) as count, 
        COUNT(CASE WHEN lost_revenue > 0 THEN 1 END) as with_revenue, 
        COUNT(CASE WHEN lost_revenue = 0 THEN 1 END) as zero_revenue, 
        COUNT(CASE WHEN lost_revenue IS NULL THEN 1 END) as null_revenue 
    FROM raw_marts.mart_revenue_lost 
    GROUP BY opportunity_type 
    ORDER BY opportunity_type
""")).fetchall()

print("\nOpportunity Type Distribution in mart_revenue_lost:")
print("=" * 80)
for r in result:
    print(f"{r[0]}:")
    print(f"  Total records: {r[1]}")
    print(f"  With revenue > 0: {r[2]}")
    print(f"  With revenue = 0: {r[3]}")
    print(f"  With revenue NULL: {r[4]}")
    print()

