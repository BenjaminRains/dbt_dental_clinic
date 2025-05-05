from connection_factory import get_source_connection, get_target_connection

# Example: Test source connection
with get_source_connection().connect() as conn:
    result = conn.execute("SELECT 1")
    print(result.scalar())

# Example: Test target connection
with get_target_connection().connect() as conn:
    result = conn.execute("SELECT 1")
    print(result.scalar())
