-- Create ETL Load Status Table
CREATE TABLE IF NOT EXISTS raw.etl_load_status (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL UNIQUE,
    last_extracted TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
    rows_extracted INTEGER DEFAULT 0,
    extraction_status VARCHAR(50) DEFAULT 'pending',
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create ETL Transform Status Table
CREATE TABLE IF NOT EXISTS raw.etl_transform_status (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL UNIQUE,
    last_transformed TIMESTAMP NOT NULL DEFAULT '1969-01-01 00:00:00',
    rows_transformed INTEGER DEFAULT 0,
    transformation_status VARCHAR(50) DEFAULT 'pending',
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_etl_load_status_table_name 
ON raw.etl_load_status(table_name);

CREATE INDEX IF NOT EXISTS idx_etl_load_status_last_extracted 
ON raw.etl_load_status(last_extracted);

CREATE INDEX IF NOT EXISTS idx_etl_transform_status_table_name 
ON raw.etl_transform_status(table_name);

CREATE INDEX IF NOT EXISTS idx_etl_transform_status_last_transformed 
ON raw.etl_transform_status(last_transformed);

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON TABLE raw.etl_load_status TO analytics_user;
GRANT ALL PRIVILEGES ON TABLE raw.etl_transform_status TO analytics_user;
GRANT USAGE, SELECT ON SEQUENCE raw.etl_load_status_id_seq TO analytics_user;
GRANT USAGE, SELECT ON SEQUENCE raw.etl_transform_status_id_seq TO analytics_user; 