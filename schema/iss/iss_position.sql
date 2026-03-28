CREATE TABLE IF NOT EXISTS iss_position (
    id SERIAL PRIMARY KEY,
    longitude FLOAT,
    latitude FLOAT,
    timestamp INT,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uc_iss_position UNIQUE (longitude, latitude, timestamp)
)