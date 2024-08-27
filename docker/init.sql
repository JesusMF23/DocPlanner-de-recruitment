CREATE TABLE IF NOT EXISTS weather_data (
    station_id VARCHAR(255),
    date TIMESTAMP,
    tavg FLOAT,
    tmin FLOAT,
    tmax FLOAT,
    prcp FLOAT,
    PRIMARY KEY (station_id, date)
);