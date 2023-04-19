CREATE TABLE IF NOT EXISTS trip_data (
    trip_duration INT,
    start_time STRING,
    stop_time STRING,
    start_station_id INT,
    start_station_name STRING,
    start_station_latitude DOUBLE,
    start_station_longitude DOUBLE,
    end_station_id INT,
    end_station_name STRING,
    end_station_latitude DOUBLE,
    end_station_longitude DOUBLE,
    bike_id INT,
    user_type STRING,
    birth_year STRING,
    gender INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");


LOAD DATA INPATH 's3://emr-cs6240-s/data_large.csv' INTO TABLE trip_data;

-- SELECT trip_data.start_station_name, Count(*) as station_frequency
-- FROM trip_data
-- WHERE trip_data.start_station_name IS NOT NULL
-- GROUP BY trip_data.start_station_name
-- ORDER BY station_frequency;

SELECT trip_data.start_station_id, trip_data.start_station_name, trip_data.end_station_id, trip_data.end_station_name, Count(*) as station_frequency
FROM trip_data
WHERE trip_data.start_station_name IS NOT NULL AND trip_data.start_station_id IS NOT NULL AND trip_data.end_station_id IS NOT NULL
GROUP BY trip_data.start_station_id, trip_data.end_station_id
ORDER BY station_frequency
-- SELECT * FROM trip_data;
-- SELECT * FROM weather_data;

