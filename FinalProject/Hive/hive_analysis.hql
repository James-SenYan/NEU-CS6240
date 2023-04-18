DROP TABLE IF EXISTS trip_data;
DROP TABLE IF EXISTS weather_data;
CREATE TABLE trip_data (
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

CREATE TABLE weather_data (
    weather_date STRING,
    prcp FLOAT,
    snow FLOAT,
    snwd FLOAT,
    tmin INT,
    tmax INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://emr-cs6240-s/nyc_weather.csv' INTO TABLE weather_data;

SELECT (CAST(if(substr(trip_data.start_time, 1, 10) LIKE '%/%/%',
            substr(from_unixtime(unix_timestamp(substr(trip_data.start_time, 1, INSTR(trip_data.start_time, ' ') - 1), 'MM/dd/yyyy'), 'yyyy-MM-dd'), 1, 4),
            substr(trip_data.start_time, 1, 4)) AS INT) - CAST(trip_data.birth_year AS INT)) AS age,
    Count(*) as num_of_people, Max(weather_data.prcp) as max_precipitation,
    Avg(weather_data.prcp) as average_precipitation, Min(weather_data.prcp) as minimum_precipitation
FROM weather_data JOIN trip_data
ON if(substr(trip_data.start_time, 1, 10) LIKE '%/%/%',
      from_unixtime(unix_timestamp(substr(trip_data.start_time, 1, INSTR(trip_data.start_time, ' ') - 1), 'MM/dd/yyyy'), 'yyyy-MM-dd'),
      substr(trip_data.start_time, 1, INSTR(trip_data.start_time, ' ') - 1)) = weather_data.weather_date
WHERE trip_data.birth_year IS NOT NULL AND LENGTH(trip_data.start_time) >= 17
GROUP BY
    CAST(if(substr(trip_data.start_time, 1, 10) LIKE '%/%/%',
            substr(from_unixtime(unix_timestamp(substr(trip_data.start_time, 1, INSTR(trip_data.start_time, ' ') - 1), 'MM/dd/yyyy'), 'yyyy-MM-dd'), 1, 4),
            substr(trip_data.start_time, 1, 4)) AS INT) - CAST(trip_data.birth_year AS INT)
ORDER BY age;

-- SELECT * FROM trip_data;
-- SELECT * FROM weather_data;

