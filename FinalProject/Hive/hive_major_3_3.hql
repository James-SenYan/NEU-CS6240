-- DROP TABLE IF EXISTS trip_data;
-- DROP TABLE IF EXISTS weather_data;
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

-- LOAD DATA INPATH 's3://emr-cs6240-s/data_large.csv' INTO TABLE trip_data;
SELECT CONCAT(start_station_name, ',',
              IF(substr(trip_data.start_time, 1, 10) LIKE '%/%/%',
                 from_unixtime(unix_timestamp(substr(trip_data.start_time, 1, INSTR(trip_data.start_time, ' ') - 1), 'MM/dd/yyyy'), 'EEEE'),
                 from_unixtime(unix_timestamp(substr(trip_data.start_time, 1, INSTR(trip_data.start_time, ' ') - 1), 'yyyy-MM-dd'), 'EEEE')
                  )
           ) AS day_of_week,
       COUNT(*) AS num_of_people
FROM trip_data
WHERE LENGTH(trip_data.start_time) >= 17
GROUP BY start_station_name, CONCAT(start_station_name, ',',
                                    IF(substr(trip_data.start_time, 1, 10) LIKE '%/%/%',
                                       from_unixtime(unix_timestamp(substr(trip_data.start_time, 1, INSTR(trip_data.start_time, ' ') - 1), 'MM/dd/yyyy'), 'EEEE'),
                                       from_unixtime(unix_timestamp(substr(trip_data.start_time, 1, INSTR(trip_data.start_time, ' ') - 1), 'yyyy-MM-dd'), 'EEEE')
                                        )
    );
-- SELECT * FROM trip_data;
-- SELECT * FROM weather_data;

