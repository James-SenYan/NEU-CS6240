REGISTER file:/lib/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

Bike_Original_table = LOAD 's3://emr-cs6240-s/data_large.csv' USING CSVLoader (',', 'SKIP_INPUT_HEADER') AS (trip_duration:int,
start_time:chararray, stop_time:chararray, start_station_id:int, start_station_name:chararray,
start_station_latitude:float, start_station_longitude:float,
end_station_id:int, end_station_name:chararray, end_station_latitude:float,
end_station_longitude:float, bike_id:int, user_type:int,
birth_year:datetime, gender:int);
Filtered_Bike_Info = FILTER Bike_Original_table BY (NOT (gender IS NULL));
Bike_With_Age = FOREACH Filtered_Bike_Info GENERATE trip_duration,
((INDEXOF(start_time, '/') >= 0) ? ToDate(SUBSTRING(start_time, 0, INDEXOF(start_time, ' ')), 'MM/dd/yyyy') :
       ToDate(SUBSTRING(start_time, 0, INDEXOF(start_time, ' ')), 'yyyy-MM-dd')) AS init_time,
start_station_id, end_station_id, bike_id, gender,
ROUND(((INDEXOF(start_time, '/') >= 0 ? ToUnixTime(ToDate(SUBSTRING(start_time, 0, INDEXOF(start_time, ' ')), 'MM/dd/yyyy')) :
              ToUnixTime(ToDate(SUBSTRING(start_time, 0, INDEXOF(start_time, ' ')), 'yyyy-MM-dd'))) - ToUnixTime(birth_year))/(365*24*60*60)) AS rider_age_second;
--dump Bike_With_Age;
Weather_Original_table = LOAD 's3://emr-cs6240-s/nyc_weather.csv' USING CSVLoader (',') AS (date:datetime,
Precipitation_in_inches:float, snowfall_in_inches:float, minimum_temperature:int, maximum_temperature:int);

Joined_Weather_Bike = JOIN Weather_Original_table BY date,
Bike_With_Age by init_time;

Filtered_Joined_Weather_Bike = FOREACH Joined_Weather_Bike GENERATE $1 AS precipitation, $11 AS rider_age;

grouped_rides = GROUP Filtered_Joined_Weather_Bike BY rider_age;
stats_rides = FOREACH grouped_rides GENERATE
    group AS age,
    COUNT(Filtered_Joined_Weather_Bike.precipitation) AS num_rides,
    AVG(Filtered_Joined_Weather_Bike.precipitation) AS avg_precipitation,
    MAX(Filtered_Joined_Weather_Bike.precipitation) AS max_precipitation,
    MIN(Filtered_Joined_Weather_Bike.precipitation) AS min_precipitation;
dump stats_rides;
