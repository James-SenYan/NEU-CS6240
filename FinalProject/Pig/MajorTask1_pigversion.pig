--REGISTER file:/lib/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
--s3://emr-cs6240-s/input/data.csv

Bike_Original_table = LOAD '../../../data_small.csv' USING CSVLoader (',', 'SKIP_INPUT_HEADER') AS (trip_duration:int,
start_time:datetime, stop_time:datetime, start_station_id:int, start_station_name:chararray,
start_station_latitude:float, start_station_longitude:float,
end_station_id:int, end_station_name:chararray, end_station_latitude:float,
end_station_longitude:float, bike_id:int, user_type:int,
birth_year:datetime, gender:int);

Filtered_Bike_Info = FILTER Bike_Original_table BY NOT (birth_year IS NULL);
Bike_With_Age = FOREACH Filtered_Bike_Info GENERATE trip_duration, start_time,
start_station_id, end_station_id, bike_id, gender,
(ROUND((ToUnixTime(start_time) - ToUnixTime(birth_year))/(365*24*60*60))) AS rider_age_second;

Weather_Original_table = LOAD '../../../nyc_weather.csv' USING CSVLoader (',') AS (date:datetime,
Precipitation_in_inches:float, snowfall_in_inches:float, minimum_temperature:int, maximum_temperature:int);

Joined_Weather_Bike = JOIN Weather_Original_table BY ToString(date, 'yyyy-MM-dd'),
Bike_With_Age by ToString(start_time, 'yyyy-MM-dd');

Filtered_Joined_Weather_Bike = FOREACH Joined_Weather_Bike GENERATE $1 AS precipitation, $11 AS rider_age;

grouped_rides = GROUP Filtered_Joined_Weather_Bike BY rider_age;
stats_rides = FOREACH grouped_rides GENERATE
    group AS age,
    COUNT(Filtered_Joined_Weather_Bike.precipitation) AS num_rides,
    AVG(Filtered_Joined_Weather_Bike.precipitation) AS avg_precipitation,
    MAX(Filtered_Joined_Weather_Bike.precipitation) AS max_precipitation,
    MIN(Filtered_Joined_Weather_Bike.precipitation) AS min_precipitation;
--Filtered_Flights1 = FILTER Flights1 BY ((origin == 'ORD') AND (dest != 'JFK') AND (cancelled != 1.00) AND (diverted != 1.00));
--Filtered_Flights2 = FILTER Flights2 BY ((origin != 'ORD') AND (dest == 'JFK') AND (cancelled != 1.00) AND (diverted != 1.00));
--Joined_Flight = JOIN Filtered_Flights1 BY (dest, flightDate), Filtered_Flights2 BY (origin, flightDate);
--Filtered_Joined_Flight = FILTER Joined_Flight BY $35 < $68;
--Final_Filter = FILTER Filtered_Joined_Flight BY ($5 <= ToDate('2008-05-31') AND $5 >= ToDate('2007-06-01') AND $49 <= ToDate('2008-05-31') AND $49 >= ToDate('2007-06-01'));
--sum_data = FOREACH Final_Filter GENERATE ($37 + $81) AS sum_col, 1 as id;
--group_data = GROUP sum_data by id;
--avg_data = FOREACH group_data GENERATE AVG(sum_data.sum_col), COUNT(sum_data.sum_col);
dump stats_rides;
