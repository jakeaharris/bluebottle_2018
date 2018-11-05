INSERT INTO hourly_weather
(
    station_name,
    date,
    hourly_dry_bulb_temp,
    daily_avg_dry_bulb_temp
)
SELECT
    STATION_NAME,
    DATE,
    HOURLYDRYBULBTEMPF,
    DAILYAverageDryBulbTemp
FROM stage_hourly_weather
;
