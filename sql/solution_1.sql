WITH temp_prod_sum AS (
    SELECT
        hourly_dry_bulb_temp,
        item_name,
        sum(net_quantity) as item_count
    FROM hourly_weather w
    JOIN morse_store_data s
    ON 1=1
    AND strftime('%d-%m-%Y %H', w.date) = strftime('%d-%m-%Y %H', s.order_date)
    WHERE 1=1
    AND w.hourly_dry_bulb_temp != ''
    AND w.daily_avg_dry_bulb_temp = ''
    GROUP BY hourly_dry_bulb_temp, item_name
)
SELECT *
FROM temp_prod_sum t1
WHERE 1=1
AND t1.item_count = (
    SELECT max(item_count)
    FROM temp_prod_sum t2
    WHERE 1=1
    AND t2.hourly_dry_bulb_temp = t1.hourly_dry_bulb_temp
)
AND t1.item_name = (
    SELECT max(item_name)
    FROM temp_prod_sum t2
    WHERE 1=1
    AND t2.hourly_dry_bulb_temp = t1.hourly_dry_bulb_temp
    AND t2.item_count = t1.item_count
)
;
