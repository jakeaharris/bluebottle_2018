WITH daily_item_temp AS (
    SELECT
        w.date,
        s.item_name,
        w.daily_avg_dry_bulb_temp,
        sum(s.net_quantity) as item_count
    FROM hourly_weather w
    JOIN morse_store_data s
    ON 1=1
    AND strftime('%d-%m-%Y', w.date) = strftime('%d-%m-%Y', s.order_date)
    WHERE 1=1
    AND w.hourly_dry_bulb_temp = ''
    AND w.daily_avg_dry_bulb_temp != ''
    GROUP BY
        strftime('%d-%m-%Y', w.date),
        s.item_name
)
SELECT
    t.item_name
    ,AVG(c.item_count - t.item_count) AS avg_next_day_cooler
    ,AVG(h.item_count - t.item_count) AS avg_next_day_hotter
FROM daily_item_temp t
LEFT JOIN daily_item_temp c
ON 1=1
AND c.item_name = t.item_name
AND strftime('%d-%m-%Y', c.date) = strftime('%d-%m-%Y', date(t.date, '+1 day'))
AND t.daily_avg_dry_bulb_temp - c.daily_avg_dry_bulb_temp >= 2
LEFT JOIN daily_item_temp h
ON 1=1
AND h.item_name = t.item_name
AND strftime('%d-%m-%Y', h.date) = strftime('%d-%m-%Y', date(t.date, '+1 day'))
AND h.daily_avg_dry_bulb_temp - t.daily_avg_dry_bulb_temp >=2
GROUP BY t.item_name
;
