INSERT INTO morse_store_data
(
    order_date,
    item_name,
    net_quantity
)
-- S: 02-23-2016 11:05:25
-- G: 2016-01-01 07:53
SELECT
    SUBSTR(local_created_at, 7, 4) || "-" ||
    SUBSTR(local_created_at, 1, 2) || "-" ||
    SUBSTR(local_created_at, 4, 2) || " " ||
    SUBSTR(local_created_at, 12, 19),
    item_name,
    net_quantity
FROM stage_morse_store_data
;
