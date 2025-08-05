SELECT
    customerid,
    last_order,              
    last_order_date_local,  
    second_last_order,
    second_last_order_date_local,
    COUNT(distinct t.orderid) AS order_count_for_customer -- Count orders for each customer within the filtered range
FROM
    (
        SELECT
            f.customerid,
            f.orderid,
            f.order_date_time_local,
            LAST_VALUE(f.orderid) OVER (PARTITION BY f.customerid ORDER BY f.order_date_time_local) AS last_order,
            LAST_VALUE(f.order_date_time_local) OVER (PARTITION BY f.customerid ORDER BY f.order_date_time_local) AS last_order_date_local,
          NTH_VALUE(f.orderid, 2) OVER (PARTITION BY f.customerid ORDER BY f.order_date_time_local DESC) AS second_last_order,
          NTH_VALUE(f.order_date_time_local, 2) OVER (PARTITION BY f.customerid ORDER BY f.order_date_time_local DESC) AS second_last_order_date_local
        FROM
            `fact_order` f
        LEFT JOIN
            `dim_order` d
            ON f.orderid = d.orderid
        WHERE
            f.orderdatetime BETWEEN DATETIME("2025-07-05") AND DATETIME_ADD(DATETIME("2025-08-05"), INTERVAL 1 DAY)
            AND d.orderday BETWEEN DATETIME("2025-07-05") AND DATETIME_ADD(DATETIME("2025-08-05"), INTERVAL 1 DAY)
            AND d.country = 'NL'
    ) AS t -- Alias the subquery as 't'
GROUP BY ALL -- Groups by all non-aggregated columns in the outer SELECT (customerid, last_order, last_order_date_local)
HAVING order_count_for_customer > 5
