SELECT
    year,
    month,
    COUNT(DISTINCT user_id) AS unique_users
FROM
    transactions
WHERE
    transaction_type = 'purchase'
GROUP BY
    year,
    month
ORDER BY
    year,
    month;
