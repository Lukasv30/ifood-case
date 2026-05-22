SELECT
    pickup_year,
    pickup_month,
    avg_total_amount,
    trip_count
FROM workspace.ifood_case.gold_avg_total_amount_by_month
ORDER BY pickup_year, pickup_month;