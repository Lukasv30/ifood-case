SELECT
    pickup_hour,
    avg_passenger_count,
    trip_count
FROM workspace.ifood_case.gold_avg_passenger_count_may_by_hour
ORDER BY pickup_hour;