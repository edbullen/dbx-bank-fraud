-- Select the count and amount / value of fraud transactions over time - chunked into 1 hr series
-- use this for populating the Fraud over time sample dashboard, as shown in the README screenshot

-- NOTE: set the Unity Catalog and Schema as appropriate to pick up the location of view gold_transactions
SELECT, count(1) nbFraudTransactions, sum(amount) sumFraudTransactions
FROM (
    select from_unixtime(unix_timestamp(current_timestamp()) + (step * 3600)) ts, amount
    from gold_transactions
    join (--just getting the last 30 days
        select max(step)-(24*30) windowStart, max(step) windowEnd
        from gold_transactions
    ) on step >= windowStart and step <= windowEnd
    where is_fraud
)
GROUP BY ts
ORDER BY ts