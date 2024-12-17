SELECT is_fraud, source_file, count(*)
FROM silver_transactions
GROUP BY is_fraud, source_file
ORDER BY 2,1;
