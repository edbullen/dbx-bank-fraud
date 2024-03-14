-- Select the amount / value of fraud transactions flowing in or out of a country
-- use this for populating the Fraud by Country sample dashboard, as shown in the README screenshot
-- also use this for showing the flow between countries as per the Sankey diagram in the README screenshot


-- NOTE: set the Unity Catalog and Schema as appropriate to pick up the location of view gold_transactions
SELECT countryOrig , countryDest , type, value
FROM (
    select countryOrig , countryDest , type, sum(amount ) as value
    from gold_transactions
    where is_fraud and amount > 350000
    group by countryOrig , countryDest ,type
)
ORDER BY value desc
limit 1000;