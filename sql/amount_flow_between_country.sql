select countryOrig , countryDest , type, value from (
    select countryOrig , countryDest , type, count(amount ) as value
    from gold_transactions
    where amount > 350000
    group by countryOrig , countryDest ,type
) order by value desc
limit 20

--in terms of number of transactions
--russia making / receiving more transactions > 350000, than othr countries
--most these transactions are transfer
