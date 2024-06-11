select type, is_fraud, count(1) as count
from gold_transactions
group by type, is_fraud;