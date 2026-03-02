// Detect circular transfer patterns (A -> B -> C -> A) within a depth of 3 to 5 hops
MATCH path = (a:Account)-[:TRANSFERRED_TO*3..5]->(a)
RETURN path, 
       nodes(path) AS involved_accounts, 
       length(path) AS hop_count
LIMIT 10;

// Identify accounts receiving more than 5 transactions in a short burst
MATCH (sender:Account)-[r:TRANSFERRED_TO]->(receiver:Account)
WITH receiver, count(r) AS transaction_count, sum(r.amount) AS total_amount
WHERE transaction_count > 5
RETURN receiver.account_id AS suspicious_account, 
       transaction_count, 
       total_amount
ORDER BY total_amount DESC;