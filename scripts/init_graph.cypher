// Create a unique constraint for Account nodes to prevent duplication and improve lookup speed
CREATE CONSTRAINT account_id_unique IF NOT EXISTS
FOR (a:Account) REQUIRE a.account_id IS UNIQUE;

// Create an index on the transaction timestamp for faster temporal queries
CREATE INDEX transaction_time_idx IF NOT EXISTS
FOR ()-[r:TRANSFERRED_TO]-() ON (r.timestamp);