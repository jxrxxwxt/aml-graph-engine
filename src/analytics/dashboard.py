import os
import logging
from neo4j import GraphDatabase
from datetime import datetime, timezone

# Configure professional logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connection details aligning with the .env configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "aml_graph_pass"

class AMLDashboard:
    """
    Core analytics dashboard for retrieving and summarizing fraud patterns from Neo4j.
    """
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_circular_transfers(self):
        """
        Retrieves circular money laundering patterns (A -> B -> C -> A).
        """
        query = """
        MATCH path = (a:Account)-[:TRANSFERRED_TO*3..5]->(a)
        RETURN a.account_id AS start_node, length(path) AS hops, 
               [n IN nodes(path) | n.account_id] AS path_accounts
        LIMIT 5
        """
        with self.driver.session() as session:
            return list(session.run(query))

    def get_high_value_alerts(self):
        """
        Identifies high-value transactions that exceed the standard threshold.
        """
        query = """
        MATCH (s:Account)-[r:TRANSFERRED_TO]->(d:Account)
        WHERE r.amount > 400000
        RETURN s.account_id AS sender, d.account_id AS receiver, 
               r.amount AS amount, r.timestamp AS time
        ORDER BY r.amount DESC
        LIMIT 10
        """
        with self.driver.session() as session:
            return list(session.run(query))

    def render_report(self):
        """
        Prints a clean, formatted terminal-based report for AML officers.
        """
        print("-" * 80)
        print(f"AML GRAPH ENGINE - TRANSACTION MONITORING REPORT")
        print(f"REPORT GENERATED AT: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print("-" * 80)

        # Section 1: Circular Patterns
        print("\n[DETECTED CIRCULAR PATTERNS]")
        circles = self.get_circular_transfers()
        if not circles:
            print("No circular patterns detected in current window.")
        for idx, row in enumerate(circles, 1):
            print(f"{idx}. Loop detected starting at: {row['start_node']} ({row['hops']} hops)")
            print(f"   Path: {' -> '.join(row['path_accounts'])}")

        # Section 2: High Value Transactions
        print("\n[HIGH-VALUE TRANSACTION ALERTS (> 400,000 THB)]")
        alerts = self.get_high_value_alerts()
        if not alerts:
            print("No high-value anomalies detected.")
        for idx, row in enumerate(alerts, 1):
            print(f"{idx}. AMOUNT: {row['amount']:,.2f} THB | FROM: {row['sender']} | TO: {row['receiver']}")
            print(f"   TIMESTAMP: {row['time']}")
        
        print("-" * 80)

if __name__ == "__main__":
    dashboard = AMLDashboard(NEO4J_URI, NEO4J_USER, NEO4J_PASS)
    try:
        dashboard.render_report()
    finally:
        dashboard.close()