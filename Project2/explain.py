import logging
import json

# Set up logging configuration for consistency with project.py
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run_explain(query, conn):
    """
    Executes the EXPLAIN command on a given SQL query using a PostgreSQL connection
    and returns the JSON-formatted plan.

    See 'EXPLAIN' documentation:
    https://www.postgresql.org/docs/current/sql-explain.html

    See 'JSON Format Explain Plan' section in:
    https://www.postgresonline.com/journal/archives/171-Explain-Plans-PostgreSQL-9.0-Text,-JSON,-XML,-YAML-Part-1-You-Choose.html

    See psycopg2 documentation:
    https://www.psycopg.org/docs/cursor.html#cursor.execute

    Parameters:
    query (str): SQL query to be explained.
    conn (psycopg2.connection): Active database connection object.

    Returns:
    list: A list of dictionaries representing the JSON formatted execution plan returned by PostgreSQL.
    """
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN (ANALYZE true, FORMAT json) {query}")
        explain_output = cur.fetchone()[0]
        logging.info("EXPLAIN command executed successfully.")
        # psycopg2 implicitly converts the JSON output to a list of dictionaries (python)
        return explain_output


def analyze_execution_plan(explain_output):
    """
    Recursively analyzes an execution plan node and all sub-nodes to extract useful metrics.

    Parameters:
    explain_output (dict): A list containing JSON execution plans from PostgreSQL.

    Returns:
    dict: A dictionary containing extracted metrics and potentially nested sub-plan metrics.
    """
    def analyze_node(node):
        # Extract relevant metrics from the node
        node_analysis = {
            'Node Type': node.get('Node Type'),
            'Total Cost': node.get('Total Cost', 0),
            'Plan Rows': node.get('Plan Rows', 0),
            'Actual Rows': node.get('Actual Rows', 0),
            'Actual Loops': node.get('Actual Loops', 1),
        }

        # If there are sub-plans, analyze them recursively
        if 'Plans' in node:
            node_analysis['Sub-plans'] = [analyze_node(sub_node) for sub_node in node['Plans']]

        return node_analysis

    # Start the analysis from the top-level node
    if explain_output:
        # Ensure there's at least one plan to analyze
        return analyze_node(explain_output[0]['Plan'])
    else:
        logging.error("No execution plan found.")
        return {}


def generate_report(analysis_results):
    """
    Generates a formatted JSON report from the analysis results.

    Parameters:
    analysis_results (dict): Analysis results of the execution plan.

    Returns:
    str: A string representation of the JSON-formatted analysis report.
    """
    report = json.dumps(analysis_results, indent=4)
    logging.info("Report generated.")
    return report
