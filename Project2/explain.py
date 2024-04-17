import logging
import json

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run_explain(query, conn):
    """
    Executes the EXPLAIN command on a given SQL query using a PostgreSQL connection
    and returns the JSON-formatted plan.

    See 'EXPLAIN' documentation:
    https://www.postgresql.org/docs/current/sql-explain.html

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


def analyze_node(node):
    """
    Analyze a single node within the execution plan, extracting both estimated
    and computed cost metrics, and recursively processing any sub-plans.

    Parameters:
    node (dict): A single node from the JSON execution plan.

    Returns:
    dict: Node and sub-nodes analysis including both estimated and computed costs.
    """
    # Extract estimated cost metrics provided by PostgreSQL
    estimated_cost = extract_cost_metrics(node)
    # Compute custom cost metrics based on what we've learned in the lectures
    computed_cost = compute_node_cost(node)

    # Create a dictionary for this node's analysis that includes both sets of cost metrics
    node_analysis = {
        'Node Type': node.get('Node Type'),
        'Relation Name': node.get('Relation Name', 'N/A'),
        'Cost Analysis': {
            'Estimated Costs': estimated_cost,
            'Computed Costs': computed_cost
        }
    }

    # Recursively analyze any sub-plans and include their analysis
    if 'Plans' in node:
        sub_plans = [analyze_node(sub_node) for sub_node in node['Plans']]
        node_analysis['Sub-plans'] = sub_plans

    return node_analysis


def analyze_execution_plan(explain_output):
    """
    Initiates the recursive analysis of the entire execution plan from the top-level node.

    Parameters:
    explain_output (list): The JSON execution plan as a list from PostgreSQL.

    Returns:
    dict: A dictionary representing the analyzed execution plan including nested sub-plans.
    """
    if explain_output:
        # The execution plan is enclosed in a list -> start with the first item
        return analyze_node(explain_output[0]['Plan'])
    else:
        logging.error("No execution plan found.")
        return {}


def extract_cost_metrics(node):
    """
    Utility function to extract cost-related metrics from a node in the execution plan.
    These metrics are cost estimates calculated by the PostgreSQL planner.

    See 'JSON Format Explain Plan' section in:
    https://www.postgresonline.com/journal/archives/171-Explain-Plans-PostgreSQL-9.0-Text,-JSON,-XML,-YAML-Part-1-You-Choose.html

    Parameters:
    node (dict): Node of the execution plan.

    Returns:
    dict: Extracted cost metrics.
    """
    cost_metrics = {
        'Node Type': node.get('Node Type'),
        'Startup Cost': node.get('Startup Cost', 0.0),  # might not be useful for us
        'Total Cost': node.get('Total Cost', 0.0),
        'Plan Rows': node.get('Plan Rows', 0),
        'Plan Width': node.get('Plan Width', 0),
        'Actual Startup Time': node.get('Actual Startup Time', 0.0),  # might not be useful for us
        'Actual Total Time': node.get('Actual Total Time', 0.0),  # might not be useful for us
        'Actual Rows': node.get('Actual Rows', 0),
        'Actual Loops': node.get('Actual Loops', 1),
    }

    return cost_metrics


# TODO: Compute custom cost metrics based on what we've learned in the lectures
def compute_node_cost(node):
    """
    Utility function to compute custom cost metrics of a node in the execution plan.

    Parameters:
    node (dict): Node of the execution plan.

    Returns:
    dict: The computed costs and other relevant metrics for the node.
    """
    # Temporary, to be replaced with actual computation
    computed_cost = {
        'IO Cost': 2,
        'Cardinality of output (no. of rows)': 20,
    }

    return computed_cost

# TODO: Function to explain the computation of various cost in the QEP, explaining differences if any

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
