
import logging
import json
from collections import defaultdict

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REFRESH_STATS_QUERY = """
DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOR table_name IN
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'public' -- Specify the schema if needed
    LOOP
        EXECUTE format('ANALYZE %I', table_name);
    END LOOP;
END $$;
"""

RELATION_PROPERTIES_QUERY = """
SELECT relname, reltuples, relpages 
FROM pg_class 
WHERE relkind IN ('r');
"""

SETTINGS_QUERY = """
SELECT relname, current_setting('random_page_cost')::real,current_setting('cpu_index_tuple_cost')::real, current_setting('cpu_operator_cost')::real, current_setting('cpu_tuple_cost')::real, current_setting('seq_page_cost')::real, relpages AS pages, reltuples AS tuples, relallvisible as visible_pages
from pg_class;
"""

class Explainer:
    tableSet = {'lineitem', 'orders','customer','partsupp','supplier','part','nation','region'}
    properties = defaultdict(lambda: {})

    def __init__(self, conn):
        self.conn = conn
        self.run(REFRESH_STATS_QUERY)

        result = self.run(SETTINGS_QUERY)
        for relname, random_page_cost, cpu_index_tuple_cost, cpu_operator_cost, cpu_tuple_cost, seq_page_cost, pages, tuples, visible_pages in result:
            if relname.split('_')[0] in self.tableSet:
                self.properties[relname]['pages'] = pages
                self.properties[relname]['visible_pages'] = visible_pages
                self.properties[relname]['tuples'] = tuples
                self.properties['random_page_cost'] = random_page_cost
                self.properties['cpu_index_tuple_cost'] = cpu_index_tuple_cost
                self.properties['cpu_operator_cost'] = cpu_operator_cost
                self.properties['cpu_tuple_cost'] = cpu_tuple_cost
                self.properties['seq_page_cost'] = seq_page_cost

        self.cost_estimator = CostEstimator(self.properties)

    def run(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        try:
            return cur.fetchall()
        except:
            logging.warning("No rows fetched; Returning []")
            return []

    def run_explain(self, query):
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
        with self.conn.cursor() as cur:
            # cur.execute(f"EXPLAIN (ANALYZE true, BUFFERS true, FORMAT json) {query}")
            cur.execute(f"EXPLAIN (ANALYZE true, FORMAT json) {query}")
            explain_output = cur.fetchone()[0]
            logging.info("EXPLAIN command executed successfully.")
            # psycopg2 implicitly converts the JSON output to a list of dictionaries (python)
            return explain_output

    def analyze_node(self, node):
        """
        Analyze a single node within the execution plan, extracting estimated cost metrics from the PostgreSQL planner
        ,  and recursively processing any sub-plans.

        Parameters:
        node (dict): A single node from the JSON execution plan.

        Returns:
        dict: Node and sub-nodes analysis including both estimated and computed costs.
        """
        # Extract estimated cost metrics provided by PostgreSQL
        plan_properties = self.get_plan_properties(node)
        estimated_cost, explanation = self.cost_estimator.estimate(node)

        # Create a dictionary for this node's analysis that includes both sets of cost metrics
        node_analysis = {
            'Node Type': node.get('Node Type'),
            'Relation Name': node.get('Relation Name', 'N/A'),
            'Cost Analysis': {
                'Actual Estimated Cost': node.get('Total Cost'),
                'Estimated Cost': estimated_cost,
                'Explanation': explanation
            }
        }

        # Recursively analyze any sub-plans and include their analysis
        if 'Plans' in node:
            sub_plans = [self.analyze_node(sub_node) for sub_node in node['Plans']]
            node_analysis['Sub-plans'] = sub_plans

        return node_analysis


    def analyze_execution_plan(self, explain_output):
        """
        Initiates the recursive analysis of the entire execution plan from the top-level node.

        Parameters:
        explain_output (list): The JSON execution plan as a list from PostgreSQL.

        Returns:
        dict: A dictionary representing the analyzed execution plan including nested sub-plans.
        """
        if explain_output:
            # The execution plan is enclosed in a list -> start with the first item
            return self.analyze_node(explain_output[0]['Plan'])
        else:
            logging.error("No execution plan found.")
            return {}


    def get_plan_properties(self, node):
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
        props = {
            'Node Type': node.get('Node Type'),
            'Startup Cost': node.get('Startup Cost', 0.0),
            'Total Cost': node.get('Total Cost', 0.0),
            'Plan Rows': node.get('Plan Rows', 0),
            'Plan Width': node.get('Plan Width', 0),
            'Actual Startup Time': node.get('Actual Startup Time', 0.0),  # might not be useful for us
            'Actual Total Time': node.get('Actual Total Time', 0.0),  # might not be useful for us
            'Actual Rows': node.get('Actual Rows', 0),
            'Actual Loops': node.get('Actual Loops', 1),
            'Shared Hit Blocks': node.get('Shared Hit Blocks', 0),  # needed for scan formula
            'Shared Read Blocks': node.get('Shared Read Blocks', 0),
            'Shared Dirtied Blocks': node.get('Shared Dirtied Blocks', 0),
            'Shared Written Blocks': node.get('Shared Written Blocks', 0),
        }

        return props


    # TODO: Function to explain the computation of various cost in the QEP, explaining differences if any

    def generate_report(self, analysis_results):
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
        
class CostEstimator:
    def __init__(self, properties):
        self.properties = properties

    def estimate(self, node):
        operator = node['Node Type']
        if operator == 'Seq Scan':
            return self.scan_cost_function(node)
        if operator == 'Index Only Scan':
            return self.index_only_scan_cost_function(node)
        else:
            raise Exception(f"Cost function is undefined for operator {operator}")

    def scan_cost_function(self, node):
        rows, table_props = node['Plan Rows'], self.properties[node['Relation Name']]
        seq_pages_accessed = table_props['pages']
        explanation = f"Cost function: (seq_pages_accessed * {self.properties['seq_page_cost']}) + (rows * {self.properties['cpu_tuple_cost']})\nwhere seq_pages_accessed= {seq_pages_accessed} and rows= {rows}"
        return [(seq_pages_accessed * self.properties['seq_page_cost']) + (rows * self.properties['cpu_tuple_cost']), explanation]

    def index_only_scan_cost_function(self, node) -> float:
        explanation_array = ["Formula: total_cost = index_access_cost + table_pages_fetch_cost"]
        relation_name, rows = node['Relation Name'], node['Plan Rows']
        relation_pages, relation_tuples =  self.properties[relation_name]['pages'], self.properties[relation_name]['tuples']

        index_selectivity = rows/relation_tuples
        explanation_array.append("Calculation for index_access_cost:")
        explanation_array.append(f"index_selectivity = estimated_rows({rows}) / total_rows({relation_tuples}) = {index_selectivity}")

        estimated_pages, estimated_tuples = relation_pages * index_selectivity, relation_tuples * index_selectivity
        explanation_array.append(f"estimated_pages = selectivity({index_selectivity}) * total pages({relation_pages}) = {estimated_pages}")
        explanation_array.append(f"estimated_tuples = selectivity({index_selectivity}) * total tuples({relation_tuples}) = {estimated_tuples}")

        estimated_index_pages = self.properties[node['Index Name']]['pages']
        random_page_cost, cpu_index_tuple_cost, cpu_operator_cost, cpu_tuple_cost, seq_page_cost = self.properties['random_page_cost'], self.properties['cpu_index_tuple_cost'], self.properties['cpu_operator_cost'], self.properties['cpu_tuple_cost'], self.properties['seq_page_cost']
        explanation_array.append(f"From DB: random_page_cost={random_page_cost} cpu_index_tuple_cost={cpu_index_tuple_cost} cpu_operator_cost={cpu_operator_cost} cpu_tuple_cost={cpu_tuple_cost} seq_page_cost={seq_page_cost}")
        estimated_index_cost = estimated_index_pages * random_page_cost + estimated_tuples * (cpu_index_tuple_cost + cpu_operator_cost)
        explanation_array.append(f"index_access_cost = estimated_index_pages({estimated_pages}) * random_page_cost({random_page_cost}) + estimated_tuples({estimated_tuples}) * (cpu_index_tuple_cost({cpu_index_tuple_cost}) + cpu_operator_cost({cpu_operator_cost})) = {estimated_index_cost}")
        
        explanation_array.append("Calculation for index_access_cost:")
        frac_visible = self.properties[relation_name]['visible_pages']/relation_pages
        explanation_array.append(f"fraction_pages_visible = relallvisible({self.properties[relation_name]['visible_pages']}) / total_pages({relation_pages}) = {frac_visible}")
        estimated_table_cost = (1-frac_visible) * estimated_pages * seq_page_cost + estimated_tuples * cpu_tuple_cost
        explanation_array.append(f"table_pages_fetch_cost = (1-frac_visible={frac_visible}) * estimated_pages({estimated_pages}) * seq_page_cost({seq_page_cost}) + estimated_tuples({estimated_tuples}) * cpu_tuple_cost({cpu_tuple_cost})")

        estimated_total_cost = estimated_index_cost + estimated_table_cost
        explanation_array.append(f"Therefore total cost = index_access_cost({estimated_index_cost}) + table_pages_fetch_cost({estimated_table_cost}) = {estimated_total_cost}")
        explanation = '\n'.join(explanation_array)

        return [estimated_total_cost, explanation]
    
    def merge_join_function_cost_function(self, node):
        explanation_array = ["Formula: total_cost = left_cost + right_cost"]
        left_rows, right_rows = node['Plan Rows'], node['Plan Rows']
        left_props, right_props = self.properties[node['Relation Name']], self.properties[node['Relation Name']]
        left_pages, right_pages = left_props['pages'], right_props['pages']
        left_tups, right_tups = left_props['tuples'], right_props['tuples']
        left_cost = (left_pages * self.properties['seq_page_cost']) + (left_tups * self.properties['cpu_tuple_cost'])
        right_cost = (right_pages * self.properties['seq_page_cost']) + (right_tups * self.properties['cpu_tuple_cost'])
        explanation_array.append(f"left_cost = (left_pages({left_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (left_tups({left_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {left_cost}")
        explanation_array.append(f"right_cost = (right_pages({right_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (right_tups({right_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {right_cost}")
        estimated_total_cost = left_cost + right_cost
        explanation_array.append(f"Therefore total cost = left_cost({left_cost}) + right_cost({right_cost}) = {estimated_total_cost}")
        explanation = '\n'.join(explanation_array)
        return [estimated_total_cost, explanation]
    
    def hash_join_cost_function(self, node):
        explanation_array = ["Formula: total_cost = left_cost + right_cost"]
        left_rows, right_rows = node['Plan Rows'], node['Plan Rows']
        left_props, right_props = self.properties[node['Relation Name']], self.properties[node['Relation Name']]
        left_pages, right_pages = left_props['pages'], right_props['pages']
        left_tups, right_tups = left_props['tuples'], right_props['tuples']
        left_cost = (left_pages * self.properties['seq_page_cost']) + (left_tups * self.properties['cpu_tuple_cost'])
        right_cost = (right_pages * self.properties['seq_page_cost']) + (right_tups * self.properties['cpu_tuple_cost'])
        explanation_array.append(f"left_cost = (left_pages({left_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (left_tups({left_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {left_cost}")
        explanation_array.append(f"right_cost = (right_pages({right_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (right_tups({right_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {right_cost}")
        estimated_total_cost = left_cost + right_cost
        explanation_array.append(f"Therefore total cost = left_cost({left_cost}) + right_cost({right_cost}) = {estimated_total_cost}")
        explanation = '\n'.join(explanation_array)
        return [estimated_total_cost, explanation]
    
    def unique_cost_function(self, node):
        explanation_array = ["Formula: total_cost = child_cost"]
        child_cost = node['Total Cost']
        explanation_array.append(f"child_cost = {child_cost}")
        estimated_total_cost = child_cost
        explanation_array.append(f"Therefore total cost = child_cost({child_cost}) = {estimated_total_cost}")
        explanation = '\n'.join(explanation_array)
        return [estimated_total_cost, explanation]
