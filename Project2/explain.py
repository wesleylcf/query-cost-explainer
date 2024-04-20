
import logging
import json
from collections import defaultdict

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REFRESH_STATS_QUERY = """
DO $$
DECLARE
    table_name TEXT;
    tables_to_analyze TEXT[] := ARRAY['lineitem', 'orders','customer','partsupp','supplier','part','nation','region'];
BEGIN
    FOREACH table_name IN ARRAY tables_to_analyze
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

    def __init__(self, conn, debug=False):
        self.conn = conn
        # self.run(REFRESH_STATS_QUERY)
        self.debug = debug

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
            logging.warning(f"No rows fetched for query {query}; Returning []")
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
        estimated_cost, explanation = self.cost_estimator.estimate(node)
        
        node['explanation'] = explanation
        node['estimated_cost'] = estimated_cost
 
        # Add cost of children
        if 'Plans' in node:
            for child in node['Plans']:
                child_node = self.analyze_node(child)
                # Special handling for 'Limit' nodes, as they do not add cost to their children
                if node['Node Type'] == 'Limit':
                    continue
                else:
                    # Regular behavior of non-LIMIT nodes: add child costs
                    node['estimated_cost'] += child_node['Total Cost']
        
        node['estimated_cost'] = round(node['estimated_cost'], 2)

        error_margin = 0.1
        if node['Node Type'] == 'Nested Loop':
            error_margin = 0.2
        if self.debug and abs(node['estimated_cost'] - node['Total Cost'])/node['Total Cost'] > error_margin:
            print(node)
            raise Exception(f"Estimated cost({node['estimated_cost']}) differs from Actual cost({node['Total Cost']}) significantly")
        
        return node


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
        self.MATERIALIZED_CONSECUTIVE_ACCESS_COST = 0.0125 # too complex; https://postgrespro.com/blog/pgsql/5969618

    def estimate(self, node):
        operator = node['Node Type']
        if operator == 'Seq Scan':
            return self.scan_cost_function(node)
        if operator == 'Index Only Scan':
            return self.index_only_scan_cost_function(node)
        if operator == 'Materialize':
            return self.materialize_cost_function(node)
        if operator == 'Nested Loop':
            return self.nested_loop_cost_function(node)
        if operator == 'Merge Join':
            return self.merge_join_function_cost_function(node)
        if operator == 'Hash Join':
            return self.hash_join_cost_function(node)
        if operator == 'Unique':        
            return self.unique_cost_function(node)
        if operator == 'Sort':
            return self.sort_cost_function(node)
        if operator == 'Aggregate':
            return self.aggregate_cost_function(node)
        if operator == 'Gather':
            return self.gather_cost_function(node)
        if operator == 'Limit':
            return self.limit_cost_function(node)
        if operator == 'Gather Merge':
            return self.gather_merge_cost_function(node)
        else:
            raise Exception(f"Cost function is undefined for operator {operator}")

    def nested_loop_cost_function(self, node):
        """
            Assume that they define child plans as a stack(last one executed first)
        """
        materialize_node = scan_node = None
        current_rows = node['Plan Rows']

        for child in node['Plans']:
            if child['Node Type'] == 'Materialize':
                materialize_node = child
            else:
                scan_node = child
        
        scan_rows, scan_cost = scan_node['Plan Rows'], scan_node['Total Cost']
        explanation_array = [f"Explanation for {node['Node Type']}"]

        consecutive_materialize_access_cost = (scan_rows-1) * self.MATERIALIZED_CONSECUTIVE_ACCESS_COST
        materialize_cost = materialize_node['Total Cost'] + consecutive_materialize_access_cost
        explanation_array.append(f"consecutive_materialize_access_cost = scans_row-1({scan_rows-1}) * MATERIALIZED_CONSECUTIVE_ACCESS_COST(0.0125)")
        explanation_array.append(f"materialize_cost = initial_materialized_access_cost({materialize_node['Node Type']}) + consecutive_materialize_access_cost({consecutive_materialize_access_cost}) = {materialize_cost}")

        explanation_array.append(f"scan_cost = {scan_cost}")

        output_rows_cost = current_rows * self.properties['cpu_tuple_cost']
        explanation_array.append(f"output_rows_cost = output rows({current_rows}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']}) = {output_rows_cost}")

        total_cost = materialize_cost + scan_cost + output_rows_cost
        explanation_array.append(f"total_cost = materialize_cost({materialize_cost}) + scan_cost({scan_cost}) + output_rows_cost({output_rows_cost}) = {total_cost}")
        
        return [output_rows_cost, '\n'.join(explanation_array)]

    def materialize_cost_function(self, node):
        rows = node['Plan Rows']
        cpu_operator_cost = self.properties['cpu_operator_cost']
        total_cost = 2 * cpu_operator_cost * rows
        return [total_cost, f"Materialize cost = 2 * cpu_operator_cost({cpu_operator_cost}) * tuples({rows}) = {total_cost}"]

    def scan_cost_function(self, node):
        rows, table_props = node['Plan Rows'], self.properties[node['Relation Name']]
        seq_pages_accessed = table_props['pages']
        total_cost = (seq_pages_accessed * self.properties['seq_page_cost']) + (rows * self.properties['cpu_tuple_cost'])
        explanation = f"Total cost = seq_pages_accessed({seq_pages_accessed}) * seq_page_cost({self.properties['seq_page_cost']}) + rows({rows}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']}) = {total_cost}"
        return [total_cost, explanation]

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
        explanation_array = ["Formula: total_cost = left_cost + right_cost + sort_cost"]
        # left_rows, right_rows = node['Plan Rows'], node['Plan Rows']
        left_props, right_props = self.properties[node['Relation Name']], self.properties[node['Relation Name']]
        left_pages, right_pages = left_props['pages'], right_props['pages']
        left_tups, right_tups = left_props['tuples'], right_props['tuples']
        left_cost = (left_pages * self.properties['seq_page_cost']) + (left_tups * self.properties['cpu_tuple_cost'])
        right_cost = (right_pages * self.properties['seq_page_cost']) + (right_tups * self.properties['cpu_tuple_cost'])
        explanation_array.append(f"left_cost = (left_pages({left_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (left_tups({left_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {left_cost}")
        explanation_array.append(f"right_cost = (right_pages({right_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (right_tups({right_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {right_cost}")
        sort_cost = (left_tups + right_tups) * math.log(left_tups + right_tups) * self.properties['cpu_operator_cost']
        explanation_array.append(f"sort_cost = (left_tups({left_tups}) + right_tups({right_tups})) * log(left_tups({left_tups}) + right_tups({right_tups})) * cpu_operator_cost({self.properties['cpu_operator_cost']}) = {sort_cost}")
        estimated_total_cost = left_cost + right_cost + sort_cost
        explanation_array.append(f"Therefore total cost = left_cost({left_cost}) + right_cost({right_cost}) + sort_cost({sort_cost}) = {estimated_total_cost}")
        explanation = '\n'.join(explanation_array)
        return [estimated_total_cost, explanation]
    
    def hash_join_cost_function(self, node):
        explanation_array = ["Formula: total_cost = left_cost + right_cost + hash_cost"]
        # left_rows, right_rows = node['Plan Rows'], node['Plan Rows']
        left_props, right_props = self.properties[node['Relation Name']], self.properties[node['Relation Name']]
        left_pages, right_pages = left_props['pages'], right_props['pages']
        left_tups, right_tups = left_props['tuples'], right_props['tuples']
        left_cost = (left_pages * self.properties['seq_page_cost']) + (left_tups * self.properties['cpu_tuple_cost'])
        right_cost = (right_pages * self.properties['seq_page_cost']) + (right_tups * self.properties['cpu_tuple_cost'])
        explanation_array.append(f"left_cost = (left_pages({left_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (left_tups({left_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {left_cost}")
        explanation_array.append(f"right_cost = (right_pages({right_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (right_tups({right_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {right_cost}")
        smaller_tups = min(left_tups, right_tups)
        hash_cost = smaller_tups * self.properties['cpu_operator_cost']
        explanation_array.append(f"hash_cost = smaller_tups({smaller_tups}) * cpu_operator_cost({self.properties['cpu_operator_cost']}) = {hash_cost}")
        estimated_total_cost = left_cost + right_cost + hash_cost
        explanation_array.append(f"Therefore total cost = left_cost({left_cost}) + right_cost({right_cost}) + hash_cost({hash_cost}) = {estimated_total_cost}")
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
    
    def sort_cost_function(self, node):
        input_rows = node['Plan Rows']
        cpu_operator_cost = self.properties['cpu_operator_cost']
        disk_cost_per_page = self.properties['seq_page_cost']
        pages = node['Plan Rows'] / node['Plan Width']

        sort_cost = input_rows * (cpu_operator_cost + (disk_cost_per_page * pages))
        total_cost = round(sort_cost + node['Total Cost'], 2)

        explanation = f"Total cost = sort_cost({round(sort_cost, 2)}) + child_cost({round(node['Total Cost'], 2)}) = {total_cost}"
        return [total_cost, explanation]\


    def aggregate_cost_function(self, node):
        cpu_operator_cost = self.properties['cpu_operator_cost']
        cpu_tuple_cost = self.properties['cpu_tuple_cost']

        child_node = node['Plans'][0]
        rows_processed = child_node['Plan Rows']
        child_cost = child_node['Total Cost']

        estimated_rows_returned = 1  # Sum, Count etc. only return 1 row
        total_cost = (rows_processed * cpu_operator_cost) + (estimated_rows_returned * cpu_tuple_cost)
        total_cost_with_child = total_cost + child_cost

        explanation = []
        explanation.append("Formula: Total cost = (rows_processed * cpu_operator_cost) + (estimated_rows_returned * cpu_tuple_cost)")
        explanation.append(f"Total Cost = (rows_processed({rows_processed}) * cpu_operator_cost({cpu_operator_cost})) + (estimated_rows_returned({estimated_rows_returned}) * cpu_tuple_cost({cpu_tuple_cost})) = {total_cost}")
        explanation.append(f"Total Cost with child (estimated_cost) = Total Cost({total_cost}) + child_cost({child_cost}) = {total_cost_with_child}")
        # explanation.append("Note: The actual cost may vary due to parallel execution efficiencies and other runtime factors.")

        return [total_cost, explanation]

    def gather_cost_function(self, node):
        # See documentation: https://www.postgresql.org/docs/current/how-parallel-query-works.html

        base_gather_cost = 1000  # Fixed base cost
        cost_per_worker = 0.1  # Fixed cost per worker observed empirically
        number_of_workers = node.get('Workers Launched', 1)

        child_node = node['Plans'][0]
        child_cost = child_node['Total Cost']

        # Adding a minor variable cost based on the number of workers
        worker_cost = cost_per_worker * number_of_workers

        total_cost = base_gather_cost + worker_cost
        total_cost_with_child = total_cost + child_cost

        explanation = []
        explanation.append("Formula: Total cost = base_gather_cost + (cost_per_worker * number_of_workers)")
        explanation.append(f"Total Cost = base_gather_cost({base_gather_cost}) + (cost_per_worker({cost_per_worker}) * number_of_workers({number_of_workers})) = {total_cost}")
        explanation.append(f"Total Cost with child (estimated_cost) = Total Cost({total_cost}) + child_cost({child_cost}) = {total_cost_with_child}")
        explanation.append("Note:")
        explanation.append("The base gather cost represents the fixed overhead for parallel query setup and coordination.")
        explanation.append("The cost per worker represents the additional cost for each worker process involved in the parallel execution based on empirical data.")

        return [total_cost, explanation]

    def limit_cost_function(self, node):
        # See StackOverflow: https://stackoverflow.com/questions/75522000/why-postgresql-explain-cost-is-low-in-limit-and-result-phase-but-high-in-index-s

        child_node = node['Plans'][0]
        child_cost = child_node['Total Cost']
        child_rows = child_node['Plan Rows']  # Total rows the child node would process without limit
        limit_rows = node['Plan Rows']  # Number of rows after applying LIMIT

        # Assuming the limit_rows are less than the child_rows, we adjust the cost proportionally
        if limit_rows < child_rows and child_rows > 0:
            cost_reduction_factor = limit_rows / child_rows
        else:
            cost_reduction_factor = 1  # No reduction if limit is equal to potential rows

        # Minimum scale factor to avoid overestimation (too complex to calculate)
        min_scale_factor = 0.2
        if(cost_reduction_factor != 1):
            cost_reduction_factor = max(cost_reduction_factor, min_scale_factor)

        # Adjusted cost considers the early termination of the scan or operation
        total_cost = child_cost * cost_reduction_factor

        explanation = []
        explanation.append("Formula: Total Cost = child_cost * cost_reduction_factor")
        explanation.append(f"cost_reduction_factor = limit_rows({limit_rows}) / child_rows({child_rows}) = {cost_reduction_factor}")
        explanation.append(f"Total Cost = child_cost({child_cost}) * cost_reduction_factor({cost_reduction_factor}) = {total_cost}")
        explanation.append("Note:")
        explanation.append("The cost reduction factor accounts for the early termination of the operation due to the LIMIT clause.")
        explanation.append("When dealing with non-scan children like Gather Merge node, the standard formula for cost_reduction_factor might not always make sense.")
        explanation.append("=> This is because the non-scan child node may perform complex operations like parallel merging.")
        explanation.append("=> Hence, a minimum scale factor of 0.2 is applied to avoid overestimation in such cases.")
        explanation.append("=> This formula is a simplified version and may not cover all edge cases accurately, however, it is accurate for scan type children nodes.")

        return [total_cost, explanation]

