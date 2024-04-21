
import logging
import json
import math
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
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"EXPLAIN (ANALYZE true, FORMAT json) {query}")
                explain_output = cur.fetchone()[0]
                logging.info("EXPLAIN command executed successfully.")
                # psycopg2 implicitly converts the JSON output to a list of dictionaries (python)
                return explain_output
        except Exception as e:
            logging.error("An error occurred while executing the EXPLAIN command", e)
            # Rollback any ongoing transactions
            self.conn.rollback()
            raise Exception("An error occurred while executing the query. Please check your input.")

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
        if operator == 'Index Scan':
            return self.index_scan_cost(node)
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
        if operator == 'Hash':
            return self.hash_cost_function(node)
        # else:
        #     raise Exception(f"Cost function is undefined for operator {operator}")
        return [0, [f"Cost function is not implemented for operator: {operator}"]]


    def nested_loop_cost_function(self, node):
        materialize_node = scan_node = None
        current_rows = node['Plan Rows']

        for child in node['Plans']:
            if child['Node Type'] == 'Materialize':
                materialize_node = child
            else:
                scan_node = child
        
        scan_rows, scan_cost = scan_node['Plan Rows'], scan_node['Total Cost']
        # explanation_array = [f"Explanation for {node['Node Type']}"]
        consecutive_materialize_access_cost = (scan_rows-1) * self.MATERIALIZED_CONSECUTIVE_ACCESS_COST
        if materialize_node:
            materialize_cost = materialize_node['Total Cost'] + consecutive_materialize_access_cost
            materialize_cost_str = f"Materialize Cost: Materialization Cost + Total Consecutive Materialize Access Cost({consecutive_materialize_access_cost}) = {round(materialize_cost, 2)}"
        else:
            materialize_cost = 0
            materialize_cost_str = "No Materialize Node"
        # explanation_array.append(f"consecutive_materialize_access_cost = scans_row-1({scan_rows-1}) * MATERIALIZED_CONSECUTIVE_ACCESS_COST(0.0125)")
        # explanation_array.append(f"materialize_cost = initial_materialized_access_cost({materialize_node['Node Type']}) + consecutive_materialize_access_cost({consecutive_materialize_access_cost}) = {materialize_cost}")

        # explanation_array.append(f"scan_cost = {scan_cost}")

        output_rows_cost = current_rows * self.properties['cpu_tuple_cost']
        # explanation_array.append(f"output_rows_cost = output rows({current_rows}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']}) = {output_rows_cost}")

        total_cost = materialize_cost + scan_cost + output_rows_cost
        # explanation_array.append(f"total_cost = materialize_cost({materialize_cost}) + scan_cost({scan_cost}) + output_rows_cost({output_rows_cost}) = {total_cost}")
        
        explanation_array = [
            f"Total Consecutive Materialize Access Cost = (Scanned rows({scan_rows}) - 1) * Consecutive Materialize Access Cost({self.MATERIALIZED_CONSECUTIVE_ACCESS_COST}) = {round(consecutive_materialize_access_cost, 2)}",
            materialize_cost_str,
            f"Scan Cost = {round(scan_cost, 2)}",
            f"Output Rows Cost = {round(current_rows, 2)} * {round(self.properties['cpu_tuple_cost'], 2)} = {round(output_rows_cost, 2)}",
            f"Total Cost: Materialize Cost({round(materialize_cost, 2)}) + Scan Cost({round(scan_cost, 2)}) + Output Rows Cost({output_rows_cost}) = {round(total_cost, 2)}"
            f"Note: The estimated cost will differ significantly because the cost of the initial materialize access is included in the plan, but data for further consecutive calls isn't included, and its not feasible to calculate it with just the statistics of the relations."
        ]

        return self.toResponse(output_rows_cost, explanation_array)

    def materialize_cost_function(self, node):
        rows = node['Plan Rows']
        cpu_operator_cost = self.properties['cpu_operator_cost']
        children_cost = self.getChildrenCost(node)
        cost = 2 * cpu_operator_cost * rows
        explanation_array = [f"Materialize Cost = 2 * CPU Operator Cost({cpu_operator_cost}) * Tuples({rows}) = {round(cost, 2)}"]
        explanation_array.append(f"Total Cost = Cost of children({children_cost}) + Materialize Cost({cost}) = {round(cost + children_cost, 2)}")
        return self.toResponse(cost, explanation_array)

    def scan_cost_function(self, node):
        rows, table_props = node['Plan Rows'], self.properties[node['Relation Name']]
        seq_pages_accessed = table_props['pages']
        total_cost = (seq_pages_accessed * self.properties['seq_page_cost']) + (rows * self.properties['cpu_tuple_cost'])
        explanation_array = [f"Total cost = Sequential Pages Accessed({seq_pages_accessed}) * Sequential Page Cost({self.properties['seq_page_cost']}) + rows({rows}) * CPU Tuple Cost({self.properties['cpu_tuple_cost']}) = {round(total_cost,2)}"]
        return self.toResponse(total_cost, explanation_array)

    def index_scan_cost(self, node):
        total_cost = node['Total Cost']
        scan_cost = node['Total Cost']
        output_rows = node['Plan Rows']
        explanation_array = [
            f"Scan Cost: {round(scan_cost, 2)}",
            f"Output Rows Cost: {round(output_rows, 2)} * {round(self.properties['cpu_tuple_cost'], 2)} = {round(output_rows * self.properties['cpu_tuple_cost'], 2)}",
            f"Total Cost: {round(total_cost, 2)}"
        ]
        return self.toResponse(total_cost, explanation_array)

    def index_only_scan_cost_function(self, node) -> float:
        explanation_array = ["Formula: Total Cost = Index Access Cost + Table Pages Fetch Cost"]
        relation_name, rows = node['Relation Name'], node['Plan Rows']
        relation_pages, relation_tuples =  self.properties[relation_name]['pages'], self.properties[relation_name]['tuples']

        index_selectivity = rows/relation_tuples
        explanation_array.append("Calculation for Index Access Cost:")
        explanation_array.append(f"Index Selectivity = estimated_rows({rows}) / Total Rows({relation_tuples}) = {round(index_selectivity, 2)}")

        estimated_pages, estimated_tuples = relation_pages * index_selectivity, relation_tuples * index_selectivity
        explanation_array.append(f"Estimated Pages = Index Selectivity({index_selectivity}) * Total Pages({relation_pages}) = {round(estimated_pages, 2)}")
        explanation_array.append(f"Estimated Tuples = Index Selectivity({index_selectivity}) * Total Tuples({relation_tuples}) = {round(estimated_tuples, 2)}")

        estimated_index_pages = self.properties[node['Index Name']]['pages']
        random_page_cost, cpu_index_tuple_cost, cpu_operator_cost, cpu_tuple_cost, seq_page_cost = self.properties['random_page_cost'], self.properties['cpu_index_tuple_cost'], self.properties['cpu_operator_cost'], self.properties['cpu_tuple_cost'], self.properties['seq_page_cost']
        explanation_array.append(f"From DB: Random Page Cost={random_page_cost} CPU Index Tuple Cost={cpu_index_tuple_cost} CPU Operator Cost={cpu_operator_cost} CPU Tuple Cost={cpu_tuple_cost} Sequential Page Cost={seq_page_cost}")
        estimated_index_cost = estimated_index_pages * random_page_cost + estimated_tuples * (cpu_index_tuple_cost + cpu_operator_cost)
        explanation_array.append(f"Index Access Cost = Estimated Pages({estimated_pages}) * Random Page Cost({random_page_cost}) + Estimated Tuples({estimated_tuples}) * (CPU Index Tuple Cost({cpu_index_tuple_cost}) + CPU Operator Cost({cpu_operator_cost})) = {round(estimated_index_cost,2)}")
        
        explanation_array.append("Calculation for Table Pages Fetch Cost:")
        frac_visible = self.properties[relation_name]['visible_pages']/relation_pages
        explanation_array.append(f"Fraction Of Visible Pages = relallvisible({self.properties[relation_name]['visible_pages']}) / Total Pages({relation_pages}) = {round(frac_visible)}")
        estimated_table_cost = (1-frac_visible) * estimated_pages * seq_page_cost + estimated_tuples * cpu_tuple_cost
        explanation_array.append(f"Table Pages Fetch Cost = (1-frac_visible={frac_visible}) * Estimated Pages({estimated_pages}) * Sequential Page Cost({seq_page_cost}) + Estimated Tuples({estimated_tuples}) * CPU Tuple Cost({cpu_tuple_cost})")

        estimated_total_cost = estimated_index_cost + estimated_table_cost
        explanation_array.append(f"Therefore Total Cost = Index Access Cost({estimated_index_cost}) + Table Pages Fetch Cost({estimated_table_cost}) = {round(estimated_total_cost,2)}")

        explanation_array.append(f"Note: The estimated cost will differ significantly because any changes still present in the heap that haven't been vacuumed increase the total plan cost")
        return self.toResponse(estimated_total_cost, explanation_array)
    
    def merge_join_function_cost_function(self, node):
        explanation_array = ["Formula: total_cost = left_cost + right_cost + sort_cost"]
        # left_rows, right_rows = node['Plan Rows'], node['Plan Rows']
        # left_props, right_props = self.properties[node['Relation Name']], self.properties[node['Relation Name']]
    
        left_props, right_props = None, None
        for child in node['Plans']:
            if child['Node Type'] == 'Sort':
                for sub_child in child['Plans']:
                    if 'Relation Name' in sub_child:
                        left_props = self.properties[sub_child['Relation Name']]
                        break
            elif 'Relation Name' in child:
                right_props = self.properties[child['Relation Name']]
        if left_props and right_props:
            left_pages, right_pages = round(left_props['pages'],2), round(right_props['pages'],2)
            left_tups, right_tups = round(left_props['tuples'],2), round(right_props['tuples'],2)
            left_cost = round((left_pages * self.properties['seq_page_cost']) + (left_tups * self.properties['cpu_tuple_cost']),2)
            right_cost = round((right_pages * self.properties['seq_page_cost']) + (right_tups * self.properties['cpu_tuple_cost']),2)
            explanation_array.append(f"left_cost = (left_pages({left_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (left_tups({left_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {left_cost}")
            explanation_array.append(f"right_cost = (right_pages({right_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (right_tups({right_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {right_cost}")
            sort_cost = round((left_tups + right_tups) * math.log(left_tups + right_tups) * self.properties['cpu_operator_cost'],2)
            explanation_array.append(f"sort_cost = (left_tups({left_tups}) + right_tups({right_tups})) * log(left_tups({left_tups}) + right_tups({right_tups})) * cpu_operator_cost({self.properties['cpu_operator_cost']}) = {sort_cost}")
            estimated_total_cost = round(left_cost + right_cost + sort_cost,2)
            explanation_array.append(f"Therefore total cost = left_cost({left_cost}) + right_cost({right_cost}) + sort_cost({sort_cost}) = {estimated_total_cost}")
            return self.toResponse(round(estimated_total_cost,2), explanation_array)
        else:
            return self.toResponse(0, ["Error: Left or right properties not found."])
    
    # def hash_join_cost_function(self, node):
    #     print("Node:", node)
    #     explanation_array = ["Formula: total_cost = left_cost + right_cost + hash_cost"]
    #     # left_rows, right_rows = node['Plan Rows'], node['Plan Rows']
    #     left_props, right_props = self.properties[node['Relation Name']], self.properties[node['Relation Name']]

    #     left_props = node['Plans'][0]['Relation Name']
    #     right_props = node['Plans'][1]['Relation Name']
    #     left_pages, right_pages = left_props['pages'], right_props['pages']
    #     left_tups, right_tups = left_props['tuples'], right_props['tuples']
    #     left_cost = (left_pages * self.properties['seq_page_cost']) + (left_tups * self.properties['cpu_tuple_cost'])
    #     right_cost = (right_pages * self.properties['seq_page_cost']) + (right_tups * self.properties['cpu_tuple_cost'])
    #     explanation_array.append(f"left_cost = (left_pages({left_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (left_tups({left_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {left_cost}")
    #     explanation_array.append(f"right_cost = (right_pages({right_pages}) * seq_page_cost({self.properties['seq_page_cost']})) + (right_tups({right_tups}) * cpu_tuple_cost({self.properties['cpu_tuple_cost']})) = {right_cost}")
    #     smaller_tups = min(left_tups, right_tups)
    #     hash_cost = smaller_tups * self.properties['cpu_operator_cost']
    #     explanation_array.append(f"hash_cost = smaller_tups({smaller_tups}) * cpu_operator_cost({self.properties['cpu_operator_cost']}) = {hash_cost}")
    #     estimated_total_cost = left_cost + right_cost + hash_cost
    #     explanation_array.append(f"Therefore total cost = left_cost({left_cost}) + right_cost({right_cost}) + hash_cost({hash_cost}) = {estimated_total_cost}")
    #     explanation = '\n'.join(explanation_array)
    #     return [estimated_total_cost, explanation]

    def hash_cost_function(self, node):
        input_rows = node['Plan Rows']
        cpu_operator_cost = self.properties['cpu_operator_cost']
        explanation_array = ["Formula: Hash Cost = CPU Operator Cost * Input Rows"]
        hash_cost = round(cpu_operator_cost * input_rows,2)
        children_cost = self.getChildrenCost(node)

        total_cost = round(hash_cost + children_cost, 2)
        explanation_array.append(f"Hash Cost = CPU Operator Cost({cpu_operator_cost:.2f}) * Input Rows({input_rows})")
        explanation_array.append(f"Total Cost = Hash Cost({hash_cost:.2f}) + Children Cost({children_cost:.2f}) = {total_cost}")

        return self.toResponse(hash_cost, explanation_array)

    def hash_join_cost_function(self, node):
        total_hash_cost = 0
        cpu_operator_cost = self.properties['cpu_operator_cost']

        explanation_array = ["Formula: Hash Join Cost = Build Cost + Probe Cost + Hash Cost"]

        for child in node['Plans']:
            build_relation = child
            if 'Plans' in child:
                probe_relation = child['Plans'][0]
            else:
                probe_relation = child

            disk_cost_per_page = self.properties['seq_page_cost']
            build_pages = build_relation['Plan Rows'] / build_relation['Plan Width']
            probe_pages = probe_relation['Plan Rows'] / probe_relation['Plan Width']

            build_rows = build_relation['Plan Rows']
            build_size = build_rows * build_relation['Plan Width']
            probe_rows = probe_relation['Plan Rows']
            probe_size = probe_rows * probe_relation['Plan Width']

            smaller_relation = min(build_rows, probe_rows)
            
            hash_cost = smaller_relation * cpu_operator_cost
            explanation_array.append(f"Hash Table Cost = Smaller Relation({smaller_relation}) * CPU Operator Cost({cpu_operator_cost})")
            hash_join_cost = hash_cost + max(build_rows, probe_rows) * cpu_operator_cost + smaller_relation * cpu_operator_cost
            total_hash_cost += hash_join_cost
            explanation_array.append(f"Hash Join Cost = Build Rows({build_relation['Plan Rows']}) * CPU Operator Cost({cpu_operator_cost}) + Probe Rows({probe_relation['Plan Rows']}) * CPU Operator Cost({cpu_operator_cost}) + Hash Table Cost({hash_join_cost:.2f})")
            break
        
        children_cost = self.getChildrenCost(node)
        total_cost = round(total_hash_cost+children_cost,2)
        explanation_array.append(f"Total Cost = Hash Join Cost({total_hash_cost:.2f}) + Children Cost({children_cost:.2f}) = {total_cost}")

        return self.toResponse(round(total_hash_cost,2), explanation_array)

    
    def unique_cost_function(self, node):
        explanation_array = ["Formula: total_cost = child_cost"]
        child_cost = node['Total Cost']
        explanation_array.append(f"child_cost = {round(child_cost,2)}")
        estimated_total_cost = child_cost
        explanation_array.append(f"Therefore total cost = child_cost({child_cost}) = {estimated_total_cost}")
        return self.toResponse(round(estimated_total_cost,2), explanation_array)
    
    def sort_cost_function(self, node):
        input_rows = node['Plan Rows']
        cpu_operator_cost = self.properties['cpu_operator_cost']
        disk_cost_per_page = self.properties['seq_page_cost']
        pages = node['Plan Rows'] / node['Plan Width']
        explanation_array = ["Formula: 2 * CPU Operator Cost * Input Rows * log2(Input Rows) + Disk Cost per Page * Pages"]
        sort_cost = 2 * cpu_operator_cost * input_rows * math.log2(input_rows) + disk_cost_per_page * pages

        children_cost = self.getChildrenCost(node)

        total_cost = round(sort_cost+children_cost,2)
        # explanation_array.append(f"Sort Cost = 2 * CPU Operator Cost({round({cpu_operator_cost})}) * {input_rows} * log2({input_rows}) + {disk_cost_per_page} * {pages}")
        explanation_array.append(f"Sort Cost = 2 * CPU Operator Cost({cpu_operator_cost:.2f}) * Input Rows({input_rows}) * log2(Input Rows({input_rows})) + Disk Cost per Page({disk_cost_per_page:.2f}) * Pages({pages:.2f})",)
        explanation_array.append(f"Total Cost = Sort Cost({sort_cost:.2f}) + Children Cost({children_cost:.2f}) = {total_cost}")

        #  sort_cost = input_rows * (cpu_operator_cost + (disk_cost_per_page * pages))
        # total_cost = round(sort_cost + node['Total Cost'], 2)
        # explanation = f"Total Sort cost = sort_cost({round(sort_cost, 2)}) + child_cost({round(node['Total Cost'], 2)}) = {total_cost}"

        return self.toResponse(sort_cost, explanation_array)


    def aggregate_cost_function(self, node):
        cpu_operator_cost = self.properties['cpu_operator_cost']
        cpu_tuple_cost = self.properties['cpu_tuple_cost']

        child_node = node['Plans'][0]
        rows_processed = child_node['Plan Rows']
        child_cost = child_node['Total Cost']

        estimated_rows_returned = node['Plan Rows']
        total_cost = (rows_processed * cpu_operator_cost) + (estimated_rows_returned * cpu_tuple_cost)
        total_cost_with_child = total_cost + child_cost

        explanation = []
        explanation.append("Formula: Total Cost = (Rows Processed * CPU Operator Cost) + (Estimated Rows Returned * CPU Tuple Cost) + Child Cost")
        explanation.append(f"Aggregate Cost = (Rows Processed({round(rows_processed, 2)}) * CPU Operator Cost({round(cpu_operator_cost, 2)})) + (Estimated Rows Returned({round(estimated_rows_returned, 2)}) * CPU Tuple Cost({round(cpu_tuple_cost, 2)})) = {round(total_cost, 2)}")
        explanation.append(f"Total Cost = Aggregate Cost({round(total_cost, 2)}) + Child Cost({round(child_cost, 2)}) = {round(total_cost_with_child, 2)}")
        explanation.append("Note:")
        explanation.append("The estimated cost may be lesser than the actual cost due to exclusion of additional costs incurred from operations like Group By.")

        return self.toResponse(total_cost, explanation)

    def gather_cost_function(self, node):
        child_node = node['Plans'][0]
        child_total_cost = child_node['Total Cost']
        child_startup_cost = child_node['Startup Cost']

        # Constants verified in documentation
        parallel_setup_cost = 1000
        parallel_tuple_cost = 0.1

        # Initializing startup and running costs
        startup_cost = 0
        run_cost = 0

        # Adding parallel setup and communication costs
        startup_cost += parallel_setup_cost
        run_cost += parallel_tuple_cost * node['Plan Rows']

        # Final cost calculations
        total_cost = startup_cost + run_cost
        total_cost_with_child = total_cost + child_total_cost

        explanation = []
        explanation.append("Formula: Total Cost = StartUp Cost + Run Cost + Child Cost")
        explanation.append(f"StartUp Cost = Parallel Setup Cost = {round(startup_cost, 2)}")
        explanation.append(f"Run Cost = Child Total Cost({round(child_total_cost, 2)}) - Child StartUp Cost({round(child_startup_cost,2 )}) + (Parallel Tuple Cost({round(parallel_tuple_cost, 2)}) * Rows({round(node['Plan Rows'], 2)})) = {round(run_cost, 2)}")
        explanation.append(f"Gather Cost = StartUp Cost({round(startup_cost, 2)}) + Run Cost({round(run_cost, 2)}) = {round(total_cost, 2)}")
        explanation.append(f"Total Cost = Gather Cost({round(total_cost, 2)}) + Child Cost({round(child_total_cost, 2)}) = {round(total_cost_with_child, 2)}")

        return self.toResponse(total_cost, explanation)

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
        explanation.append("Formula: Total Cost = Child Cost * Cost Reduction Factor")
        explanation.append(f"Cost Reduction Factor = Limit Rows({round(limit_rows, 2)}) / Child Rows({round(child_rows, 2)}) = {round(cost_reduction_factor, 2)}")
        explanation.append(f"Total Cost = Child Cost({round(child_cost, 2)}) * Cost Reduction Factor({round(cost_reduction_factor, 2)}) = {round(total_cost, 2)}")
        explanation.append("Note:")
        explanation.append("The Cost Reduction Factor accounts for the early termination of the operation due to the LIMIT clause.")
        explanation.append("When dealing with non-scan children like Gather Merge node, the standard formula for Cost Reduction Factor might not be accurate.")
        explanation.append("=> This is because the non-scan child node may perform complex operations like parallel merging.")
        explanation.append("=> Hence, a minimum scale factor of 0.2 is applied to avoid overestimation in such cases.")
        explanation.append("=> This formula is a simplified version and may not cover all edge cases accurately, however, it is accurate for scan type children nodes.")

        return self.toResponse(total_cost, explanation)

    def gather_merge_cost_function(self, node):

        child_node = node['Plans'][0]
        child_cost = child_node['Total Cost']

        # Constants verified in documentation
        parallel_setup_cost = 1000
        parallel_tuple_cost = 0.1

        # Initializing startup and running costs
        startup_cost = 0
        run_cost = 0

        # Calculating the number of workers and logN
        num_workers = node.get('Workers Launched', 0) + 1
        logN = math.log2(num_workers)

        # Setting the comparison cost
        comparison_cost = 2.0 * self.properties['cpu_operator_cost']

        # Calculating the heap creation cost
        heap_creation_cost = comparison_cost * num_workers * logN
        startup_cost += heap_creation_cost

        # Calculating the per-tuple heap maintenance cost
        run_cost += node['Plan Rows'] * comparison_cost * logN
        run_cost += self.properties['cpu_operator_cost'] * node['Plan Rows']
        heap_maintenance_cost = run_cost

        # Adding parallel setup and communication costs
        startup_cost += parallel_setup_cost
        run_cost += parallel_tuple_cost * node['Plan Rows'] * 1.05

        # Final cost calculations
        total_startup_cost = startup_cost
        total_cost = total_startup_cost + run_cost
        total_cost_with_child = total_cost + child_cost

        explanation = []
        explanation.append("Formula: Total Cost = StartUp Cost + Run Cost + Child Cost")
        explanation.append(f"StartUp Cost = Heap Creation Cost({round(heap_creation_cost, 2)}) + Parallel SetUp Cost({round(parallel_setup_cost, 2)}) = {round(total_startup_cost, 2)}")
        explanation.append(f"Run Cost = Heap Maintenance Cost({round(heap_maintenance_cost, 2)}) + Parallel Tuple Cost({round(parallel_tuple_cost, 2)}) * Rows({round(node['Plan Rows'], 2)}) * 1.05 = {round(run_cost, 2)}")
        explanation.append(f"Gather Merge Cost = StartUp Cost({round(total_startup_cost, 2)}) + Run Cost({round(run_cost, 2)}) = {round(total_cost, 2)}")
        explanation.append(f"Total Cost = Gather Merge Cost({round(total_cost, 2)}) + Child Cost({round(child_cost, 2)}) = {round(total_cost_with_child, 2)}")

        return self.toResponse(total_cost, explanation)

    def getChildrenCost(self, node):
        if 'Plans' not in node:
            return 0

        return sum([child['Total Cost'] for child in node['Plans']])
    
    def toResponse(self, cost, explanation_array) -> [float, str]:
        explanation_array = [f"{index+1}. {line}" for index, line in enumerate(explanation_array)]
        return [round(cost,2), '\n'.join(explanation_array)]
        