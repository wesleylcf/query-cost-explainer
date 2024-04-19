import psycopg2
import logging
from explain import run_explain, analyze_execution_plan, generate_report

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# This is the main entry point for the application.
# It initializes the database connection and sets up the environment to interact with the PostgreSQL database.
# It also initializes interface.py, the GUI.

conn = None


def init():
    """
    Initialize the application by establishing a connection to the database.
    """
    global conn
    # window = UI()
    # window.setOnDatabaseChanged( lambda: self.onDatabaseChanged())
    # window.setOnAnalyseClicked( lambda: self.analyseQuery() )
    try:
        conn = psycopg2.connect(
            dbname="TPC-H",
            user="postgres",
            password="password",
            host="localhost",
            port="5432"
        )
        logging.info("Database connection established.")
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")


def handle_query(query):
    """
    Handles input SQL query by executing an EXPLAIN command and returns the analyzed results.
    To be called from interface.py.

    Parameters:
    query (str): SQL query to explain and analyze.

    Returns:
        analysis_results (dict): The results from analyzing the EXPLAIN output, None if an error occurs.
    """
    global conn
    if not conn:
        logging.warning("Database connection is not established.")
        return None
    try:
        logging.info("Running EXPLAIN on the provided query...")
        explain_output = run_explain(query, conn)
        analysis_results = analyze_execution_plan(explain_output)
        display_results(analysis_results)  # For debugging purposes
        return analysis_results
    except Exception as e:
        logging.error(f"An error occurred while processing the query: {e}")
        return None


def display_results(results):
    """
    Displays the analysis results from the EXPLAIN command.
    For debugging purposes.

    Parameters:
    results (dict): Results from the analysis of the EXPLAIN command.
    """
    if results:
        report = generate_report(results)
        logging.info("Analysis Report:")
        logging.info(report)
    else:
        logging.info("No results to display.")


def close_connection():
    """
    Closes the database connection.
    To be called from interface.py.
    """
    global conn
    if conn:
        conn.close()
        logging.info("Database connection closed.")


def main():
    # Initialize the application
    init()

    # TODO: Initialize the GUI, call the relevant function from interface.py

    # TODO: Remove these once interface.py is implemented
    # Query to summarize the total account balance of suppliers located in a specific region.
    test_query = """
        SELECT r.r_name AS region, SUM(s.s_acctbal) AS total_balance
        FROM public.supplier s
        JOIN public.nation n ON s.s_nationkey = n.n_nationkey
        JOIN public.region r ON n.n_regionkey = r.r_regionkey
        GROUP BY r.r_name;
    """
    # This method will be called from interface.py when user inputs a query.
    handle_query(test_query)

    # TODO: Remove this once interface.py is implemented
    # This method will be called from interface.py when user chooses to close the application.
    close_connection()


if __name__ == "__main__":
    main()
