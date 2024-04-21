import psycopg2
import logging
import sys
from PyQt5.QtWidgets import QApplication
from explain import Explainer
from interface import UI

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Application:
    def __init__(self):
        self.app = QApplication(sys.argv)
        self.window = UI()
        self.window.setOnDatabaseChanged(self.onDatabaseChanged)
        self.window.setOnAnalyseClicked(self.analyseQuery)
        self.window.onQueryChange(self.resetError)
        self.conn = None
        self.explainer = None
        self.connected = False
        self.window.connect_btn.clicked.connect(self.connect)
        self.window.disconnect_btn.clicked.connect(self.close_connection)

    
    def resetError(self):
        self.window.setError("")

    def onDatabaseChanged(self):
        # Placeholder for database change logic?
        logging.info("Database changed.")

    def analyseQuery(self):
        query = self.window.readInput()
        if self.explainer:
            try:
                logging.info("Running EXPLAIN on the provided query...")
                explain_output = self.explainer.run_explain(query)
                analysis_results = self.explainer.analyze_execution_plan(explain_output)
                report = self.explainer.generate_report(analysis_results)
                # self.window.setResult(report)
                self.window.setTreeData(analysis_results)
                logging.info("Analysis Report:")
                logging.info(report)
            except Exception as e:
                logging.error("An error occurred while processing the query", e)
                self.window.setError(str(e))

    def connect(self):
        name, user, password, host, port = self.window.connect_database()
        if name and user and password and host and port:
            try:
                self.conn = psycopg2.connect(
                    dbname=name,
                    user=user,
                    password=password,
                    host=host,
                    port=port
                )
                self.explainer = Explainer(self.conn)
                self.connected = True
                self.window.set_status(self.connected)
                logging.info("Database connection established.")
            except Exception as e:
                self.connected = False
                self.window.set_status(self.connected)
                logging.error("An error occurred while connecting to the database", e)
        else:
            logging.error("Please fill all the fields.")
        
        # default
        #    self.conn = psycopg2.connect(
        #          dbname="TPC-H",
        #          user="postgres",
        #          password="password",
        #          host="localhost",
        #          port="5432"

    def close_connection(self):
        if self.conn:
            self.connected = False
            self.window.set_status(self.connected)            
            self.conn.close()
            logging.info("Database connection closed.")

    def run(self):
        self.window.show()
        sys.exit(self.app.exec_())

    def __del__(self):
        self.close_connection()

def main():
    app = Application()
    app.run()

if __name__ == "__main__":
    main()
