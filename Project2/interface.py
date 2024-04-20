from PyQt5 import uic
from PyQt5.QtWidgets import *
import json

class UI(QMainWindow):
    def __init__(self):
        super(UI, self).__init__()
        uic.loadUi("form.ui", self)
        # link to UI widgets

        self.input_sql = self.findChild(QTextEdit, "inputText")
        self.label_qep = self.findChild(QTextBrowser, "textBrowser")
        self.btn_analyse = self.findChild(QPushButton, "estimateBtn")
        self.btn_clear = self.findChild(QPushButton, "clearBtn")
        self.tree_view = self.findChild(QGraphicsView, "treeView")
        
        self.btn_clear.clicked.connect(self.clear)

        # self.centralWidget().setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        # self.input_sql.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        # self.label_qep.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        # self.btn_analyse.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        # self.btn_clear.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        # self.tree_view.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        # self.scroll_area = QScrollArea()
        # self.scroll_area.setWidgetResizable(True)
        # self.scroll_content = QWidget()
        # self.scroll_layout = QVBoxLayout(self.scroll_content)
        # self.scroll_layout.addWidget(self.label_qep)
        # self.scroll_area.setWidget(self.scroll_content)
        # self.setCentralWidget(self.scroll_area)
        
    def showError(self, errMessage, execption=None):
        dialog = QMessageBox()
        dialog.setStyleSheet("QLabel{min-width: 300px;}");
        dialog.setWindowTitle("Error")
        dialog.setText(errMessage)
        if execption is not None:
            dialog.setDetailedText(str(execption))
        dialog.setStandardButtons(QMessageBox.Ok)
        dialog.exec_()

    def clear(self):
        self.input_sql.setPlainText("")
        self.label_qep.setText("")
        
    def readInput(self):
        return self.input_sql.toPlainText()
    
    def setInput(self, text):
        self.input_sql.setPlainText(text)

    
    
    def setResult(self, text):
        
        def process_plan(plan):
            node_type = plan['Node Type']
            actual_cost = plan['Total Cost']
            estimated_cost = plan['estimated_cost']
            explanation = '\n'.join(plan.get('explanation', []))
            return f"Node Type: {node_type}\nActual Cost: {actual_cost}\nEstimated Cost: {estimated_cost}\nExplanation: {explanation}"

        def process_plans(plans):
            result = []
            for plan in plans:
                result.append("\n" + process_plan(plan))
                if 'Plans' in plan:
                    result.append("\n" + process_plans(plan['Plans']))
            return '\n'.join(result)
    
        data = json.loads(text)
        node_type = data['Node Type']
        actual_cost = data['Total Cost']
        estimated_cost = data['estimated_cost']
        explanation = '\n'.join(data.get('explanation', []))
        plans = process_plans(data['Plans'])

        result = f"Node Type: {node_type}\nActual Cost: {actual_cost}\nEstimated Cost: {estimated_cost}\nExplanation: {explanation}\n\nPlans:\n{plans}"
        print(result)
        print("HELLOOOOOOOOOO")
        self.label_qep.setText(result)
    
    # callback setter
    def setOnAnalyseClicked(self, callback):
        if callback:
            self.btn_analyse.clicked.connect(callback)
        
    def setOnDatabaseChanged(self, callback):
        self.cb_db_changed = callback

    # private events handling 
    def _onDatabaseChanged(self, cur_index):
        if hasattr(self, "cb_db_changed"):
            self.cb_db_changed()
        
    def _onSchemaItemDoubleClicked(self, item, col):
        # append item text to input text area
        self.setInput( f"{self.readInput()} {item.text(col)} ") 

        


'''
# maybe this part move to main script
if __name__ == "__main__":
    app = QApplication(sys.argv)
    apply_stylesheet(app, theme="light_cyan_500.xml")
    window = UI()
    
    # fake schema
    schema = {
        "tabel1":["item_1", "item_2", "item_3"],
        "tabel2":["item_4", "item_5", "item_6"],
        "tabel3":["item_7", "item_8", "item_9"],
        "tabel4":["item_10", "item_11", "item_12"]
    }
    window.setSchema(schema)
    
    # assigning callback
    window.setOnClicked(
        lambda: window.setResult( window.readInput() )
    )
    window.show()
    sys.exit(app.exec_())
'''