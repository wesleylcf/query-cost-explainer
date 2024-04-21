from PyQt5 import uic
from PyQt5.QtWidgets import *
from PyQt5.QtGui import QStandardItemModel, QStandardItem
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class UI(QMainWindow):
    def __init__(self):
        super(UI, self).__init__()
        uic.loadUi("form.ui", self)
        # link to UI widgets
        self.state = { 'qep': None, 'tree': {} }

        self.input_sql = self.findChild(QTextEdit, "inputText")
        self.label_qep = self.findChild(QTextBrowser, "textBrowser")
        self.btn_analyse = self.findChild(QPushButton, "estimateBtn")
        self.btn_clear = self.findChild(QPushButton, "clearBtn")
        # self.tree_view = self.findChild(QGraphicsView, "treeView")
        self.tree_view = self.findChild(QTreeView, "treeView")  # Changed from QGraphicsView to QTreeView

        
        self.btn_clear.clicked.connect(self.clear)
        self.tree_view.clicked.connect(self.on_tree_item_clicked)  # Connect the clicked signal to the method


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

    def on_tree_item_clicked(self, index):
        model = self.tree_view.model()
        item = model.itemFromIndex(index)
        if item is not None:
            node = item.data()
            self.label_qep.setText(self.node_to_string(node))
    
    def node_to_string(self, node):
            node_type = node['Node Type']
            actual_cost = node['Total Cost']
            estimated_cost = node['estimated_cost']
            explanation = node['explanation']
            return f"Node Type: {node_type}\nActual Cost: {actual_cost}\nEstimated Cost: {estimated_cost}\nExplanation: {explanation}"

    def setTreeData(self, root):
        model = QStandardItemModel()
        self.buildTree(model, "", root)
        self.tree_view.setModel(model)

    def buildTree(self, parent, parent_id, node):
        node_item = QStandardItem(node["Node Type"])
        parent.appendRow(node_item)
        node_item.setData(node)
        if 'Plans' in node:
            for child in node["Plans"]:
                self.buildTree(node_item, "", child)