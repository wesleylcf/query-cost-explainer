from PyQt5 import uic
from PyQt5.QtCore import QTime, pyqtSignal
from PyQt5.QtWidgets import *
from PyQt5.QtGui import QStandardItemModel, QStandardItem
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class UI(QMainWindow):
    error_text_signal = pyqtSignal(str)
    def __init__(self):
        super(UI, self).__init__()
        
        uic.loadUi("login.ui", self)
        # self.gridLayout.setRowStretch(0, 1)  # First row takes 1/3 of the height
        # self.gridLayout.setRowStretch(1, 2)  # Second row takes 2/3 of the height

        self.state = { 'qep': None, 'tree': {} }

        # Widgets 
        self.query_input = self.findChild(QTextEdit, "inputText")
        self.cost_view = self.findChild(QTextBrowser, "costView")
        self.estimate_btn = self.findChild(QPushButton, "estimateBtn")
        self.clear_btn = self.findChild(QPushButton, "clearBtn")
        self.tree_view = self.findChild(QTreeView, "treeView")

        # Login 
        self.connect_btn = self.findChild(QPushButton, "connectBtn")
        self.disconnect_btn = self.findChild(QPushButton, "disconnectBtn")
        self.name_input = self.findChild(QLineEdit, "nameInput")
        self.user_input = self.findChild(QLineEdit, "userInput")
        self.pw_input = self.findChild(QLineEdit, "pwInput")
        self.host_input = self.findChild(QLineEdit, "hostInput")
        self.port_input = self.findChild(QLineEdit, "portInput")
        self.status_box = self.findChild(QGroupBox, "statusBox")
        self.status_text = self.findChild(QLabel, "statusLabel")
        self.status_text.setText("Disconnected")
         
        self.error_text = self.findChild(QLabel, "error")
        self.error_text.setStyleSheet("color: red")
        self.error_text.setWordWrap(True)
        
        # Button Actions
        self.connect_btn.clicked.connect(self.connect_database)
        self.clear_btn.clicked.connect(self.clear)
        self.tree_view.clicked.connect(self.on_tree_item_clicked)
        self.error_text_signal.connect(self.error_text.setText)


    def connect_database(self):
        name = self.name_input.text()
        user = self.user_input.text()
        password = self.pw_input.text()
        host = self.host_input.text()
        port = self.port_input.text()
        return name, user, password, host, port

    def set_status(self, connected):
        current_time = QTime.currentTime().toString()
        if (connected):
            self.status_text.setText(f"Connected Successfully @ {current_time}")
        else:
            self.status_text.setText("Disconnected")

    def clear(self):
        self.query_input.setPlainText("")
        self.cost_view.setText("")
    def readInput(self):
        return self.query_input.toPlainText()
    
    def setInput(self, text):
        self.query_input.setPlainText(text)
        self.input_sql.setPlainText(text)

    def clear(self):
        self.input_sql.setPlainText("")
        self.label_qep.setText("")
        self.setError("")

    def onQueryChange(self, callback):
        self.input_sql.textChanged.connect(callback)
    
    # callback setter
    def setOnAnalyseClicked(self, callback):
        self.setError("")
        if callback:
            self.estimate_btn.clicked.connect(callback)
        
    def setOnDatabaseChanged(self, callback):
        self.cb_db_changed = callback

    def _onDatabaseChanged(self, cur_index):
        if hasattr(self, "cb_db_changed"):
            self.cb_db_changed()
        
    def _onSchemaItemDoubleClicked(self, item, col):
        self.setInput( f"{self.readInput()} {item.text(col)} ") 

    def on_tree_item_clicked(self, index):
        model = self.tree_view.model()
        item = model.itemFromIndex(index)
        if item is not None:
            node = item.data()
            self.cost_view.setText(self.node_to_string(node))
    
    def node_to_string(self, node):
            node_type = node['Node Type']
            actual_cost = node['Total Cost']
            estimated_cost = node['estimated_cost']
            explanation = node['explanation']
            return f"Node Type: {node_type}\n\nActual Cost: {actual_cost}\n\nEstimated Cost: {estimated_cost}\n\nExplanation:\n{explanation}"

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

    def setError(self, string: str):
        self.error_text_signal.emit(string)