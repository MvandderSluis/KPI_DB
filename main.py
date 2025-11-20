import sys
import argparse
import pyodbc
from PyQt5 import QtWidgets as qtw, QtCore as qtc


class SqlTableEditor(qtw.QWidget):
    """
    Generieke editor voor eenvoudige SQL-tabellen.
    Kan read-only of volledig mutabel zijn.
    """

    def __init__(self, connection, table_name, columns, order_by=None, read_only=False, parent=None):
        super().__init__(parent)

        self.connection = connection
        self.cursor = connection.cursor()
        self.table_name = table_name
        self.columns = columns
        self.order_by = order_by or columns[0]
        self.read_only = read_only

        layout = qtw.QVBoxLayout(self)

        # Tabel
        self.table = qtw.QTableWidget()
        self.table.setColumnCount(len(self.columns))
        self.table.setHorizontalHeaderLabels(self.columns)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(qtw.QHeaderView.Stretch)

        # Bij read-only: cellen niet bewerkbaar
        if self.read_only:
            self.table.setEditTriggers(qtw.QAbstractItemView.NoEditTriggers)

        layout.addWidget(self.table)

        # Knoppen onderaan
        self.btn_layout = qtw.QHBoxLayout()
        self.btn_add = qtw.QPushButton("Rij toevoegen")
        self.btn_delete = qtw.QPushButton("Geselecteerde rij(en) verwijderen")
        self.btn_save = qtw.QPushButton("Opslaan naar database")
        self.btn_refresh = qtw.QPushButton("Verversen")

        self.btn_add.clicked.connect(self.add_row)
        self.btn_delete.clicked.connect(self.delete_selected_rows)
        self.btn_save.clicked.connect(self.save_changes)
        self.btn_refresh.clicked.connect(self.load_data)

        # Bij read-only: knoppen verbergen
        if not self.read_only:
            self.btn_layout.addWidget(self.btn_add)
            self.btn_layout.addWidget(self.btn_delete)
            self.btn_layout.addWidget(self.btn_save)
        self.btn_layout.addWidget(self.btn_refresh)
        self.btn_layout.addStretch()

        layout.addLayout(self.btn_layout)

        self.load_data()

    # ---------- Data laden / muteren ----------

    def load_data(self):
        self.table.setRowCount(0)
        try:
            sql = f"SELECT {', '.join(self.columns)} FROM {self.table_name}  ORDER BY {self.order_by}"
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()

            for r_idx, row in enumerate(rows):
                self.table.insertRow(r_idx)
                for c_idx, value in enumerate(row):
                    text = "" if value is None else str(value)
                    item = qtw.QTableWidgetItem(text)

                    if self.read_only:
                        # Bij read-only ook cellen op "enabled but not editable"
                        item.setFlags(item.flags() & ~qtc.Qt.ItemIsEditable)

                    self.table.setItem(r_idx, c_idx, item)

        except Exception as e:
            qtw.QMessageBox.critical(self, "Fout bij laden", f"Fout bij het laden van {self.table_name}:\n{e}")

    # --- Alleen mutabel als read_only=False ---

    def add_row(self):
        if self.read_only:
            return
        self.table.insertRow(self.table.rowCount())

    def delete_selected_rows(self):
        if self.read_only:
            return
        selected = self.table.selectionModel().selectedRows()
        for index in sorted(selected, key=lambda x: x.row(), reverse=True):
            self.table.removeRow(index.row())

    def save_changes(self):
        if self.read_only:
            return

        reply = qtw.QMessageBox.question(self, "Bevestigen", f"Alle bestaande records in {self.table_name} "
                                                             f"worden overschreven.\n Doorgaan?",
                                         qtw.QMessageBox.Yes | qtw.QMessageBox.No)
        if reply != qtw.QMessageBox.Yes:
            return

        try:
            self.connection.autocommit = False
            self.cursor.execute(f"DELETE FROM {self.table_name}")

            col_list = ", ".join(self.columns)
            params = ", ".join(["?"] * len(self.columns))
            insert_sql = f"INSERT INTO {self.table_name} ({col_list}) VALUES ({params})"

            for r in range(self.table.rowCount()):
                values = []
                for c in range(len(self.columns)):
                    item = self.table.item(r, c)
                    text = item.text().strip() if item else ""
                    values.append(text if text != "" else None)

                if all(v is None for v in values):
                    continue

                self.cursor.execute(insert_sql, values)

            self.connection.commit()
            self.connection.autocommit = True

            qtw.QMessageBox.information(self, "Opgeslagen", f"Wijzigingen zijn opgeslagen in {self.table_name}.")
            self.load_data()

        except Exception as e:
            self.connection.rollback()
            self.connection.autocommit = True
            qtw.QMessageBox.critical(self, "Fout bij opslaan", f"Er is een fout opgetreden "
                                                               f"bij het opslaan van {self.table_name}:\n{e}")


class MainWindow(qtw.QMainWindow):
    def __init__(self, settings):
        super().__init__()

        self.server = settings.get("server")
        self.database = settings.get("database")
        self.version = 0.1

        self._connect()

        self.setWindowTitle(f"KPI beheer app v{self.version}")
        self.resize(900, 600)
        # Menustructuur
        menubar = self.menuBar()
        log_menu = menubar.addMenu('Logbestanden')
        table_menu = menubar.addMenu('Tabelonderhoud')

        # Acties
        action_units = table_menu.addAction('Units')
        action_targets = table_menu.addAction("Targets")
        action_phishing = table_menu.addAction("Phishing")
        action_training = table_menu.addAction("Training")
        action_etl_log = log_menu.addAction('ETL logging')
        action_etl_status = log_menu.addAction('ETL status')
        # triggers
        action_targets.triggered.connect(lambda: self._open_table('targets'))
        action_units.triggered.connect(lambda: self._open_table('units'))
        action_phishing.triggered.connect(lambda: self._open_table('phishing'))
        action_training.triggered.connect(lambda: self._open_table('training'))
        action_etl_log.triggered.connect(lambda: self._open_table('etl_log'))
        action_etl_status.triggered.connect(lambda: self._open_table('etl_status'))

        # Centrale widget: tabbladen voor meerdere tabellen
        self.tabs = qtw.QTabWidget()
        self.setCentralWidget(self.tabs)

        self.show()

    def _open_table(self, tab_name):
        # Controleer of tab al bestaat
        for i in range(self.tabs.count()):
            if self.tabs.tabText(i) == tab_name:
                self.tabs.setCurrentIndex(i)
                return

        # Nieuw tabblad aanmaken
        if tab_name == 'units':
            editor = SqlTableEditor(connection=self.connection, table_name="MST.MST_Units",
                                    columns=["Unit", "[Unit name]"], order_by="Unit", read_only=False)
        elif tab_name == 'targets':
            editor = SqlTableEditor(connection=self.connection, table_name="MST.MST_Targets",
                                    columns=["KPI", "Target", "Active", "Active_From", "Active_To"], order_by="KPI",
                                    read_only=False,)
        elif tab_name == 'phishing':
            editor = SqlTableEditor(connection=self.connection, table_name='MST.MST_Phishing_Campaigns',
                                    columns=["Campaign_id", "Name", "Active"], order_by="Name")
        elif tab_name == 'training':
            editor = SqlTableEditor(connection=self.connection, table_name='MST.MST_Training_Campaigns',
                                    columns=["Name", "Active", "Type"], order_by="Name", read_only=False)
        elif tab_name == 'etl_log':
            editor = SqlTableEditor(connection=self.connection, table_name='MST.Logdata',
                                    columns=['Timestamp', 'Source', 'Severity', '[Log regel]'],
                                    order_by='Timestamp DESC', read_only=True)
        elif tab_name == 'etl_status':
            editor = SqlTableEditor(connection=self.connection, table_name='MST.Source_status',
                                    columns=["Source", "Run_date", "Max_fetches", "Fetched_today", "Finished"],
                                    order_by="Source", read_only=True)
        self.tabs.addTab(editor, tab_name)
        self.tabs.setCurrentIndex(self.tabs.count() - 1)

    def _connect(self):
        if not self.server:
            self.server = r'HI000090\SQLEXPRESS'
        if not self.database:
            self.database = 'KPI database'

        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            "Trusted_Connection=yes;"
        )
        self.connection = pyodbc.connect(connection_string)
        self.cursor = self.connection.cursor()
        self.cursor.fast_executemany = True


if __name__ == '__main__':
    app = qtw.QApplication(sys.argv)

    parser = argparse.ArgumentParser(description="Importeer data naar SQL Server.")
    parser.add_argument("--server", required=False, help="Naam van de SQL Server")
    parser.add_argument("--database", required=False, help="Naam van de database")
    args = parser.parse_args()

    settings = {"server": args.server, "database": args.database}

    mw = MainWindow(settings)
    sys.exit(app.exec_())
