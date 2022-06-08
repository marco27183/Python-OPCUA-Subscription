from msilib.schema import Class
import pyodbc
import sqlalchemy
import datetime
import pandas as pd


class SQLConnector:
    def __init__(self, database, server):
        self.server = server
        self.database = database
        self.connection = None
        self.dbEngine = None

    def connectPYODBC(self):
        self.cnxn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
            + self.server
            + ";DATABASE="
            + self.database,
            trusted_connection="yes",
        )
        self.cursor = self.cnxn.cursor()

    def createEngine(self, echo):
        self.dbEngine = sqlalchemy.create_engine(
            "mssql+pyodbc://@"
            + self.server
            + "/"
            + self.database
            + "?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server",
            connect_args={"connect_timeout": 10},
            echo=echo,
            pool_pre_ping=True,
        )
        self.connectToDatabase()

    def connectToDatabase(self):
        if self.testEngineConnection():
            self.connection = self.dbEngine.connect()
            print(datetime.datetime.now(), ": Connection successfull")
        else:
            print(datetime.datetime.now(), ": No connection created")

    def testEngineConnection(self):
        try:
            with self.dbEngine.connect() as con:
                con.execute("SELECT 1")
            print(datetime.datetime.now(), ": Engine is valid")
            return True
        except Exception as e:
            print(datetime.datetime.now(), f": Engine invalid: {str(e)}")
            return False

    def closeConnection(self):
        self.connection.close()

    def disposeEngine(self):
        self.dbEngine.dispose()

    def finishSQL(self):
        try:
            self.closeConnection()
            self.disposeEngine()
            return True
        except:
            return False

    def optimisticPing(self):
        if self.dbEngine.connection_invalidated:
            print("Connection was invalidated")
        self.connectToDatabase()


def load_df(database, tableName, server):
    try:
        sql_connection = SQLConnector(database, server)
        sql_connection.createEngine(False)
        if sql_connection.dbEngine.dialect.has_table(
            sql_connection.dbEngine, tableName, schema="dbo"
        ):
            print("Table found")
            df = pd.read_sql_table(
                tableName,
                sql_connection.dbEngine,
                schema="dbo",
                parse_dates=["ServerZeit", "SourceZeit", "ReceiveZeit"],
            )
        else:
            print("Table does not exist in provided server and database")
            df = None
        closed = sql_connection.finishSQL()
        print("Connection was closed succesfully:", closed)
        return df
    except Exception as e:
        print("Connection failed")
        print(e)
        return None
