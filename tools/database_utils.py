import re

from sqlalchemy import create_engine, inspect, URL, text
from typing import Dict, List


def _build_db_url(db_type: str, host: str, port: int, username: str, password: str, database: str) -> URL:
    """构建SQLAlchemy连接URL"""
    db_type = db_type.lower()

    if db_type == 'doris':
        db_type = 'mysql'

    drivers = {
        'mysql': 'pymysql',
        'oracle': 'cx_oracle',
        'sqlserver': 'pyodbc',
        'postgresql': 'psycopg2'
    }

    if db_type == 'oracle':
        return URL.create(
            "oracle+cx_oracle",
            username=username,
            password=password,
            host=host,
            port=port
        )

    if db_type == 'sqlserver':
        return URL.create(
            "mssql+pyodbc",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "encrypt": "yes",
                "TrustServerCertificate": "yes"
            }
        )

    return URL.create(
        f"{db_type}+{drivers[db_type]}",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database
    )

class DBSchemaExtractor:
    def __init__(self, db_type: str, host: str, port: int, username: str, password: str, database: str):
        db_url = _build_db_url(db_type, host, port, username, password, database)
        self.engine = create_engine(db_url)
        self.inspector = inspect(self.engine)
        self.db_type = db_type

    def get_all_tables_schema(self, table_names: str | None = None) -> Dict:
        schemas = {}
        # 如果 Doris 不支持 inspector 的反射，则需要自定义获取表名方式
        if self.db_type == "doris":
            # 这里通过 SHOW TABLES 获取 Doris 表名，根据实际情况调整查询语句
            with self.engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES;")).fetchall()
                all_table_names = [row[0] for row in result]
        else:
            all_table_names = self.inspector.get_table_names()

        target_tables = all_table_names
        if table_names:
            target_tables = [table.strip() for table in table_names.split(',')]
            target_tables = [table for table in target_tables if table in all_table_names]

        for table in target_tables:
            schemas[table] = self._get_table_schema(table)

        return schemas

    def _get_table_schema(self, table_name: str) -> Dict:
        if self.db_type == "oracle":
            return self._get_oracle_table_schema(table_name)
        elif self.db_type == "doris":
            return self._get_doris_table_schema(table_name)
        else:
            schema = {"table_name": table_name, "comment": "", "columns": []}
            schema["comment"] = self._get_table_comment(table_name)
            for column in self.inspector.get_columns(table_name):
                schema["columns"].append({
                    "name": column["name"],
                    "comment": (column.get("comment") or "").replace("\n", ""),
                    "type": str(column["type"])
                })
            return schema

    def _get_table_comment(self, table_name: str) -> str:
        query = ""
        if self.db_type == "mysql":
            query = f"SELECT table_comment FROM information_schema.tables WHERE table_name = '{table_name}';"
        elif self.db_type == "postgresql":
            query = f"SELECT obj_description('{table_name}'::regclass, 'pg_class');"
        elif self.db_type == "sqlserver":
            query = f"SELECT cast(EP.value as nvarchar(500)) FROM sys.tables T INNER JOIN sys.extended_properties EP ON T.object_id = EP.major_id WHERE T.name = '{table_name}' AND EP.minor_id = 0;"
        elif self.db_type == "doris":
            # Doris：从 CREATE 语句中解析表注释
            create_stmt = self._get_doris_create_statement(table_name)
            match = re.search(r"COMMENT='(.*?)'", create_stmt, re.IGNORECASE)
            return match.group(1) if match else table_name
        if query:
            with self.engine.connect() as conn:
                result = conn.execute(text(query)).fetchone()
                return result[0] if result and result[0] else table_name
        return table_name

    def _get_doris_create_statement(self, table_name: str) -> str:
        query = f"SHOW CREATE TABLE {table_name};"
        with self.engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            return result[1] if result and result[1] else ""

    def _get_doris_table_schema(self, table_name: str) -> Dict:
        # 通过 information_schema 查询 Doris 表和字段信息
        schema = {"table_name": table_name, "comment": "", "columns": []}
        db_name = self.engine.url.database
        with self.engine.connect() as conn:
            # 获取表注释
            table_query = f"SELECT table_comment FROM information_schema.tables WHERE table_schema = '{db_name}' AND table_name = '{table_name}';"
            table_result = conn.execute(text(table_query)).fetchone()
            if table_result:
                schema["comment"] = table_result[0]
            # 获取字段信息
            col_query = f"SELECT column_name, column_type, column_comment FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' ORDER BY ordinal_position"
            col_results = conn.execute(text(col_query)).fetchall()
            for row in col_results:
                schema["columns"].append({
                    "name": row[0],
                    "type": row[1],
                    "comment": (row[2] or "").replace("\n", "")
                })
        return schema

    def _get_oracle_table_schema(self, table_name: str) -> Dict:
        schema = {"table_name": table_name, "comment": "", "columns": []}
        with self.engine.connect() as conn:
            query = f"SELECT COMMENTS FROM USER_TAB_COMMENTS WHERE TABLE_NAME = '{table_name}';"
            result = conn.execute(text(query)).fetchone()
            schema["comment"] = result[0] if result else ""
            query = f"SELECT COLUMN_NAME, DATA_TYPE, (SELECT COMMENTS FROM USER_COL_COMMENTS WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = C.COLUMN_NAME) AS COMMENT FROM USER_TAB_COLUMNS C WHERE TABLE_NAME = '{table_name}';"
            result = conn.execute(text(query)).fetchall()
            for row in result:
                schema["columns"].append({
                    "name": row[0],
                    "type": row[1],
                    "comment": (row[2] or "").replace("\n", "")
                })
        return schema
