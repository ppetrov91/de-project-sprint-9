import psycopg
from psycopg import Connection


class PostgresClient:
    def __init__(self, 
                 host: str,
                 port: int, 
                 db_name: str, 
                 user: str, 
                 pw: str, 
                 sslmode: str = "require") -> None:
        self.__host = host
        self.__port = port
        self.__db_name = db_name
        self.__user = user
        self.__pw = pw
        self.__sslmode = sslmode
        self.__connection = None

    def url(self) -> str:
        return f"""
            host={self.__host}
            port={self.__port}
            dbname={self.__db_name}
            user={self.__user}
            password={self.__pw}
            target_session_attrs=read-write
            sslmode={self.__sslmode}
        """

    def get_connection(self) -> Connection:
        if not self.__connection:
            self.__connection = psycopg.connect(self.url())
            
        return self.__connection

    def exec_sql_files(self, file_data_dict) -> None:
        conn = self.get_connection()
        with conn.transaction(), conn.cursor() as cur:
            for file, data in file_data_dict.items():
                with open(file, "r") as f:
                    sql = f.read()
                    cur.execute(sql, data)

    def bulk_data_load(self, sql, data) -> None:
        conn = self.get_connection()

        with conn.transaction(), conn.cursor() as cur:
            cur.executemany(sql, data)

    def close(self) -> None:
        if self.__connection and not self.__connection.closed:
            self.__connection.close()

        self.__connection = None