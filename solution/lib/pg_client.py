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
            self.__connection = psycopg.connect(self.url(), autocommit=True)
            
        return self.__connection

    def save_data_to_pg(self, sql, data) -> None:
        conn = self.get_connection()

        with conn.cursor() as cur:
            cur.executemany(sql, data)

    def close(self) -> None:
        if self.__connection and not self.__connection.closed:
            self.__connection.close()

        self.__connection = None