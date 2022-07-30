import logging
import sqlite3
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import sqlalchemy.engine
from sqlalchemy import create_engine

# Table names
LOG_TABLE = 'log'
SESSION_TABLE = 'session'

# File paths
BASE_DIR = Path(__file__).parent.parent.absolute()
DATA_DIR = BASE_DIR / 'data'
APPLICATION_LOG = BASE_DIR / 'application.log'
DB_CONNECTION_STR = f'sqlite:///{DATA_DIR / "db.sqlite3"}'

# Logging
logging.basicConfig(filename=APPLICATION_LOG,
                    filemode='a',
                    format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger('Sessionize EDGAR Apache Logs')


class Session:
    """
    Session objects
    """
    def __init__(self, ip: str, date: pd.Timestamp, size: int) -> None:
        """
        - Add 30 minutes timedelta to determine `session_end` datetime
        - Initialize `download_count` to 1
        """
        self.ip = ip
        self.session_start = date
        self.session_end = self.session_start + pd.Timedelta('30 minutes')
        self.download_size = size
        self.download_count = 1

    def __str__(self) -> str:
        return f'Session({self.ip} = {self.session_start} - {self.session_end})'

    def add_record(self, ip: str, size: int) -> None:
        """
        Add value to the `download_size` and increment `download_count` by 1
        """
        assert ip == self.ip
        self.download_size += size
        self.download_count += 1


def reset_session_table(con: sqlite3.Connection) -> None:
    con.execute(f'DROP TABLE IF EXISTS {SESSION_TABLE}')
    con.execute(f"""
                CREATE TABLE {SESSION_TABLE} (
                    id INTEGER PRIMARY KEY,
                    ip TEXT NOT NULL,
                    session_start DATETIME NOT NULL,
                    session_end DATETIME NOT NULL,
                    download_size INTEGER NOT NULL,
                    download_count INTEGER NOT NULL
                )
                """)


def clean_sessions_dict(con: sqlalchemy.engine.Connection,
                        latest_date: pd.Timestamp,
                        sessions_dict: Dict[str, Session]) -> None:
    """
    Clean sessions dictionary out of expired sessions
    The sessions however must be recorded to the database before deletion
    """
    sessions_to_write = []
    for ip, session in sessions_dict.items():
        if session.session_end < latest_date:
            sessions_to_write.append(session)

    if sessions_to_write:
        query = f"""
        INSERT INTO {SESSION_TABLE} (
              ip
            , session_start
            , session_end
            , download_size
            , download_count
        ) VALUES (?, ?, ?, ?, ?)
        """

        def iterparams():
            for s in sessions_to_write:
                yield (
                    s.ip,
                    s.session_start.strftime('%Y-%m-%d %H:%M:%S'),
                    s.session_end.strftime('%Y-%m-%d %H:%M:%S'),
                    s.download_size,
                    s.download_count,
                )

        logger.info('Recording %s session(s)', len(sessions_to_write))
        con.connection.executemany(query, iterparams())
        con.connection.commit()

        # Delete sessions from sessions_dict
        for session in sessions_to_write:
            del sessions_dict[session.ip]


def sessionize(chunksize: int = 1000000, limit: Tuple[int, int] = None) -> None:
    """
    Sessionize log file

    1. Connect to db.sqlite3 database and read the data chunk by chunk
    2. Process logs into their respective sessions
    3. Write the session information into the database for further analysis
    """
    # Class containing the sessions
    sessions_dict = dict()

    engine = create_engine(DB_CONNECTION_STR)
    with engine.begin() as con:
        # Reset `session` table first
        reset_session_table(con=con)

        # Get logs from database
        logger.info('Reading logs from database => %s', DB_CONNECTION_STR)

        # We need to anticipate that the logs may be randomly ordered
        # Hence, we should explicitly include ORDER BY to the query
        # Make a connection to the db.sqlite3 database
        query = f'SELECT * FROM {LOG_TABLE} ORDER BY date'
        if limit:
            query = query + f' LIMIT {limit[0]}, {limit[1]}'

        log_reader = pd.read_sql_query(query,
                                       con=con,
                                       chunksize=chunksize,
                                       parse_dates=['date'])

        # Iterate through log chunks
        for chunk_number, log_df in enumerate(log_reader, start=1):
            logger.info('Reading chunk number %s of the log table', chunk_number)

            for _, row in log_df.iterrows():
                # Do some housekeeping on `session_dict` to rid of expired sessions
                clean_sessions_dict(con=con, latest_date=row['date'], sessions_dict=sessions_dict)

                try:
                    # Update existing session
                    session = sessions_dict[row['ip']]
                    session.add_record(ip=row['ip'], size=row['size'])
                    logger.info('Added a record to an existing session: %s', str(session))

                except KeyError:
                    # Create session
                    session = Session(ip=row['ip'], date=row['date'], size=row['size'])
                    sessions_dict[session.ip] = session
                    logger.info('Created a new session => %s', session)

        logger.info('Sessionization complete!')


if __name__ == '__main__':
    """
    Supply `sessionizer` with the location of the log file
    """
    sessionize()
