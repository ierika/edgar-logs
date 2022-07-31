import logging
import sqlite3
import time
from pathlib import Path
from typing import List, Tuple

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

    def __init__(self, ip: str, date: pd.Timestamp) -> None:
        """
        - Add 30 minutes timedelta to determine `session_end` datetime
        - Initialize `download_count` to 1
        """
        self.ip = ip
        self.session_start = date
        self.session_end = self.session_start + pd.Timedelta('30 minutes')
        self.download_size = 0
        self.download_count = 0

    def __str__(self) -> str:
        return f'Session({self.ip} = {self.session_start} - {self.session_end})'

    def add_record(self, size: int) -> None:
        """
        Add value to the `download_size` and increment `download_count` by 1
        """
        self.download_size += size
        self.download_count += 1


def reset_session_table(con: sqlite3.Connection) -> None:
    """Reset session table"""
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


def record_sessions(con: sqlalchemy.engine.Connection,
                    session_list: List[Session],
                    retry_limit: int) -> None:
    """
    Clean sessions dictionary out of expired sessions
    The sessions however must be recorded to the database before deletion
    """
    if session_list:
        logger.info('Recording %s expired session(s)', len(session_list))

        def iterparams():
            for s in session_list:
                yield (
                    s.ip,
                    s.session_start.strftime('%Y-%m-%d %H:%M:%S'),
                    s.session_end.strftime('%Y-%m-%d %H:%M:%S'),
                    s.download_size,
                    s.download_count,
                )

        query = f"""
                INSERT INTO {SESSION_TABLE} (
                      ip
                    , session_start
                    , session_end
                    , download_size
                    , download_count
                ) VALUES (?, ?, ?, ?, ?)
                """

        logger.info('Recording %s session(s)', len(session_list))
        # noinspection PyUnresolvedReferences
        con.connection.executemany(query, iterparams())

        # Retry if database has locked out
        retry_counter = 0
        while True:
            retry_counter += 1
            try:
                con.connection.commit()
                break
            except sqlite3.OperationalError as e:
                # Raise exception if retry limit has been exceeded
                if retry_counter == retry_limit:
                    raise e

                # Otherwise, delay a bit and continue
                time.sleep(1)
                continue

        # Empty the `session_list` list
        session_list.clear()


def sessionize(chunksize: int = 1000000,
               limit: Tuple[int, int] = None,
               retry_limit: int = 5,
               executemany_limit: int = 1000) -> None:
    """
    Sessionize log file

    1. Connect to db.sqlite3 database and read the data chunk by chunk
    2. Process logs into their respective sessions
    3. Write the session information into the database for further analysis
    """
    # Dictionary containing all the sessions
    sessions_dict = dict()

    # We accumulate the expired sessions here for batch processing
    expired_sessions = []

    engine = create_engine(DB_CONNECTION_STR)
    with engine.begin() as con:
        # Reset `session` table first
        reset_session_table(con=con)

        # Get logs from database
        logger.info('Reading logs from database => %s', DB_CONNECTION_STR)

        # We need to anticipate that the logs may be randomly ordered
        # Hence, we should explicitly include ORDER BY to the query
        query = f"""
        SELECT id, ip, date, size, idx, code
        FROM {LOG_TABLE}
        ORDER BY date
        """
        if limit:
            query = query + f' LIMIT {limit[0]}, {limit[1]}'

        log_reader = pd.read_sql_query(query,
                                       con=con,
                                       chunksize=chunksize,
                                       index_col='id',
                                       parse_dates=['date'])

        # Iterate through log chunks
        for chunk_number, log_df in enumerate(log_reader, start=1):
            logger.info('Reading chunk number %s of the log table', chunk_number)

            # Flag the valid downloads
            is_valid_code = (log_df['code'] >= 200) & (log_df['code'] < 400)
            is_not_idx = log_df['idx'] == 0
            log_df['is_download'] = is_valid_code & is_not_idx

            for _, row in log_df.iterrows():
                # Housekeeping - record expired sessions to the database
                # We will only do it if reaches a certain threshold
                expired_sessions_count = len(expired_sessions)
                if expired_sessions_count >= executemany_limit:
                    record_sessions(con=con,
                                    session_list=expired_sessions,
                                    retry_limit=retry_limit)

                try:
                    # Get existing session
                    session = sessions_dict[row['ip']]

                    # Update session is expired, re-establish session and -
                    # move the old one to the session_list bucket
                    if session.session_end < row['date']:
                        expired_sessions.append(session)
                        session = Session(ip=row['ip'], date=row['date'])
                        if row['is_download']:
                            session.add_record(size=row['size'])
                        sessions_dict[row['ip']] = session
                        logger.debug('Re-established session => %s', session)

                    # Update existing session if still valid
                    else:
                        if row['is_download']:
                            session.add_record(size=row['size'])
                            logger.debug('Added a record to an existing session: %s', session)

                except KeyError:
                    # Create new session
                    session = Session(ip=row['ip'], date=row['date'])
                    if row['is_download']:
                        session.add_record(size=row['size'])
                    sessions_dict[session.ip] = session
                    logger.debug('Created a new session => %s', session)

        # Finally, make sure all the unfinished sessions at the end of the process gets recorded
        record_sessions(con=con,
                        session_list=list(sessions_dict.values()),
                        retry_limit=retry_limit)

        logger.info('Sessionization complete!')


if __name__ == '__main__':
    """
    Supply `sessionizer` with the location of the log file
    """
    # TODO: Take in arguments from CLI
    sessionize()
