from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import declarative_base

Base = declarative_base()
# TODO engine declare

class Log(Base):
    __tablename__ = 'log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ip = Column(String(20), nullable=False)
    date = Column(DateTime, nullable=False)
    size = Column(Integer, nullable=False)

    def __repr__(self):
        return f"Log({self.id=} {self.ip=} {self.date=})"


class Session(Base):
    __tablename__ = 'session'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ip = Column(String(20), nullable=False)
    session_start = Column(DateTime, nullable=False)
    session_end = Column(DateTime, nullable=False)
    download_size = Column(Integer, nullable=False)
    download_count = Column(Integer, nullable=False)

    def __repr__(self):
        return f"Session({self.id} {self.ip=} {self.session_start=} {self.session_end=})"


Base.metadata.create_all()
