from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL

engine = create_engine('mysql://nofees:rQjSc!W[()&gM4b{@keysbalances.cu4uitdlx0ou.ap-northeast-1.rds.amazonaws.com:3306/main', pool_recycle=3600)
Base = declarative_base()


class EthereumAccount(Base):
    __tablename__ = 'EthereumAccount'
    public_key   = Column(String(130), primary_key=True)
    private_key  = Column(String(70), nullable=False)
    balance  = Column(Integer)
    username = Column(String(40))
    last_block = Column(Integer)

class NanoAccount(Base):
    __tablename__ = 'NanoAccount'
    public_key   = Column(String(130), primary_key=True)
    private_key  = Column(String(70), nullable=False)
    balance  = Column(Integer)
    username = Column(String(40))
    last_block = Column(String(70))

def create_tables():
    Base.metadata.create_all(bind=engine)

Session = scoped_session(sessionmaker(bind=engine))
session = Session()
