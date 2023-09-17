from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.ext.declarative import declarative_base


### db_connect_config 
db_config = {
    "host": '',
    "port": ,
    "database": "",
    "user": "",
    "password": ""
}


db_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"

### sql alchemy connect, session 
engine = create_engine(db_url)

Base = declarative_base()

Session = sessionmaker(bind=engine)
session= Session()
