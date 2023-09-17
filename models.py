from sqlalchemy import Boolean, Column, Integer, String, DateTime,text
from database import Base
from pydantic import BaseModel
import datetime


class UserRequest(Base):
    __tablename__ = 'query_user_requests'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(100))
    request_time = Column(String(100))
    slash_command_name = Column(String(100))
    until_ranking = Column(Integer)
    query_result_csv_path = Column(String(100))
    query_result_img_path = Column(String(100))

class UserRequestLog(Base):
    __tablename__ = "user_request_log"
    request_id = Column(Integer,primary_key=True, autoincrement=True)
    user_id = Column(String(100))
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    action = Column(String(100))
    target_command = Column(String(100))
    
    
    
    
    
### user_result Table
class UserResult(Base):
    __tablename__ = 'query_user_results'
    id = Column(String(50), primary_key=True)
    user_id = Column(String(50))
    timestamp = Column(DateTime)
    query_result_path = Column(String(100))
    query_result_img_path = Column(String(100))

### request
class UserRequestData(BaseModel):
    user_id: str 
    request_time: str
    slash_command_name : str
    until_ranking: int
    
class UserResultData(BaseModel):
    user_id: str 
    plot_img_path: str
    
