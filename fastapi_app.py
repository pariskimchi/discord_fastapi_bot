from tarfile import TarError
from venv import create
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import textwrap

from sqlalchemy import create_engine, Column, String, DateTime, text,Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime 

import os 
import pandas as pd 
from datetime import datetime

import requests
import boto3
import json
import plotly.express as px
import plotly.graph_objects as go 
import plotly.io as pio

from database import Base, session, engine
from models import UserRequest

from fastapi import APIRouter

bucket = "etf-stock-project-data-lake-investinglion"
query_result_dir = "query_result_dir"
img_query_dir = "img_query_dir"

aws_access_key_id, aws_secret_access_key= ('AKIARHB5I5BIZPCAL3KH', 'aNOmpQuUiIbp9LyzD6KEFDeW5D0oWQiQS/WVZdmo')

### fastapi_app 
app = FastAPI()

    
### request
class UserRequestData(BaseModel):
    user_id: str 
    request_time: str
    slash_command_name : str
    until_ranking: int

class UserRequestDataQoq(BaseModel):
    user_id: str 
    request_time: str 
    command: str 
    command_group : str 
    command_type : str 
    input_params : dict

class UserRequestDataDiscord(BaseModel):
    user_id: str 
    request_time: str 
    command: str 
    command_group : str 
    command_type : str 
    input_params : dict


## 그냥 로그 데이터 통합해서 넣자 
class UserLogData(Base):
    __tablename__ = 'user_log_data'
    log_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(100))
    request_time = Column(String(100))
    response_time = Column(String(100))
    status = Column(String(25))
    command = Column(String(100))
    command_group = Column(String(100))
    command_type = Column(String(100))
    input_params = Column(String(255))
    target_query_key_path = Column(String(255))
    query_result_csv_path = Column(String(255))
    query_result_img_path = Column(String(255))


######### FASTAPI REST 

### 1. 사용자가 원하는 쿼리 요구사항 (query_request_text) 요청.
### 사용자 요청 데이터를 RDS에 적재 
@app.post('/user_request_qoq/rank')
def create_user_request_qoq(data:UserRequestData):
    
    # query_Text -> (mapping) -> target_query
    try:
        target_qoq_query = f"""
            SELECT 
                qoq_growth_rank as rank,
                ticker,
                ROUND(qoq_growth,2) as qoq_growth,
                3_quarters_growth,
                date
            FROM sp500_sales_growth_qoq
            ORDER BY qoq_growth_rank
            LIMIT {data.until_ranking};
        """
        
        print(f"user_id:{data.user_id}")
        print(f"reqeust_time:{data.request_time}")
        print(f"slack_command_name:{data.slash_command_name}")
        print(f"until_ranking:{data.until_ranking}")
        
        #### 요청되면 query_request_text -> 적합한 쿼리문으로 매핑 변환 
        #### 매핑 변환된 쿼리문을 -> target_mapping_query
        #### target_mapping_query를 보내서 결과를 받는다
        
        ### 쿼리문 실행 + s3에 결과 적재 + s3에 저장 이미지 경로
        result_df = execute_query(target_qoq_query)
        
        user_query_result_csv_name = f"{data.user_id}_{data.request_time}.csv"
        user_query_result_csv_path = f"{query_result_dir}/{user_query_result_csv_name}"
        
        ### result_Df -> csv (SAVE -> s3)
        save_df_to_s3(result_df, bucket, user_query_result_csv_path)
        
        #### plotly로 이미지화 해서 s3에 저장
        # img_query_dir

        img_key = f"{img_query_dir}/{data.user_id}_{data.request_time}.png"
        img_bytes = plot_table(result_df)    
        
        ## s3에 이미지 저장 
        save_data_to_s3(img_bytes, bucket, img_key)

        query_result_path = os.path.join(bucket,user_query_result_csv_path )
        query_img_path = os.path.join(bucket, img_key)
        
        user_request = UserRequest(
            user_id = data.user_id, 
            request_time = data.request_time, 
            slash_command_name = data.slash_command_name,
            until_ranking = data.until_ranking,
            query_result_csv_path = query_result_path,
            query_result_img_path = query_img_path 
        )
        
        session.add(user_request)
        session.commit()
        
        return_dict = {
            'user_id':user_request.user_id, 
            'query_result_csv_path':user_request.query_result_csv_path,
            'query_result_img_path':user_request.query_result_img_path
        }
        
        
        return return_dict
    except:
        print("Exception error!!!")    


### user_request/
@app.post('/user_request_qoq/tickers')
def get_growth_sales_qoq_tickers(data:UserRequestDataQoq):
    
    ticker_list = data.input_params['tickers']
    
    ## valid ticker flag check 
    ticker_string = ','.join([f"'{ticker}'" for ticker in ticker_list])  # 생성된 티커 문자열
    
    #### 그냥 로그 데이터 통합해서 넣자
    query_result_csv_path, query_result_img_path = "",""
    
    ### target_qoq_query_tickers = 
    target_qoq_query = f"""
            SELECT 
                qoq_growth_rank as rank,
                ticker,
                ROUND(qoq_growth,2) as qoq_growth,
                3_quarters_growth,
                date
            FROM sp500_sales_growth_qoq
            WHERE (ticker, date) IN (
                SELECT ticker, MAX(date) as max_date
                FROM sp500_sales_growth_qoq
                WHERE ticker IN ({ticker_string})
                GROUP BY ticker
                ) 
            ORDER BY qoq_growth_rank 
    """
    
    status_flag = False
    target_qoq_query = textwrap.dedent(target_qoq_query)
    print("TARGET_QUERY")
    print(F"target_qoq_query: {target_qoq_query}")

    ### params과 함께 쿼리문 실행 
    result_df = execute_query(target_qoq_query)
    
    if result_df.shape[0] > 0 :
        print("####################")
        print(f"result_df:{result_df}")
        
        user_query_result_csv_name = f"{data.user_id}_{data.request_time}.csv"
        user_query_result_csv_path = f"{query_result_dir}/{user_query_result_csv_name}"

        ## result df -> csv (SAVE -> s3)
        result_df_csv = result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket, user_query_result_csv_path)
        
        ### 이미지화 + s3에 저장 
        img_key = f"{img_query_dir}/{data.user_id}_{data.request_time}.png"
        img_bytes = plot_table(result_df) 
        save_data_to_s3(img_bytes, bucket, img_key)
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path )
        query_result_img_path = os.path.join(bucket, img_key)
        
        status_flag = True
        response_msg= "SUCCESS"

    else:
        response_msg = "Ticker is not valid"
    #### 통합 로그 request, result

    response_time = datetime.now().strftime('%Y%m%d_%H%M%S')

    user_log_data = UserLogData(
        user_id =  data.user_id, 
        request_time = data.request_time, 
        response_time = response_time, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group,
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path, 
        query_result_img_path = query_result_img_path
    )
    ### add 
    session.add(user_log_data)
    session.commit()
    
    print("LOG DATA UPLOADED")
    
    # user_log_data_dict 그냥 딕셔너리화 시켜야지 
    user_log_data_dict = {
        'user_id': user_log_data.user_id, 
        'request_time':user_log_data.request_time,
        'response_time':user_log_data.response_time,
        'status': status_flag,
        'command':user_log_data.command,
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path, 
        'response_message':response_msg
    }
    
    
    return user_log_data_dict

# vcp rank 
# user_request_vcp/ranking
@app.post('/user_request_vcp/ranking')
def get_sp500_vcp_ranking(data:UserRequestDataDiscord):
    
    target_ranking = data.input_params['ranking']

    target_vcp_ranking_query = f"""
        SELECT DISTINCT
            date,ticker,VCP_rank,
            ROUND(close,2) as close, 
            ROUND(volume,2) as volume, 
            ROUND(MACD_12_26_9,2) as MACD_12_26_9, 
            ROUND(rsi_14,2) as rsi_14, 
            ROUND(atr_14,2) as atr_14, 
            ROUND(volatility,2) as valatility, 
            ROUND(rs,2) as rs
        FROM 
            sp500_vcp 
        WHERE date = (SELECT MAX(date) FROM sp500_vcp)
        ORDER BY CAST(VCP_rank AS SIGNED)
        LIMIT {int(target_ranking)};
    """
    
    print(target_vcp_ranking_query)
    query_result_csv_path,query_result_img_path = "",""
    
    ### 쿼리문 실행 
    result_df = execute_query(target_vcp_ranking_query)
    res_ts = datetime.now().timestamp()
    
    user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
    user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
    
    ### result_df -> csv 
    result_df_csv= result_df.to_csv(index=False)
    save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
    print(f"############################")
    print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
    print(f"{bucket}/{user_query_result_csv_path}")
    
    #### 이미지화 
    img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
    img_file_path = os.path.join(img_query_dir,img_file_name)
    img_bytes = plot_table(result_df)
    save_data_to_s3(img_bytes,bucket, img_file_path)
    print(f"############################")
    print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
    print(f"{bucket}/{img_file_path}")
    
    query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
    query_result_img_path = os.path.join(bucket, img_file_path)
    
    status_flag = True
    response_msg = "SUCCESS"
    # except:
    #     print("FAILED TO PROCESSING ")
    #     status_flag = False
    #     response_msg = "FAILED TO PROCESSING"
    
    print(query_result_csv_path)
    
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg
    }
    
    return user_log_data_dict


### rsi flag_by_date
# user_request_rsi/flag_date_range 
@app.post('/user_request_rsi/column_flag')
def get_rsi_flag_date_range(data:UserRequestDataDiscord):
    
    target_col_flag = data.input_params['column_flag']
    
    if target_col_flag == "bullish":
        target_col = "bullish_divergence"
    else:
        target_col = "bearish_divergence"
    
    target_rsi_flag_query = f"""
        SELECT 
            ticker, date,
            ROUND(close, 2) as close, 
            ROUND(rsi, 2) as rsi , 
            bullish_divergence, 
            bearish_divergence
        FROM 
            sp500_rsi_divergence 
        WHERE {target_col} = 'true' AND 
            date = (SELECT MAX(date) FROM sp500_rsi_divergence);
    """

    query_result_csv_path,query_result_img_path = "",""

    ## 쿼리문 실행 
    result_df = execute_query(target_rsi_flag_query)
    res_ts = datetime.now().timestamp()
    
    if result_df.shape[0]>0:
        
        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")

        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")

        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
    
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    
    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg
    }
    return user_log_data_dict

### wcr 
## user_request_wcr/ranking 
@app.post('/user_request_wcr/ranking')
def get_wcr_ranking(data:UserRequestDataDiscord):
    
    target_ranking = data.input_params['ranking']
    
    target_wcr_ranking = f"""
        SELECT DISTINCT
            date, 
            ticker, 
            wcr_rank, 
            ROUND(wcr, 2) as wcr
        FROM 
            sp500_wcr_dcr
        WHERE date = (SELECT MAX(date) FROM sp500_wcr_dcr)
        ORDER BY CAST(wcr_rank AS SIGNED)
        LIMIT {int(target_ranking)}
    """
    
    query_result_csv_path,query_result_img_path = "",""
    ### 쿼리문 실행 
    result_df = execute_query(target_wcr_ranking)
    res_ts = datetime.now().timestamp()
    
    user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
    user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
    
    ### result_df -> csv 
    result_df_csv= result_df.to_csv(index=False)
    save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
    print(f"############################")
    print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
    print(f"{bucket}/{user_query_result_csv_path}")
    
    #### 이미지화 
    img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
    img_file_path = os.path.join(img_query_dir,img_file_name)
    img_bytes = plot_table(result_df)
    save_data_to_s3(img_bytes,bucket, img_file_path)
    print(f"############################")
    print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
    print(f"{bucket}/{img_file_path}")
    
    query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
    query_result_img_path = os.path.join(bucket, img_file_path)
    
    status_flag = True
    response_msg = "SUCCESS"
    
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    
    
    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id
    }
    
    return user_log_data_dict

#### DCR 
## user_request_dcr/ranking 
@app.post('/user_request_dcr/ranking')
def get_dcr_ranking(data:UserRequestDataDiscord):
    
    target_ranking = data.input_params['ranking']
    
    target_dcr_ranking = f"""
        SELECT DISTINCT 
            date,
            ticker, 
            dcr_rank, 
            ROUND(dcr, 2) as dcr 
        FROM 
            sp500_wcr_dcr
        WHERE date = (SELECT MAX(date) FROM sp500_wcr_dcr)
        ORDER BY CAST(dcr_rank AS SIGNED)
        LIMIT {int(target_ranking)};
    """
    query_result_csv_path,query_result_img_path = "",""
    ### 쿼리문 실행 
    result_df = execute_query(target_dcr_ranking)
    res_ts = datetime.now().timestamp()
    
    user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
    user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
    
    ### result_df -> csv 
    result_df_csv= result_df.to_csv(index=False)
    save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
    print(f"############################")
    print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
    print(f"{bucket}/{user_query_result_csv_path}")
    
    #### 이미지화 
    img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
    img_file_path = os.path.join(img_query_dir,img_file_name)
    img_bytes = plot_table(result_df)
    save_data_to_s3(img_bytes,bucket, img_file_path)
    print(f"############################")
    print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
    print(f"{bucket}/{img_file_path}")
    
    query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
    query_result_img_path = os.path.join(bucket, img_file_path)
    
    status_flag = True
    response_msg = "SUCCESS"
    
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    
    
    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_dcr_ranking
    }
    
    return user_log_data_dict

### HS_PATTERN 
### DAILY_CHECK
### user_request_hs_pattern/daily_check 
@app.post('/user_request_hs_pattern/daily_check')
def get_hs_pattern_found(data:UserRequestDataDiscord):
    
    target_daily_query = f"""
        SELECT DISTINCT 
            date, ticker, hs_pattern_found, hs_signal 
        FROM 
            sp500_hs_pattern 
        WHERE date = (SELECT MAX(date) FROM sp500_hs_pattern)
        AND hs_pattern_found='True'
        ORDER BY ticker;
    """
    query_result_csv_path,query_result_img_path = "",""
    
    result_df = execute_query(target_daily_query)
    res_ts = datetime.now().timestamp()

    if result_df.shape[0]>0:

        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        ### result_df -> csv 
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")
        
        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
        
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_daily_query
    }
    
    return user_log_data_dict


#### sp500_techincal screening daily ranking
# user_request_technical_screening/ranking
@app.post('/user_request_technical_screening/ranking')
def get_tech_screening_ranking(data:UserRequestDataDiscord):
    
    target_ranking = data.input_params['ranking']
    
    target_query = f"""
        SELECT 
            date, ticker, 
            technical_ranking,trend_signal,
            momentum_signal, ROUND(momentum,2) AS momentum,
            relative_strength_signal, ROUND(relative_strength,2) AS relative_strength
        FROM 
            sp500_technical_screening
        WHERE 
            date = (SELECT MAX(date) FROM sp500_technical_screening)
        ORDER BY CAST(technical_ranking AS SIGNED)
        LIMIT {int(target_ranking)};
    """
    
    query_result_csv_path,query_result_img_path = "",""

    ### 쿼리문 실행 
    result_df = execute_query(target_query)
    res_ts = datetime.now().timestamp()
    
    user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
    user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
    
    ### result_df -> csv 
    result_df_csv= result_df.to_csv(index=False)
    save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
    print(f"############################")
    print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
    print(f"{bucket}/{user_query_result_csv_path}")
    
    #### 이미지화 
    img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
    img_file_path = os.path.join(img_query_dir,img_file_name)
    img_bytes = plot_table(result_df)
    save_data_to_s3(img_bytes,bucket, img_file_path)
    print(f"############################")
    print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
    print(f"{bucket}/{img_file_path}")
    
    query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
    query_result_img_path = os.path.join(bucket, img_file_path)
    
    status_flag = True
    response_msg = "SUCCESS"
    
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    
    
    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_query
    }
    
    return user_log_data_dict

#### Pennent_pattern 
### daily_check 
### user_request_pennent_pattern/daily_check 
@app.post("/user_request_pennent_pattern/daily_check")
def get_pennent_pattern_found(data:UserRequestDataDiscord):
    
    target_daily_query = f"""
        SELECT DISTINCT 
           date, ticker, pennant_found,
           high, low, volume 
        FROM 
            sp500_pennent_pattern 
        WHERE date = (SELECT MAX(date) FROM sp500_pennent_pattern)
        AND pennant_found='true'
        ORDER BY ticker;
    """

    query_result_csv_path,query_result_img_path = "",""
    
    result_df = execute_query(target_daily_query)
    res_ts = datetime.now().timestamp()

    if result_df.shape[0]>0:

        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        ### result_df -> csv 
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")
        
        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
        
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_daily_query
    }
    
    return user_log_data_dict


#### wolfe_wave_pattern 
### Daily_check 
### user_request_wolfe_wave_pattern/daily_check
@app.post('/user_request_wolfe_wave_pattern/daily_check')
def get_wolfe_wave_pattern_found(data:UserRequestDataDiscord):
    
    target_daily_query = f"""
        SELECT DISTINCT 
            date, ticker, wolfe_pattern_found 
        FROM 
            sp500_wolfe_wave_pattern
        WHERE date = (SELECT MAX(date) FROM sp500_wolfe_wave_pattern)
        AND wolfe_pattern_found = 'true'
        ORDER BY ticker ; 
    """
    
    query_result_csv_path,query_result_img_path = "",""
    
    result_df = execute_query(target_daily_query)
    res_ts = datetime.now().timestamp()

    if result_df.shape[0]>0:

        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        ### result_df -> csv 
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")
        
        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
        
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_daily_query
    }
    
    return user_log_data_dict

### uptrend_ranking 
### user_request_uptrend_rank/ranking 
@app.post('/user_request_uptrend_rank/ranking')
def get_uptrend_ranking(data:UserRequestDataDiscord):
    
    target_ranking = data.input_params['ranking']
    
    target_query = f"""
        SELECT date, ticker, uptrend_ranking, uptrend, 
            ROUND(uptrend_slope, 2) AS uptrend_slope
        FROM 
            sp500_uptrend_volume_rank
        WHERE date = (SELECT MAX(date) FROM sp500_uptrend_volume_rank)
        ORDER BY uptrend_ranking
        LIMIT {int(target_ranking)};
    """

    query_result_csv_path,query_result_img_path = "",""
    
    result_df = execute_query(target_query)
    res_ts = datetime.now().timestamp()

    if result_df.shape[0]>0:

        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        ### result_df -> csv 
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")
        
        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
        
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_query
    }
    
    return user_log_data_dict

### TR ATR 
### user_request_tr_atr/daily_ticker
@app.post('/user_request_tr_atr/daily_ticker')
def get_tr_atr_ticker(data:UserRequestDataDiscord):
    
    ticker_list = data.input_params['tickers']

    ### ticker_list_valid check 
    valid_ticker_list = []
    valid_ticker_list_query = f"""
        SELECT DISTINCT 
            ticker 
        FROM sp500_tr_atr 
        WHERE date = (SELECT MAX(date) FROM sp500_tr_atr);
    """
    valid_ticker_df = execute_query(valid_ticker_list_query)
    valid_ticker_list = valid_ticker_df['ticker'].unique().tolist()
    
    valid_check_ticker_list = []
    for ticker_obj in ticker_list:
        if ticker_obj in valid_ticker_list:
            valid_check_ticker_list.append(ticker_obj)
    
    valid_ticker_string = ','.join([f"'{ticker}'" for ticker in valid_check_ticker_list])  # 생성된 티커 문자열
    
    print(f'ticker_tuples: {valid_ticker_string}')

    target_query = f"""
        SELECT 
            date, ticker, adj_close, 
            ROUND(tr, 2) as tr, 
            ROUND(atr, 2) as atr,
            tr_rating, 
            atr_rating
        FROM sp500_tr_atr
        WHERE (ticker,date) IN (
            SELECT ticker, MAX(date) as date
            FROM sp500_tr_atr
            WHERE ticker IN ({valid_ticker_string})
            GROUP BY ticker
            )
        ORDER BY atr DESC;
    """

    query_result_csv_path,query_result_img_path = "",""
    
    result_df = execute_query(target_query)
    res_ts = datetime.now().timestamp()

    if result_df.shape[0]>0:

        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        ### result_df -> csv 
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")
        
        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
        
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_query
    }
    
    return user_log_data_dict


### trading_volume_weekly
### user_request_trading_volume/weekly_ranking
@app.post('/user_request_trading_volume/weekly_ranking')
def get_trading_volume_weekly_ranking(data:UserRequestDataDiscord):
    
    target_ranking = data.input_params['ranking']
    
    target_query = f"""
        SELECT 
            date, ticker, weekly_ranking, 
            weekly_trading_volume 
        FROM sp500_trading_volume 
        WHERE date = (SELECT MAX(date) FROM sp500_trading_volume)
        ORDER BY weekly_ranking 
        LIMIT {int(target_ranking)}
    """
    
    query_result_csv_path,query_result_img_path = "",""
    
    result_df = execute_query(target_query)
    res_ts = datetime.now().timestamp()

    if result_df.shape[0]>0:

        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        ### result_df -> csv 
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")
        
        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
        
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_query
    }
    
    return user_log_data_dict


### trading_volume_monthly 
### user_request_trading_volume/monthly_ranking
@app.post('/user_request_trading_volume/monthly_ranking')
def get_trading_volume_monthly_ranking(data:UserRequestDataDiscord):
    
    target_ranking = data.input_params['ranking']
    
    target_query = f"""
        SELECT 
            date, ticker, monthly_ranking,
            monthly_trading_volume 
        FROM sp500_trading_volume 
        WHERE date = (SELECT MAX(date) FROM sp500_trading_volume)
        ORDER BY monthly_ranking
        LIMIT {int(target_ranking)};
    """
    
    query_result_csv_path,query_result_img_path = "",""
    
    result_df = execute_query(target_query)
    res_ts = datetime.now().timestamp()

    if result_df.shape[0]>0:

        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        ### result_df -> csv 
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")
        
        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
        
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_query
    }
    
    return user_log_data_dict

#### relative_strength 
### daily_ranking 
### /user_request_relative_strength/daily_ranking
@app.post('/user_request_relative_strength/daily_ranking')
def get_relative_strength_ranking(data:UserRequestDataDiscord):
    
    target_ranking = data.input_params['ranking']
    
    target_query = f"""
        SELECT date, 
        ticker, ranking_relative_strength,
        ROUND(relative_strength,2) AS relative_strength, 
        ROUND(price_performance, 2) AS price_performance, 
        ROUND(adj_close, 2) AS adj_close
        FROM sp500_relative_strength 
        WHERE date = (SELECT MAX(date) FROM sp500_relative_strength)
        ORDER BY ranking_relative_strength 
        LIMIT {int(target_ranking)};
    """
    query_result_csv_path,query_result_img_path = "",""
    
    result_df = execute_query(target_query)
    res_ts = datetime.now().timestamp()

    if result_df.shape[0]>0:

        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        ### result_df -> csv 
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")
        
        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
        
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_query
    }
    
    return user_log_data_dict

##### quant_metric_02
#### Daily_ticker 
#### /user_request_quant_metric/daily_ticker 
@app.post('/user_request_quant_metric/daily_ticker')
def get_quant_metric_ticker(data:UserRequestDataDiscord):
    ticker_list = data.input_params['tickers']
    
    valid_ticker_list = []
    valid_ticker_list_query = f"""
        SELECT DISTINCT
            ticker 
        FROM sp500_quant_metric_02
        WHERE date = (SELECT MAX(date) FROM sp500_quant_metric_02)
    """
    valid_ticker_df = execute_query(valid_ticker_list_query)
    valid_ticker_list = valid_ticker_df['ticker'].tolist()
    
    valid_check_ticker_list = []
    for ticker_obj in ticker_list:
        if ticker_obj in valid_ticker_list:
            valid_check_ticker_list.append(ticker_obj)
            
    valid_ticker_string = ','.join([f"'{ticker}'" for ticker in valid_check_ticker_list])  # 생성된 티커 문자열

    target_query = f"""
        SELECT date, ticker,
            on_balance_volume, 
            ROUND(klinger_oscillator,2) as klinger_oscillator,
            ROUND(volume_price_trend,2) as volume_price_trend, 
            ROUND(chaikin_money_flow,2) as chaikin_money_flow, 
            ROUND(ease_of_movement,2) as ease_of_movement, 
            ROUND(money_flow_index,2) as money_flow_index, 
            ROUND(accumulation_distribution_line,2) as accumulation_distribution_line
        FROM sp500_quant_metric_02 
        WHERE date = (SELECT MAX(date) FROM sp500_quant_metric_02)
        AND ticker IN ({valid_ticker_string})
        ORDER BY ticker;
    """
    query_result_csv_path,query_result_img_path = "",""
    
    result_df = execute_query(target_query)
    res_ts = datetime.now().timestamp()

    if result_df.shape[0]>0:

        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        ### result_df -> csv 
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")
        
        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
        
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_query
    }
    
    return user_log_data_dict

##### option_analysis 
### daily_ticker 
### user_request_option_analysis/daily_ticker
@app.post('/user_request_option_analysis/daily_ticker')
def get_option_analysis_ticker(data:UserRequestDataDiscord):
    ticker_list = data.input_params['tickers']
    
    valid_ticker_list = []
    valid_ticker_list_query = f"""
        SELECT DISTINCT 
            ticker
        FROM sp500_option_analysis
        WHERE date = (SELECT MAX(date) FROM sp500_option_analysis);
    """
    valid_ticker_df = execute_query(valid_ticker_list_query)
    valid_ticker_list = valid_ticker_df['ticker'].tolist()
    
    valid_check_ticker_list = []
    for ticker_obj in ticker_list:
        if ticker_obj in valid_ticker_list:
            valid_check_ticker_list.append(ticker_obj)
            
    valid_ticker_string = ','.join([f"'{ticker}'" for ticker in valid_check_ticker_list])  # 생성된 티커 문자열

    target_query = f"""
        SELECT date, ticker, 
            openInterest,
            ROUND(impliedVolatility,2) as implied_volatility,
            option_type, 
            ROUND(vol_oi_ratio,2) as vol_oi_ratio,
            ROUND(put_call_ratio,2) as put_call_ratio,
            ROUND(iv_change,2) as iv_change,
            ROUND(Close,2) AS close
        FROM
            sp500_option_analysis 
        WHERE date = (SELECT MAX(date) FROM sp500_option_analysis)
        AND ticker IN ('NVDA','TSLA','AAPL','NVR')
        ORDER BY ticker;
    """
    query_result_csv_path,query_result_img_path = "",""
    
    result_df = execute_query(target_query)
    res_ts = datetime.now().timestamp()

    if result_df.shape[0]>0:

        user_query_result_csv_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.csv"
        user_query_result_csv_path = os.path.join(query_result_dir,user_query_result_csv_name)
        
        ### result_df -> csv 
        result_df_csv= result_df.to_csv(index=False)
        save_data_to_s3(result_df_csv, bucket,user_query_result_csv_path)
        print(f"############################")
        print(f"USER QUERY CSV RESULT {data.command} is UPLOADED")
        print(f"{bucket}/{user_query_result_csv_path}")
        
        #### 이미지화 
        img_file_name = f"{data.user_id}_{data.request_time}_{res_ts}_{data.command}.png"
        img_file_path = os.path.join(img_query_dir,img_file_name)
        img_bytes = plot_table(result_df)
        save_data_to_s3(img_bytes,bucket, img_file_path)
        print(f"############################")
        print(f"USER QUERY RESULT PNG FILE  {data.command} is UPLOADED")
        print(f"{bucket}/{img_file_path}")
        
        query_result_csv_path = os.path.join(bucket,user_query_result_csv_path)
        query_result_img_path = os.path.join(bucket, img_file_path)
        
        status_flag = True
        response_msg = "SUCCESS"
        
    else:
        status_flag = False
        response_msg = "THERE IS NO RESULTS"
        
    user_log_data = UserLogData(
        user_id = data.user_id, 
        request_time = data.request_time, 
        response_time = res_ts, 
        status = status_flag, 
        command = data.command, 
        command_group = data.command_group, 
        command_type = data.command_type,
        input_params = str(data.input_params),
        target_query_key_path = "",
        query_result_csv_path = query_result_csv_path,
        query_result_img_path = query_result_img_path
    )
    ### add to log table 
    session.add(user_log_data)
    session.commit()
    print("USER LOG DATA UPLOADED")
    session.refresh(user_log_data)
    log_id = user_log_data.log_id

    ### return_dict 
    user_log_data_dict = {
        'user_id':user_log_data.user_id, 
        'request_time':user_log_data.request_time, 
        'response_time':user_log_data.response_time,
        'status':status_flag, 
        'command':user_log_data.command, 
        'command_group':user_log_data.command_group,
        'command_type':user_log_data.command_type,
        'query_result_csv_path':user_log_data.query_result_csv_path,
        'query_result_img_path':user_log_data.query_result_img_path,
        'response_message':response_msg,
        'log_id': log_id, 
        'target_query':target_query
    }
    
    return user_log_data_dict

### SQS 
sqs_url = ""
# user_request_sqs/sqs_test
@app.post('user_request_sqs/sqs_test')
def process_sqs_message():
    sqs_client = boto3.client('sqs',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    sqs_response = sqs_client.receive_message(
        QueueUrl = sqs_url, 
        MaxNumberOfMessages=1, 
        WaitTimeSeconds=10
    )
    if 'Messages' in sqs_response:
        sqs_req_msg = sqs_response['Messages'][0]
        sqs_req_payload = json.loads(sqs_response)

   
        target_url = sqs_req_payload['rest_url']
        
        payload = sqs_req_payload
        del payload['rest_url']
        del payload['sqs_url']
        
        ### request 
        response = requests.post(target_url, json=payload)  
        return 
    


##### UTILS
#### 쿼리 실행 
def execute_query(sql_query):
    try:
        result_df = pd.read_sql_query(sql_query, engine)
        return result_df
    except Exception as e:
        print(f"Error occurred while executing query:{e}")
        return None
    
def execute_query_params(sql_query, params):
    try:
        result_df = pd.read_sql_query(sql_query, engine, params)
        return result_df
    except Exception as e:
        print(f"Error occurred while executing query:{e}")
        return None


def save_df_to_s3(df_data, bucket, key):
    csv_data = df_data.to_csv(index=False)
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3.Object(bucket, key).put(Body=csv_data)

def save_data_to_s3(data, bucket, key):
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3.Object(bucket, key).put(Body=data)

### 테이블 형태로 이미지로 출력하는 함수 
def plot_table(df):
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(df.columns)),
        cells=dict(values=[df[col] for col in df.columns])
    )])

    fig.update_layout(width=800, height=500)
    
    ## 이미지 바이트로 출력 
    image_bytes = fig.to_image(format='png')
    
    return image_bytes

def format_number(number):
    if number >= 1e9:
        return f"{number/1e9:.2f}B"
    elif number >= 1e6:
        return f"{number/1e6:.2f}M"
    elif number >= 1e3:
        return f"{number/1e3:.2f}k"
    else:
        return str(number)

