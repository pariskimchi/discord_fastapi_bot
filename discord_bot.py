
from unittest import async_case
import discord 
from discord.ext import commands 
import csv
import boto3 
import pandas as pd 
import io
from io import StringIO, BytesIO
from datetime import datetime
import requests 
import os
from pydantic import BaseModel
import json 

import interactions
aws_access_key_id, aws_secret_access_key= "",""

api_url = ""
sqs_url_from_discord=""

CHANNEL_ID= ""
MESSAGE = ""
bot_token = ""

intents = discord.Intents.default()
client = discord.Client(command_prefix='!',
                        intents=intents)
bot = commands.Bot(command_prefix='>',intents=intents)
# bot = interactions.Client(token=bot_token)

sqs_client = boto3.client("sqs",aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)


@bot.slash_command(name='vcp_rank', description = "Top rank companies by vcp_pattern_index")
async def get_query_vcp_rank(ctx:commands.Context, 
                             until_ranking:int):
    
    ## user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "ranking"
    add_url = f"user_request_vcp/{sub_add_url}"
    target_url = api_url + add_url
    # user_request_vcp/ranking
    # slash_command_name 
    command_group = "sp500_vcp"
    command_type = "ranking"
    slash_command_name = "get_query_vcp_rank"
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    ts_now = datetime.now().timestamp()

    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    ## until_rank -> input_params 
    payload['input_params']['ranking'] = until_ranking
    
    ### payload를 request로 요청 
    response = requests.post(target_url, json=payload)
    
    if response.status_code == 200:
        ### return_data 받기 
        response_data = response.json()
        with open(f"response_data_{response_data['response_time']}.json","w", encoding='utf-8-sig') as json_f:
            json.dump(response_data, json_f, indent=4)
            
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### response_data로부터 정보 읽기 
            query_result_csv_path = response_data['query_result_csv_path']
            query_result_img_path = response_data['query_result_img_path']
            s3_bucket, img_dir, img_name = query_result_img_path.split("/")
            s3_bucket, csv_dir, csv_name = query_result_csv_path.split("/")
            
            s3_img_key = os.path.join(img_dir, img_name)
            s3_csv_key = os.path.join(csv_dir, csv_name)
            
            s3_client = boto3.client('s3',
                                    aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key) 
            s3_img_obj = s3_client.get_object(
                Bucket=s3_bucket, 
                Key=s3_img_key
            )
            s3_csv_obj = s3_client.get_object(
                Bucket=s3_bucket, 
                Key=s3_csv_key
            )
            ### img_bytes 
            img_bytes = s3_img_obj['Body'].read()
            s3_csv_content = s3_csv_obj['Body'].read()
            
            embed_description = command_group + command_type + str(until_ranking)
            ### embed 설정
            embed = discord.Embed(
                title='Query result image',
                description=f'{embed_description}',
                color = discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            print(f"{response_msg}")

            await ctx.send(embed=embed,
                           file = discord.File(io.BytesIO(img_bytes),
                                               filename='image.png'))
            
        else:
            await ctx.send(f"{response_msg}")
        
    else:
        await ctx.send('Failed to Process to Request') 
        
        
### RSI bullish_true, range () 
@bot.slash_command(name='rsi_divergence_signal',
                   description="Find ticker's data of bullish signal")
async def rsi_divergence(ctx:commands.Context, bullish_or_bearish:str="bullish"):

    #user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "column_flag"
    add_url = f"user_request_rsi/{sub_add_url}"
    target_url = api_url + add_url

    command_group = "sp500_rsi"
    command_type=  "column_flag"
    slash_command_name = "rsi_divergence"
    request_ts = datetime.now().timestamp()
    
    ### bullish_bearish 
    valid_bullish_or_bearish_list = ["bullish","bearish"]
    if bullish_or_bearish not in valid_bullish_or_bearish_list:
        await ctx.send(f"Invalid bullish_or_bearish value. Please choose between {', '.join(valid_bullish_or_bearish_list)}")
        return 
    ## date range 
    # valid_date_range_list = ["1m","3m","6m","1y"]
    # if date_range not in valid_date_range_list:
    #     await ctx.send(f"Invalid date range. Please choose among {', '.join(valid_date_range_list)}")
    #     return 
    
    payload = {
        'user_id':user_id, 
        'request_time':request_ts, 
        'command':slash_command_name,
        'command_group':command_group, 
        'command_type':command_type,
        'input_params': dict()
    }
    
    ### params 
    payload['input_params']['column_flag'] = bullish_or_bearish
    
    #### payload를 request로 -> FASTAPI로 요청 
    response = requests.post(target_url, json=payload)
    
    if response.status_code == 200:
        ### return_data 받기 
        response_data = response.json()
        with open(f"response_data_{response_data['response_time']}.json","w",encoding='utf-8-sig') as json_f:
            json.dump(response_data, json_f, indent=4)
            
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ## response_data로부터 정보 읽기
            query_result_csv_path = response_data['query_result_csv_path']
            query_result_img_path = response_data['query_result_img_path']
            
            s3_bucket, img_dir, img_name = query_result_img_path.split("/")
            s3_bucket, csv_dir, csv_name = query_result_csv_path.split("/")
            
            s3_key = os.path.join(img_dir, img_name)
            s3_csv_key = os.path.join(csv_dir, csv_name)
            s3_client = boto3.client('s3',
                                    aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
            # try:
            s3_object = s3_client.get_object(
                Bucket=s3_bucket, 
                Key=s3_key
            )
            s3_csv_object = s3_client.get_object(
                Bucket=s3_bucket, 
                Key=s3_csv_key
            )
            
            image_bytes = s3_object['Body'].read()

            s3_csv_content = s3_csv_object['Body'].read()
            # df = pd.read_csv(io.BytesIO(s3_csv_content))
            embed = discord.Embed(
                title='Query result image',
                description=f'Rsi Divergence',
                color = discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            print(f"{response_msg}")
            
            await ctx.send(embed=embed,file=discord.File(io.BytesIO(image_bytes), filename='image.png'))
            
            # except Exception as e:
        #         await ctx.send('Failed to retrieve the image')
        else:
            await ctx.send(f"{response_msg}")
        
        
    else:
        await ctx.send('Failed to Process to Request') 

### wcr 
@bot.slash_command(name='wcr_rank', description='Daily Ranking of tickers by Working Capital Ratio')
async def get_query_wcr_rank(ctx:commands.Context, 
                             until_ranking:int):
    # user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "ranking"
    add_url = f"user_request_wcr/{sub_add_url}"
    target_url = api_url + add_url
    
    # command_group 
    command_group = "sp500_wcr_dcr"
    command_type = "ranking"
    slash_command_name = "get_query_wcr_rank"
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()
    
    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    
    payload['input_params']['ranking'] = until_ranking
    
    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)

    if response.status_code == 200:
        response_data = response.json()
        ## return_data 받기 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Top Ranking of WSR', 
                description='Working Capital Ratio',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")

### DCR
@bot.slash_command(name='dcr_rank', description='Daily Ranking of tickers by Debt Coverage Ratio')
async def get_query_dcr_rank(ctx:commands.Context, 
                             until_ranking:int):
    # user_id 
    user_id = ctx.author.id 
    sub_add_url = "ranking"
    add_url = f"user_request_dcr/{sub_add_url}"
    target_url = api_url + add_url
    
    # command_group 
    command_group = "sp500_wcr_dcr"
    command_type = "ranking"
    slash_command_name = "get_query_dcr_rank"
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()

    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    payload['input_params']['ranking'] = until_ranking
    
    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)
    
    if response.status_code == 200:
        response_data = response.json()
        ## return_data 받기 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Top Ranking of DCR', 
                description='Debt Coverage Ratio',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")

### HS_pattern 
@bot.slash_command(name='hs_pattern_signal', description='Daily ticker list of HS pattern signal')
async def get_query_hs_pattern(ctx:commands.Context):
    
    # user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "daily_check"
    add_url = f"user_request_hs_pattern/{sub_add_url}"
    target_url = api_url + add_url
    
    command_group = "sp500_hs_pattern"
    command_type = "daily_check"
    slash_command_name = "hs_pattern_signal"
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()
    
    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)

    if response.status_code == 200:
        response_data = response.json() 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Daily HS Pattern Found Ticker', 
                description='HS Pattern',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")

#### sp500_techincal_screening 
### daily_ranking 
@bot.slash_command(name='technical_screening_ranking', 
                   description='sp500 technical screening daily ranking by techincal indicators')
async def get_query_tech_screening_rank(ctx:commands.Context, 
                                        until_ranking:int):
    # user_id 
    user_id = ctx.author.id 
    sub_add_url = "ranking"
    add_url = f"user_request_technical_screening/{sub_add_url}"
    target_url = api_url + add_url 
    
    ## command_group 
    command_group = "sp500_technical_screening"
    command_type = "ranking"
    slash_command_name = "get_query_tech_screening_rank"
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()

    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    payload['input_params']['ranking'] = until_ranking

    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)
    
    if response.status_code == 200:
        response_data = response.json()
        ## return_data 받기 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Daily Top Rank of Techincal Screening', 
                description='daily ranking by techincal screening',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")


### pennent_pattern_signal 
@bot.slash_command(name='pennent_pattern_signal', 
                   description='Daily ticker list of pennent pattern signal')
async def get_query_pennent_pattern(ctx:commands.Context):
    
    # user_id 
    user_id = ctx.author.id 
    sub_add_url = "daily_check"
    add_url = f"user_request_pennent_pattern/{sub_add_url}"
    target_url = api_url + add_url 
    command_group = "sp500_pennent_pattern"
    command_type = "daily_check"
    slash_command_name = "pennent_pattern_signal"
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()
    
    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)

    if response.status_code == 200:
        response_data = response.json() 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Daily pennent pattern Found Ticker', 
                description='pennent pattern',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")

#### wolfe_wave_pattern_signal 
@bot.slash_command(name='wolfe_wave_pattern_signal', 
                   description='Daily ticker list of wolfe wave pattern signal')
async def get_query_wolfe_wave_pattern(ctx:commands.Context):
    
    # user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "daily_check"
    add_url = f"user_request_wolfe_wave_pattern/{sub_add_url}"
    target_url = api_url + add_url 
    ### user_request_wolfe_wave_pattern/daily_check
    
    command_group = "sp500_wolfe_wave_pattern"
    command_type = "daily_check"
    slash_command_name = "wolfe_wave_pattern_signal"
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()
    
    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)

    if response.status_code == 200:
        response_data = response.json() 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Daily wolfe wave pattern found Ticker', 
                description='wolfe wave pattern',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")

### uptrend_rank
### DAILY RANKING
@bot.slash_command(name='uptrend_rank', 
                   description='Daily sp500 uptrend_ranking')
async def get_query_uptrend_rank(ctx:commands.Context, 
                                 until_ranking:int):
    
    # user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "ranking"
    add_url = f"user_request_uptrend_rank/{sub_add_url}"
    target_url = api_url + add_url 
    
    ### command_group 
    command_group = "sp500_uptrend_rank"
    command_type = "ranking"
    slash_command_name = "get_query_uptrend_rank"
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()

    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    payload['input_params']['ranking'] = until_ranking
    
    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)
    
    
    if response.status_code == 200:
        response_data = response.json()
        ## return_data 받기 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Daily Top Rank of Uptrend ', 
                description='daily ranking by Uptrend',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")

#### TR_ATR 
### daily_show 
@bot.slash_command(name='tr_atr_ticker',
                   description='Daily check TR(true range) and ATR(average true range) indicator of the ticker')
async def get_query_tr_atr_ticker(ctx:commands.Context, 
                                  tickers:str):
    ### user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "daily_ticker"
    add_url = f"user_request_tr_atr/{sub_add_url}"
    target_url = api_url + add_url 
    
    command_group = "sp500_tr_atr"
    command_type = "daily_ticker"
    slash_command_name = "get_query_tr_atr_ticker"
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()
    
    ticker_input_text = tickers 
    if "," in ticker_input_text:
        ticker_list = ticker_input_text.split(",")
        ticker_list = [i.strip() for i in ticker_list]
    else:
        ticker_list = [tickers]
    
    
    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    
    payload['input_params']['tickers'] = ticker_list

    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)
    

    if response.status_code == 200:
        response_data = response.json()
        ## return_data 받기 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Daily TR and ATR of tickers ', 
                description='daily TR ATR',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")


#### trading_volume_weekly 
#### weekly_ranking 
@bot.slash_command(name='trading_volume_rank_weekly',
                   description='sp500 trading_volume weekly ranking')
async def get_query_trading_volume_weekly(ctx:commands.Context, 
                                          until_ranking:int):
    # user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "weekly_ranking"
    add_url = f"user_request_trading_volume/{sub_add_url}"
    target_url = api_url + add_url
    
    ### command_group 
    command_group = "sp500_trading_volume"
    command_type = "ranking"
    slash_command_name = "get_query_trading_volume_weekly"
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()

    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    payload['input_params']['ranking'] = until_ranking
    
    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)
    
    
    if response.status_code == 200:
        response_data = response.json()
        ## return_data 받기 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Weekly Top Rank of Trading Volume ', 
                description='Weekly ranking of Trading Volume',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
            await ctx.send(file=discord.File(io.BytesIO(s3_csv_content), filename='result_ranking.csv'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")

    

### trading_volume_monthly
#### monthly_ranking
@bot.slash_command(name='trading_volume_rank_monthly',
                   description='sp500 trading_volume monthly ranking')
async def get_query_trading_volume_monthly(ctx:commands.Context,
                                           until_ranking:int):
    ## user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "monthly_ranking"
    add_url = f"user_request_trading_volume/{sub_add_url}"
    target_url = api_url + add_url 
    
    ### command_group 
    command_group = "sp500_trading_volume"
    command_type = "ranking"
    slash_command_name = "get_query_trading_volume_monthly"
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()

    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    payload['input_params']['ranking'] = until_ranking
    
    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)
    
    
    if response.status_code == 200:
        response_data = response.json()
        ## return_data 받기 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Weekly Top Rank of Trading Volume ', 
                description='Weekly ranking of Trading Volume',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")

### relative_strength
### Daily_ranking 
@bot.slash_command(name='relative_strength_ranking', 
                   description='sp500 relative strength ranking')
async def get_query_relative_strength_rank(ctx:commands.Context, 
                                           until_ranking:int):
    # user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "daily_ranking"
    add_url = f"user_request_relative_strength/{sub_add_url}"
    target_url = api_url + add_url

    ### command_group 
    command_group = "sp500_relative_strength"
    command_type = 'ranking'
    slash_command_name= "get_query_relative_strength_rank"
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()

    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    payload['input_params']['ranking'] = until_ranking
    
    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)
    
    
    if response.status_code == 200:
        response_data = response.json()
        ## return_data 받기 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Daily Top Rank of Relative Strength ', 
                description='Daily ranking of Relative Strength',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")

### quant_metric_02
### daily show_ ticker 
@bot.slash_command(name='quant_metric_ticker', 
                   description='Daily quant metric of Selected ticker')
async def get_query_quant_metric_ticker(ctx:commands.Context,
                                        tickers:str):
    ### user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "daily_ticker"
    add_url = f"user_request_quant_metric/{sub_add_url}"
    target_url = api_url + add_url

    command_group = "sp500_quant_metric_02"
    command_type = "daily_ticker"
    slash_command_name = "get_query_quant_metric_ticker"
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()
    ticker_input_text = tickers 
    if "," in ticker_input_text:
        ticker_list = ticker_input_text.split(",")
        ticker_list = [i.strip() for i in ticker_list]
    else:
        ticker_list = [tickers]
    
    
    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    
    payload['input_params']['tickers'] = ticker_list

    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)
    
    
    if response.status_code == 200:
        response_data = response.json()
        ## return_data 받기 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Daily Quant metric indicators of tickers ', 
                description='daily quant metric',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")


### option_analysis 
### daily_show_ticker 
@bot.slash_command(name='option_analysis_ticker', 
                   description='daily sp500 option analysis indicators of selected ticker')
async def get_query_option_analysis_ticker(ctx:commands.Context, 
                                           tickers:str):
    ### user_id 
    user_id = ctx.author.id 
    
    sub_add_url = "daily_ticker"
    add_url = f"user_request_option_analysis/{sub_add_url}"
    target_url = api_url+add_url

    command_group = "sp500_option_analysis"
    command_type = "daily_ticker"
    slash_command_name = "get_query_option_analysis_ticker"
    

    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()
    ticker_input_text = tickers 
    if "," in ticker_input_text:
        ticker_list = ticker_input_text.split(",")
        ticker_list = [i.strip() for i in ticker_list]
    else:
        ticker_list = [tickers]
    
    
    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict()
    }
    
    payload['input_params']['tickers'] = ticker_list

    #### SQS 
    # sqs_client.send_message(
    #     QueueUrl = sqs_url,
    #     MessageBody=json.dumps(payload)
    # )
    
    ## payload -> request -> FASTAPI
    response = requests.post(target_url, json=payload)
    
    
    if response.status_code == 200:
        response_data = response.json()
        ## return_data 받기 
        process_response_save_log(response_data)
        
        response_status = response_data['status']
        response_msg = response_data['response_message']
        
        if response_status:
            ### 쿼리 결과 csv 불러오기
            s3_csv_content = get_result_csv_from_s3(response_data)            
            ### 쿼리 결과 이미지 불러오기
            s3_img_content = get_result_img_from_s3(response_data)

            ##embed 
            embed = discord.Embed(
                title='Daily sp500 option analysis indicators of tickers ', 
                description='daily sp500 option analysis',
                color=discord.Color.blue()
            )
            embed.set_image(url=f"attachment://image.png")
            
            await ctx.send(embed=embed, 
                           file=discord.File(io.BytesIO(s3_img_content), filename='image.png'))
        else:
            await ctx.send(f"{response_msg}")
    else:
        await ctx.send("Failed to process your request.")



@bot.slash_command(name='sqs_test', 
                   description='sqs_test')
async def sqs_test_bot(ctx:commands.Context, 
                                           tickers:str):
    ### user_id 
    user_id = ctx.author.id 
    
    # user_request_sqs/sqs_test
    sub_add_url = "sqs_test"
    add_url = f"user_request_sqs/{sub_add_url}"
    target_url = api_url+add_url

    command_group = "sqs_test"
    command_type = "test"
    slash_command_name = "sqs_test"
    
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')[2:]
    ts_now = datetime.now().timestamp()
    ticker_input_text = tickers 
    if "," in ticker_input_text:
        ticker_list = ticker_input_text.split(",")
        ticker_list = [i.strip() for i in ticker_list]
    else:
        ticker_list = [tickers]
    
    
    payload = {
        'user_id':user_id, 
        'request_time':timestamp_str, 
        'command':slash_command_name, 
        'command_group':command_group, 
        'command_type':command_type, 
        'input_params': dict(),
        'rest_url':target_url,
        'sqs_url':sqs_url
    }
    
    payload['input_params']['tickers'] = ticker_list

    sqs_client.send_message(
        QueueUrl= sqs_url, 
        MessageBody = json.dumps(payload)
    )
    
    
    
    
    return


### UTIL 
def process_response_save_log(response_data):
    response_ts = response_data['response_time']
    user_id = response_data['user_id']
    log_id = response_data['log_id']
    ## ts -> 
    # response_date = datetime.fromtimestamp(response_ts).strftime('%Y%m%d')[2:]
    # save log as json 
    with open(f"response_data_{user_id}_{log_id}.json","w", encoding='utf-8-sig') as json_f:
        json.dump(response_data, json_f, indent=4)
    

def get_result_csv_from_s3(response_data):
    """
        쿼리 결과 csv 를 s3로부터 가져오기
    """
    query_result_csv_path = response_data['query_result_csv_path']
    
    s3_bucket, csv_dir, csv_name = query_result_csv_path.split("/")
    s3_csv_key = os.path.join(csv_dir, csv_name)
    
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)
    ### 가져오기 
    s3_csv_object = s3_client.get_object(
                Bucket=s3_bucket, 
                Key=s3_csv_key)
    
    ### 읽기 
    s3_csv_content = s3_csv_object['Body'].read()
    
    return s3_csv_content



def get_result_img_from_s3(response_data):
    """
        쿼리 결과 이미지 s3로부터 가져오기
    """
    query_result_img_path = response_data['query_result_img_path']
    s3_bucket, img_dir, img_name = query_result_img_path.split("/")

    s3_img_key = os.path.join(img_dir, img_name)

    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    s3_img_object = s3_client.get_object(
                Bucket=s3_bucket, 
                Key=s3_img_key
            )

    image_bytes = s3_img_object['Body'].read()
    
    return image_bytes


### discord_bot process_msg 


