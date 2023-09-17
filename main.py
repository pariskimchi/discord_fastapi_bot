import asyncio
import uvicorn
import threading

from discord_bot import bot 
from fastapi_app import app 

### discord_bot 
bot_token = ""

### discord bot 실행 function
async def run_discord_bot():
    await bot.start(bot_token)

if __name__ == "__main__":
    

    bot_thread = threading.Thread(target=asyncio.run, args=(run_discord_bot(),))
    bot_thread.start()
    
#     ### FASTAPI 앱 실행 
    uvicorn.run(app, host='127.0.0.1', port=8000)
    
    # discord_bot 중지
    bot.loop.run_until_complete(bot.logout())  # 봇 로그아웃
    bot.loop.close()  # 이벤트 루프 닫기
    bot_thread.join()
    fastapi_thread = threading.Thread(target=uvicorn.run, args=(app, "127.0.0.1", 8000))
    fastapi_thread.start()



