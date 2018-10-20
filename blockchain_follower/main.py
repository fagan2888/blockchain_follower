import asyncio
import uvloop

import smoked_pool
import config
import db

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def main():
      print('Setting up DB...')
      blockchain_db = db.BlockchainDB()
      await blockchain_db.init_db_schema()
      print('KKKKKHHHHHHAAAAAANNNNNNNNN - importing genesis block...')
      await blockchain_db.import_genesis()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
