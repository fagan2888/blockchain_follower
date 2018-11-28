import asyncio
import uvloop

import smoked_pool
import config
import db
import time

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def main():
      global pool
      print('Setting up DB...')
      blockchain_db = db.BlockchainDB()
      await blockchain_db.init_db_schema()

      last_block = await blockchain_db.get_last_block()

      if last_block == None:
         await blockchain_db.init_db_schema()
         print('KKKKKHHHHHHAAAAAANNNNNNNNN - importing genesis block...')
         await blockchain_db.import_genesis()
         print(str(await blockchain_db.get_last_block()))

      # we repeat here for sanity reasons
      last_db_block = await blockchain_db.get_last_block()
      print('Last block in DB is block number %s, ID %s' % (last_db_block['block_num'], last_db_block['block_id']))


      print('Querying blockchain state...')
      blockchain_state = await pool.query('get_dynamic_global_properties',[])

      print('Last block in blockchain is block number %s, ID %s' % (blockchain_state['head_block_number'], blockchain_state['head_block_id']))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    pool = smoked_pool.smoked_pool(config.smoked_urls)
    pool.start_tasks(loop)

    loop.run_until_complete(main())
