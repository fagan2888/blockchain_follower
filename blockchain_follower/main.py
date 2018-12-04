import asyncio
import uvloop

import smoked_pool
import config
import json
import db
import time
import pprint

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def iter_loop(pool,blockchain_db):
      last_db_block = await blockchain_db.get_last_block()
      print('Last block in DB is block number %s, ID %s' % (last_db_block['block_num'], last_db_block['block_id']))

      print('Querying pool status...')
      pool_status = await pool.pool_state()
      pprint.pprint(pool_status)

      highest_block = 1
      for instance in pool_status:
          if instance['active'] and instance['head_block'] > highest_block: highest_block=instance['head_block']

      print('Querying blockchain state...')
      blockchain_state = await pool.query('get_dynamic_global_properties',[])


      last_block = await pool.query('get_block',[highest_block])
      pprint.pprint(last_block)
      print('Last block in blockchain is block number %s, ID %s' % (highest_block, last_block['block_id']))

      # TODO - do the magic here (grab the latest block that isn't in DB, start transaction, import all the data, continue)

      if highest_block > last_db_block['block_num']:
         print('importing block into DB...')
         last_block = await pool.query('get_block',[last_db_block['block_num']+1])
         async with blockchain_db.begin_tx() as (conn,tx):
               await blockchain_db.insert_block(conn=conn,block_num=last_db_block['block_num']+1,block_data=last_block)
               await tx.commit()
      else:
        await asyncio.sleep(3)


async def main():
      global pool
      global blockchain_db
      print('Setting up DB...')
      blockchain_db = db.BlockchainDB()
      await blockchain_db.init_db_schema()

      last_block = await blockchain_db.get_last_block()

      if last_block == None:
         await blockchain_db.init_db_schema()
         print('KKKKKHHHHHHAAAAAANNNNNNNNN - importing genesis block...')
         await blockchain_db.import_genesis()
         print(str(await blockchain_db.get_last_block()))

      while True:
         await iter_loop(pool,blockchain_db)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    pool = smoked_pool.smoked_pool(config.smoked_urls)
    pool.start_tasks(loop)

    loop.run_until_complete(main())
