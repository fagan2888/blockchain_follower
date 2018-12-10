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
      # TODO - start at a particular block height
      last_db_block = await blockchain_db.get_last_block()
      print('Last block in DB is block number %s, ID %s' % (last_db_block['block_num'], last_db_block['block_id']))

      print('Querying pool status...')
      pool_status = await pool.pool_state()
      pprint.pprint(pool_status)

      highest_block = 1
      for instance in pool_status:
          if instance['active'] and instance['head_block'] > highest_block: highest_block=instance['head_block']


      last_block = await pool.query('get_block',[highest_block])
      print('Last block in blockchain is block number %s, ID %s' % (highest_block, last_block['block_id']))


      if highest_block > last_db_block['block_num']:
         print('importing block into DB...')
         last_block = await pool.query('get_block',[last_db_block['block_num']+1])
         async with blockchain_db.begin_tx() as (conn,tx):
               await blockchain_db.insert_block(conn=conn,block_num=last_db_block['block_num']+1,block_data=last_block)
               block_transaction_id_offs = 0
               for block_transaction in last_block['transactions']:
                   try:
                      await blockchain_db.insert_transaction(conn=conn,tx_data={'transaction_id':  last_block['transaction_ids'][block_transaction_id_offs],
                                                                                'block_num':       last_db_block['block_num']+1,
                                                                                'ref_block_num':   block_transaction['ref_block_num'],
                                                                                'ref_block_prefix':block_transaction['ref_block_prefix'],
                                                                                'expiration':      block_transaction['expiration'],
                                                                                'operations':      block_transaction['operations'],
                                                                                'signatures':      block_transaction['signatures']})
                      block_transaction_id_offs += 1
                   except Exception as e:
                      print(e)
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
