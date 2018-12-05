import config
import asyncio
import binascii
import datetime
import json
import pprint
from async_generator import async_generator, yield_, asynccontextmanager

from sqlalchemy_aio import ASYNCIO_STRATEGY

from sqlalchemy import Column,Integer,MetaData,Table,Text,String,Binary,DateTime,ForeignKey,Numeric, create_engine, select
from sqlalchemy.schema import CreateTable,DropTable

class BlockchainDB:
   def __init__(self,db_conn_string='sqlite:///blockchain.db'):
       self.db_engine = create_engine(db_conn_string, strategy=ASYNCIO_STRATEGY)
       self.metadata = MetaData()
       self.metadata.bind = self.db_engine
       self.blocks_table = Table('blocks',self.metadata,
                                 Column('block_num',Integer,primary_key=True),
				 Column('previous', Binary(20)),
				 Column('timestamp',DateTime),
				 Column('witness',  String(16)),
				 Column('tx_merkle_root', Binary(20)),
				 Column('extensions', Text), # this should be changed if the extensions field is ever actually used
                                 Column('witness_signature', Binary(72)),
				 Column('signing_key', String(40)),
				 Column('block_id', Binary('20')))
       self.transactions_table = Table('transactions', self.metadata,
                                       Column('transaction_id', Binary(20),primary_key=True),
				       Column('block_num', Integer, ForeignKey('blocks.block_num')),
                                       Column('ref_block_num', Integer, ForeignKey('blocks.block_num')),
                                       Column('ref_block_prefix',Integer),
                                       Column('expiration',DateTime))
       self.tx_sigs_table = Table('transaction_sigs', self.metadata,
                                  Column('transaction_id', Binary(20), ForeignKey('transactions.transaction_id')),
                                  Column('signature', Binary(72)))
       self.tx_ops_table  = Table('transaction_ops', self.metadata,
                                  Column('op_id',Integer,primary_key=True, autoincrement=True), 
                                  Column('transaction_id',Binary(20),ForeignKey('transactions.transaction_id')),
                                  Column('op_type',String(16)),
                                  Column('raw_json',Text))
       self.ops_votes_table = Table('ops_votes', self.metadata,
                                    Column('op_id', Integer, ForeignKey('transaction_ops.op_id')),
                                    Column('voter', String(16)),
                                    Column('author', String(16)),
                                    Column('permlink', String(512)),
                                    Column('weight', Integer))
       self.accounts_table = Table('accounts', self.metadata,
                                   Column('account_name', String(16), primary_key=True),
                                   Column('creator',      String(16)),
                                   Column('memo_key',     String(40)),
                                   Column('json_meta',    Text),
                                   Column('smoke_balance',Numeric),
                                   Column('smoke_power_balance',Numeric))
       self.accounts_auths_table = Table('accounts_auths', self.metadata,
                                         Column('account_name', String(16), ForeignKey('accounts.account_name')),
                                         Column('auth_class',    String(16)), # owner/active/posting etc
                                         Column('pub_key',       String(40)), # either a public key or a null string
                                         Column('authed_user',   String(16)), # either a username or a null string
                                         Column('weight',        Integer))

   async def init_db_schema(self):
       """ Setup the initial table structure etc in the DB
       """
       for t in self.metadata.sorted_tables:
           has_t = await self.db_engine.has_table(t.name)
           if not has_t: await self.db_engine.execute(CreateTable(t))
   async def reset_db(self):
       """ Wipe all the data from the DB
       """
       for t in self.metadata.sorted_tables:
           has_t = await self.db_engine.has_table(t.name)
           if has_t: await self.db_engine.execute(DropTable(t))
       await self.init_db_schema()
   @asynccontextmanager
   @async_generator
   async def begin_tx(self):
       """ Start a transaction, use it like this:
           with tx as db.begin_tx():
                do_stuff()
                tx.commit()
       """ 
       async with self.db_engine.connect() as conn:
             tx = await conn.begin()
             try:
                 await yield_((conn,tx))
             finally:
                 pass
   async def import_genesis(self):
       """ Import the geneis block (it has no transactions in it, but we need to reference it in future blocks)
       """
       async with self.begin_tx() as (conn,tx):
             await self.insert_block(conn=conn,block_num=1,block_data={"previous"               :"0000000000000000000000000000000000000000",
                                                                       "timestamp"              :"2018-09-23T12:40:09",
                                                                       "witness"                :"initminer",
                                                                       "transaction_merkle_root":"0000000000000000000000000000000000000000",
                                                                       "extensions"             :[],
                                                                       "witness_signature"      :"2059a90db752a0f9c43da5c786b4bf2ba9f4ce8cb93078dcb404152b4206fc4c2c3d760e3dc9e95f15afe6e7558757a8caf04d7f96d3c875026d32df45f2bcf4b1",
                                                                       "transactions"           :[],
                                                                       "block_id"               :"000000019c05133c542cce1150290bdcc49880c2",
                                                                       "signing_key"            :"SMK619jJm3VKrHRLbKaAkFXSCUBFwwv9d4yuTYM9KT6cjJV6zno1G",
                                                                       "transaction_ids"        :[]})
             await tx.commit()
   async def insert_block(self,conn=None,tx=None,block_num=None,block_data={}):
       """ Insert a block into the DB
       """
       await conn.execute(self.blocks_table.insert().values(block_num         = block_num,
                                                            previous          = binascii.unhexlify(block_data['previous']),
                                                            timestamp         = datetime.datetime.strptime(block_data['timestamp'],'%Y-%m-%dT%H:%M:%S'), 
                                                            witness           = block_data['witness'],
                                                            tx_merkle_root    = binascii.unhexlify(block_data['transaction_merkle_root']),
                                                            witness_signature = binascii.unhexlify(block_data['witness_signature']),
                                                            block_id          = binascii.unhexlify(block_data['block_id']),
                                                            signing_key       = block_data['signing_key']))
   async def insert_transaction(self,conn=None,tx_data={}):
       """ Insert a transaction into the DB
       """
       print(tx_data)
       await conn.execute(self.transactions_table.insert().values(transaction_id   = binascii.unhexlify(tx_data['transaction_id']),
                                                                  block_num        = tx_data['block_num'],
                                                                  ref_block_num    = tx_data['ref_block_num'],
                                                                  ref_block_prefix = tx_data['ref_block_prefix'],
                                                                  expiration       = datetime.datetime.strptime(tx_data['expiration'],'%Y-%m-%dT%H:%M:%S')))
       for sig in tx_data['signatures']:
           await conn.execute(self.tx_sigs_table.insert().values(transaction_id = binascii.unhexlify(tx_data['transaction_id']),
                                                                 signature      = binascii.unhexlify(sig)))
       for op in tx_data['operations']:
           op_id = await conn.execute(self.tx_ops_table.insert().values(transaction_id = binascii.unhexlify(tx_data['transaction_id']),
                                                                        op_type        = op[0],
                                                                        raw_json       = json.dumps(op[1])))
           pprint.pprint(op)
           if op[0]=='vote':
              await conn.execute(self.ops_votes_table.insert().values(op_id=op_id.inserted_primary_key,**(op[1])))
           elif op[0]=='account_create':
              await conn.execute(self.accounts_table.insert().values(account_name = op[1]['new_account_name'],
                                                                     creator      = op[1]['creator'],
                                                                     memo_key     = op[1]['memo_key'],
                                                                     json_meta    = "{}"))
              for auth_class in ['owner','active','posting']:
                  for acc in op[1][auth_class]['account_auths']:
                      await conn.execute(self.accounts_auths_table.insert().values(account_name = op[1]['new_account_name'],
                                                                                   auth_class   = auth_class,
                                                                                   authed_user  = acc[0],
                                                                                   weight       = acc[1]))
                  for key in op[1][auth_class]['key_auths']:
                      await conn.execute(self.accounts_auths_table.insert().values(account_name = op[1]['new_account_name'],
                                                                                   auth_class   = auth_class,
                                                                                   pub_key      = key[0],
                                                                                   weight       = key[1]))


   async def get_last_block(self):
       """ Get the last block that was inserted into the DB
       """
       result    = None
       db_result = None
       async with self.db_engine.connect() as conn:
             query = select([self.blocks_table]).order_by(self.blocks_table.c.block_num.desc()).limit(1)
             result = await conn.execute(query)
             db_result = await result.fetchone()
       if db_result == None: return None
       result = {'block_num'              :db_result['block_num'],
                 'previous'               :binascii.hexlify(db_result['previous']).decode('utf-8'),
                 'timestamp'              :datetime.datetime.strftime(db_result['timestamp'],'%Y-%m-%dT%H:%M:%S'),
                 'witness'                :db_result['witness'],
                 'transaction_merkle_root':binascii.hexlify(db_result['tx_merkle_root']).decode('utf-8'),
                 'extensions'             :[],
                 'witness_signature'      :binascii.hexlify(db_result['witness_signature']).decode('utf-8'),
                 'transactions'           :[],
                 'block_id'               :binascii.hexlify(db_result['block_id']).decode('utf-8'),
                 'signing_key'            :db_result['signing_key'],
                 'transaction_ids'        :[]}
       return result
