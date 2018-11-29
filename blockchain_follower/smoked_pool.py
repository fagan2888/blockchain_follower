import json

import config
import asyncio
import websockets

# TODO - add querying only active nodes, downgrading to failed and dead as appropriate
# TODO - add querying for status across all active nodes, using async properly

class smoked_instance:
   def __init__(self,smoked_url):
       self.url = smoked_url
       loop = asyncio.get_event_loop()
       self.ws = None
       loop.run_until_complete(asyncio.ensure_future(self.assure_connected()))
   async def get_status(self):
         """ We want to ensure that each instance is both connected and in sync etc
         """
         await self.assure_connected()
         blockchain_state = await self.query('get_dynamic_global_properties',[])

         return {'head_block':blockchain_state['head_block_number'],'active':True}
   async def connect(self):
       self.ws = await websockets.client.connect(self.url)
       self.req_id = 1
   async def assure_connected(self):
       if self.ws is None: await self.connect()
       if not self.ws.open: await self.connect()
   async def query(self,method,params,request_api=None):
         # TODO - add proper error handling here
         await self.assure_connected()
         if request_api == None:
            req = json.dumps({'id':self.req_id,'method':method,'params':params,'jsonrpc':'2.0'})
            await self.ws.send(req)
            resp = await self.ws.recv()
            retval = json.loads(resp)['result']
         else:
            req = json.dumps({"id":self.req_id,"method":"call","params":[request_api,method,params],'jsonrpc':'2.0'})
            await self.ws.send(req)
            resp = await self.ws.recv()
            retval = json.loads(resp)['result']
         self.req_id += 1
         return retval
   async def ping(self):
       await self.assure_connected()
       retval = True
       try: 
          await self.ws.ping()
       except:
          retval = False
       return retval

class smoked_pool:
   def __init__(self,smoked_urls):
       self.smoked_instances        = {} # all configured smoked instances
       self.active_smoked_instances = {} # currently active instances that are responding correctly
       self.temp_failed_instances   = {} # instances that have timed out or returned errors once, if they return an error again they die
       self.dead_instances          = {} # if we have no live instances or temp failed instances, we try to do necromancy on these
       for url in smoked_urls:
           self.smoked_instances[url] = smoked_instance(url)

   async def test_smoked_instance(self, url):
       while True:
          instance = self.smoked_instances[url]
          if await instance.ping():
             self.active_smoked_instances[url] = instance
          else:
             del self.active_smoked_instances[url]
             self.dead_instances[url] = instance
          await asyncio.sleep(60)
   
   def start_tasks(self, async_loop):
       for k,v in self.smoked_instances.items():
           async_loop.create_task(self.test_smoked_instance(k))

   async def pool_state(self):
         retval = {}
         for k,v in self.active_smoked_instances.items():
             retval[k] = await v.get_status()
         return retval
   async def query(self,method,params,request_api=None):
       for k,v in self.active_smoked_instances.items():
           # TODO - run these queries using full async goodness
           # TODO - error handling here
           retval = await v.query(method,params,request_api)
           if retval != None:
              return retval








