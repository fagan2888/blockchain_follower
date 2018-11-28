import json

import config
import asyncio
import websocket

# TODO - add querying only active nodes, downgrading to failed and dead as appropriate
# TODO - add querying for status across all active nodes, using async properly

class smoked_instance:
   def __init__(self,smoked_url):
#       self.ws = websocket.WebSocket()
#       self.ws.connect(smoked_url)
#       self.req_id = 1
       self.url = smoked_url
   async def get_status(self):
         """ We want to ensure that each instance is both connected and in sync etc
         """
         await self.assure_connected()
         return {'head_block':1,'active':True,'in_sync':True}
   async def assure_connected(self):
         pass
#       if not self.ws.open:
#          await self.ws.connect(smoked_url)
#          self.req_id = 1
   async def query(self,method,params,request_api=None):

         ws = websocket.WebSocket()
         ws.connect(self.url)
         if request_api == None:
            req = json.dumps({'id':1,'method':method,'params':params,'jsonrpc':'2.0'})
            ws.send(req)
            resp = ws.recv()
            print(resp)
            retval = json.loads(resp)['result']
         else:
            req = json.dumps({"id":3,"method":"call","params":[request_api,method,params],'jsonrpc':'2.0'})
            ws.send(req)
            resp = ws.recv()
            print(resp)
            retval = json.loads(resp)['result']
         ws.shutdown()
         return retval




   async def ping(self):
        return True
#       await self.assure_connected()
#       retval = True
#       try: 
#          await self.ws.ping()
#       except:
#          retval = False
#       return retval

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

   async def query(self,method,params,request_api=None):
       for k,v in self.active_smoked_instances.items():
           # TODO - run these queries using full async goodness
           # TODO - error handling here
           retval = await v.query(method,params,request_api)
           if retval != None:
              return retval








