import json

import config
import asyncio
import websockets

class smoked_instance:
   def __init__(self,smoked_url):
       self.ws = websocket.WebSocket()
       self.ws.connect(smoked_url)
       self.req_id = 1
   async def get_status(self):
         """ We want to ensure that each instance is both connected and in sync etc
         """
         await self.assure_connected()
         return {'head_block':1,'active':True,'in_sync':True}
   async def assure_connected(self):
       if not self.ws.open:
          await self.ws.connect(smoked_url)
          self.req_id = 1
   async def query(self,method,params,request_api=None):
       await self.assure_connected()
       if request_api == None:
          req = {'id':self.req_id,'method':method,'params':params,'jsonrpc':'2.0'}
       else:
          req = {'id':self.req_id,'method':'call','params':[request_api,method,params],'jsonrpc':'2.0'}
       req = json.dumps(req)
       self.req_id += 1
       try:
          await ws.send(req)
       except:
          pass
       retval = await ws.recv()
       return json.loads(retval)
   async def ping(self):
       await self.assure_connected()
       try: 
          await self.ws.ping()
       except:
          pass

class smoked_pool:
   def __init__(self,smoked_urls):
       self.active_smoked_instances = [] # currently active instances that are responding correctly
       self.temp_failed_instances   = [] # instances that have timed out or returned errors once, if they return an error again they die
       self.dead_instances          = [] # if we have no live instances or temp failed instances, we try to do necromancy on these
   async def query(self,method,params,request_api=None):
       pass

