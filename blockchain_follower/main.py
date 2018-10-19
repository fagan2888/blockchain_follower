import asyncio
import uvloop

import smoked_pool
import config

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
