from __future__ import absolute_import

import taskutils.task as taskpy
from taskutils.task import task, addrouteforwebapp, addrouteforwebapp2, setuptasksforflask
from taskutils.sharded import shardedpagemap, shardedmap, futureshardedpagemap, futureshardedmap, futureshardedpagemapwithcount, futureshardedmapwithcount 
from taskutils.future import future, FutureTimedOutError, FutureReadyForResult, FutureNotReadyForResult, _Future, get_children

from flask import Flask

app = Flask(__name__)

setuptasksforflask(app)
