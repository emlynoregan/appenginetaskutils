from __future__ import absolute_import

from taskutils.task import task, addrouteforwebapp, addrouteforwebapp2, setuptasksforflask
from taskutils.sharded import shardedmap, futureshardedmap
from taskutils.future import future, FutureTimedOutError, FutureUnderwayError, _Future, DefaultUpdateResultF, get_children

from flask import Flask

app = Flask(__name__)

setuptasksforflask(app)
