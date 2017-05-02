from __future__ import absolute_import

import taskutils.task as taskpy
from taskutils.task import task, addrouteforwebapp, addrouteforwebapp2, setuptasksforflask
from taskutils.ndbsharded import ndbshardedpagemap, ndbshardedmap, futurendbshardedpagemap, futurendbshardedmap, futurendbshardedpagemapwithcount, futurendbshardedmapwithcount 
from taskutils.gcsfilesharded import gcsfileshardedpagemap, gcsfileshardedmap, futuregcsfileshardedpagemap, futuregcsfileshardedmap
from taskutils.future import future, FutureTimedOutError, FutureReadyForResult, FutureNotReadyForResult, _Future, get_children, twostagefuture

from flask import Flask

app = Flask(__name__)

setuptasksforflask(app)
