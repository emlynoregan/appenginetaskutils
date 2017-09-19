from __future__ import absolute_import

import taskutils.task as taskpy
from taskutils.task import task, addrouteforwebapp, addrouteforwebapp2
from taskutils.flaskutil import setuptasksforflask

from flask import Flask

app = Flask(__name__)

setuptasksforflask(app)
