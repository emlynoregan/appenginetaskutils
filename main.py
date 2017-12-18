import logging

import flask
from taskutils import setuptasksforflask, set_logging, set_dump, set_taskroute #, get_taskroute
# from taskutils.task import _launch_task
from handlers.switchboard import get_switchboard
from handlers.report import get_report

set_logging(True)
set_dump(True)
set_taskroute("/customtask")

app = flask.Flask(__name__)

setuptasksforflask(app)

get_switchboard(app)
get_report(app)

@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    return 'An internal error occurred.', 500
