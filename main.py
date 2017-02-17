import logging

from flask import Flask
from taskutils import setuptasksforflask
from handlers.switchboard import get_switchboard
from handlers.report import get_report

app = Flask(__name__)

setuptasksforflask(app)

get_switchboard(app)
get_report(app)

@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    return 'An internal error occurred.', 500