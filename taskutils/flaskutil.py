import flask
from taskutils.task import _launch_task

_DEFAULT_FLASK_URL = "/_ah/task/<fmodule>/<ffunction>"
_DEFAULT_FLASK_URL2 = "/_ah/task/<fname>"

def setuptasksforflask(flaskapp):
    @flaskapp.route(_DEFAULT_FLASK_URL, methods=["POST"])
    def taskhandler(fmodule, ffunction):
        _launch_task(flask.request.data, "%s/%s" % (fmodule, ffunction), flask.request.headers)
        return ""

    @flaskapp.route(_DEFAULT_FLASK_URL2, methods=["POST"])
    def taskhandler2(fname):
        _launch_task(flask.request.data, fname, flask.request.headers)
        return ""

