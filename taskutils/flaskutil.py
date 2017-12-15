import flask
from taskutils.task import _launch_task, get_taskroute


_DEFAULT_FLASK_URL = "/_ah/task/<fmodule>/<ffunction>"
_DEFAULT_FLASK_URL2 = "/_ah/task/<fname>"

def setuptasksforflask(flaskapp):
    @flaskapp.route("%s/<fmodule>/<ffunction>" % get_taskroute(), methods=["POST"])
    def taskhandler(fmodule, ffunction):
        _launch_task(flask.request.data, "%s/%s" % (fmodule, ffunction), flask.request.headers)
        return ""

    @flaskapp.route("%s/<fname>" % get_taskroute(), methods=["POST"])
    def taskhandler2(fname):
        _launch_task(flask.request.data, fname, flask.request.headers)
        return ""

