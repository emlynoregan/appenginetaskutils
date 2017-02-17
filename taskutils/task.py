import pickle
import yccloudpickle
from google.appengine.api import taskqueue
from google.appengine.ext import db
import webapp2
from google.appengine.ext import webapp
import flask
import logging
import functools

_DEFAULT_FLASK_URL = "/_ah/task/<name>"
_DEFAULT_WEBAPP_URL = "/_ah/task/(.*)"
_DEFAULT_ENQUEUE_URL = "/_ah/task/%s"

_TASKQUEUE_HEADERS = {"Content-Type": "application/octet-stream"}

class Error(Exception):
    """Base class for exceptions in this module."""

class PermanentTaskFailure(Error):
    """Indicates that a task failed, and will never succeed."""

class RetryTaskException(Error):
    """Indicates that task needs to be retried."""

class _TaskToRun(db.Model):
    """Datastore representation of a deferred task.
    
    This is used in cases when the deferred task is too big to be included as
    payload with the task queue entry.
    """
    data = db.BlobProperty(required=True) # up to 1 mb

def _run(data):
    """Unpickles and executes a task.
    
    Args:
      data: A pickled tuple of (function, args, kwargs) to execute.
    Returns:
      The return value of the function invocation.
    """
    try:
        func, args, kwargs = pickle.loads(data)
    except Exception, e:
        raise PermanentTaskFailure(e)
    else:
        func(*args, **kwargs)

def _run_from_datastore(key):
    """Retrieves a task from the datastore and executes it.
    
    Args:
      key: The datastore key of a _DeferredTaskEntity storing the task.
    Returns:
      The return value of the function invocation.
    """
    entity = _TaskToRun.get(key)
    if entity:
        try:
            _run(entity.data)
        except PermanentTaskFailure:
            entity.delete()
            raise
        else:
            entity.delete()

def task(f=None, *taskargs, **taskkwargs):
    if not f:
        return functools.partial(task, *taskargs, **taskkwargs)
    
    taskkwargscopy = dict(taskkwargs)
    
    def GetAndDelete(name, default = None):
        retval = default
        if name in taskkwargscopy:
            retval = taskkwargscopy[name]
            del taskkwargscopy[name]
        return retval
    queue = GetAndDelete("queue", "default")
    transactional = GetAndDelete("transactional", False)
    parent = GetAndDelete("parent")

    taskkwargscopy["headers"] = dict(_TASKQUEUE_HEADERS)

    funcname = taskkwargscopy.get("name", f.__name__)
    funcname = funcname if funcname else "unnamed"

    taskkwargscopy["url"] = _DEFAULT_ENQUEUE_URL % funcname
    
    logging.info(taskkwargscopy)

    @functools.wraps(f)    
    def runtask(*args, **kwargs):
        pickled = yccloudpickle.dumps((f, args, kwargs))
        try:
            task = taskqueue.Task(payload=pickled, *taskargs, **taskkwargscopy)
            return task.add(queue, transactional=transactional)
        except taskqueue.TaskTooLargeError:
            key = _TaskToRun(data=pickled, parent=parent).put()
            rfspickled = yccloudpickle.dumps((_run_from_datastore, [key], {}))
            task = taskqueue.Task(payload=rfspickled, *taskargs, **taskkwargscopy)
            return task.add(queue, transactional=transactional)
    return runtask

def _launch_task(pickled, name):
    try:
        logging.debug('before run "%s"' % name)
        _run(pickled)
        logging.debug('after run "%s"' % name)
    except PermanentTaskFailure:
        logging.exception("Aborting task")
    except:
        logging.exception("failure")
        raise
    # else let exceptions escape and cause a retry

class TaskHandler(webapp.RequestHandler):
    def post(self, name):
        _launch_task(self.request.body, name)

class TaskHandler2(webapp2.RequestHandler):
    def post(self, name):
        _launch_task(self.request.body, name)

def addrouteforwebapp(routes):
    routes.append((_DEFAULT_WEBAPP_URL, TaskHandler))

def addrouteforwebapp2(routes):
    routes.append((_DEFAULT_WEBAPP_URL, TaskHandler2))
    
def setuptasksforflask(flaskapp):
    @flaskapp.route(_DEFAULT_FLASK_URL, methods=["POST"])
    def taskhandler(name):
        _launch_task(flask.request.data, name)
        return ""


