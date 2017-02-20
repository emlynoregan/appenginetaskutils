import pickle
import yccloudpickle
from google.appengine.api import taskqueue
from google.appengine.ext import db
import webapp2
from google.appengine.ext import webapp
import flask
import logging
import functools

_DEFAULT_FLASK_URL = "/_ah/task/<fmodule>/<ffunction>"
_DEFAULT_WEBAPP_URL = "/_ah/task/(.*)"
_DEFAULT_ENQUEUE_URL = "/_ah/task/%s/%s"

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

def _run(data, headers):
    """Unpickles and executes a task.
    
    Args:
      data: A pickled tuple of (function, args, kwargs) to execute.
    Returns:
      The return value of the function invocation.
    """
    try:
        func, args, kwargs, passthroughargs = pickle.loads(data)
    except Exception, e:
        raise PermanentTaskFailure(e)
    else:
        if passthroughargs.get("includeheaders"):
            kwargs["headers"] = headers
        func(*args, **kwargs)

def _run_from_datastore(key, headers):
    """Retrieves a task from the datastore and executes it.
    
    Args:
      key: The datastore key of a _DeferredTaskEntity storing the task.
    Returns:
      The return value of the function invocation.
    """
    entity = _TaskToRun.get(key)
    if entity:
        try:
            _run(entity.data, headers)
        except PermanentTaskFailure:
            entity.delete()
            raise
        else:
            entity.delete()

def task(f=None, **taskkwargs):
    if not f:
        return functools.partial(task, **taskkwargs)
    
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
    includeheaders = GetAndDelete("includeheaders", False)

    taskkwargscopy["headers"] = dict(_TASKQUEUE_HEADERS)

    url = _DEFAULT_ENQUEUE_URL % (getattr(f, '__module__', 'none'), getattr(f, '__name__', 'none'))
    
    taskkwargscopy["url"] = url.lower()
    
    logging.info(taskkwargscopy)

    passthroughargs = {
        "includeheaders": includeheaders
    }

    @functools.wraps(f)    
    def runtask(*args, **kwargs):
        pickled = yccloudpickle.dumps((f, args, kwargs, passthroughargs))
        try:
            task = taskqueue.Task(payload=pickled, **taskkwargscopy)
            return task.add(queue, transactional=transactional)
        except taskqueue.TaskTooLargeError:
            key = _TaskToRun(data=pickled, parent=parent).put()
            rfspickled = yccloudpickle.dumps((_run_from_datastore, [key], {}))
            task = taskqueue.Task(payload=rfspickled, **taskkwargscopy)
            return task.add(queue, transactional=transactional)
    return runtask

def isFromTaskQueue(headers):
    """ Check if we are currently running from a task queue """
    # As stated in the doc (https://developers.google.com/appengine/docs/python/taskqueue/overview-push#Task_Request_Headers)
    # These headers are set internally by Google App Engine.
    # If your request handler finds any of these headers, it can trust that the request is a Task Queue request.
    # If any of the above headers are present in an external user request to your App, they are stripped.
    # The exception being requests from logged in administrators of the application, who are allowed to set the headers for testing purposes.
    return bool(headers.get('X-Appengine-TaskName'))

# Queue & task name are already set in the request log.
# We don't care about country and name-space.
_SKIP_HEADERS = {'x-appengine-country', 'x-appengine-queuename', 'x-appengine-taskname', 'x-appengine-current-namespace'}
    
def _launch_task(pickled, name, headers):
    try:
        # Add some task debug information.
        dheaders = []
        for key, value in headers.items():
            k = key.lower()
            if k.startswith("x-appengine-") and k not in _SKIP_HEADERS:
                dheaders.append("%s:%s" % (key, value))
        logging.debug(", ".join(dheaders))
        
        if not isFromTaskQueue(headers):
            raise PermanentTaskFailure('Detected an attempted XSRF attack: we are not executing from a task queue.')

        logging.debug('before run "%s"' % name)
        _run(pickled, headers)
        logging.debug('after run "%s"' % name)
    except PermanentTaskFailure:
        logging.exception("Aborting task")
    except:
        logging.exception("failure")
        raise
    # else let exceptions escape and cause a retry

class TaskHandler(webapp.RequestHandler):
    def post(self, name):
        _launch_task(self.request.body, name, self.request.headers)

class TaskHandler2(webapp2.RequestHandler):
    def post(self, name):
        _launch_task(self.request.body, name, self.request.headers)

def addrouteforwebapp(routes):
    routes.append((_DEFAULT_WEBAPP_URL, TaskHandler))

def addrouteforwebapp2(routes):
    routes.append((_DEFAULT_WEBAPP_URL, TaskHandler2))
    
def setuptasksforflask(flaskapp):
    @flaskapp.route(_DEFAULT_FLASK_URL, methods=["POST"])
    def taskhandler(fmodule, ffunction):
        _launch_task(flask.request.data, "%s/%s" % (fmodule, ffunction), flask.request.headers)
        return ""


