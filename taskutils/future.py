from google.appengine.ext import ndb
import functools
from taskutils import task
from yccloudpickle import yccloudpickle
import pickle
import datetime
import logging
import uuid
import json
from taskutils.task import PermanentTaskFailure
import hashlib
from google.appengine.api import taskqueue

class FutureReadyForResult(Exception):
    pass

class FutureNotReadyForResult(Exception):
    pass

class FutureTimedOutError(Exception):
    pass

class FutureCancelled(Exception):
    pass

class _FutureProgress(ndb.model.Model):
    progress = ndb.IntegerProperty()
    weight = ndb.IntegerProperty()

class _Future(ndb.model.Model):
    stored = ndb.DateTimeProperty(auto_now_add = True)
    updated = ndb.DateTimeProperty(auto_now = True)
    parentkey = ndb.KeyProperty()
    resultser = ndb.BlobProperty()
    exceptionser = ndb.BlobProperty()
    onsuccessfser = ndb.BlobProperty()
    onfailurefser = ndb.BlobProperty()
    onprogressfser = ndb.BlobProperty()
    taskkwargsser = ndb.BlobProperty()
    status = ndb.StringProperty()
    runtimesec = ndb.FloatProperty()
    readyforresult = ndb.BooleanProperty()
    timeoutsec = ndb.IntegerProperty()
    name = ndb.StringProperty()

    def get_taskkwargs(self, deletename = True):
        taskkwargs = pickle.loads(self.taskkwargsser)
        
        if deletename and "name" in taskkwargs:
            del taskkwargs["name"]
            
        return taskkwargs
    
    def has_result(self):
        return bool(self.status)
    
    def get_result(self):
        if self.status == "failure":
            raise pickle.loads(self.exceptionser)
        elif self.status == "success":
            return pickle.loads(self.resultser)
        else:
            raise FutureReadyForResult("result not ready")

    def _get_progressobject(self):
        key = ndb.Key(_FutureProgress, self.key.id())
        progressobj = key.get()
        if not progressobj:
            progressobj = _FutureProgress(key = key)
        return progressobj
    
    def get_progress(self, progressobj = None):
        progressobj = progressobj if progressobj else self._get_progressobject()
        return progressobj.progress if progressobj and progressobj.progress else 0

    def get_weight(self, progressobj = None):
        progressobj = progressobj if progressobj else self._get_progressobject()
        return progressobj.weight if progressobj and progressobj.weight else 1
        
    def update_result(self):
        if self.readyforresult:
            updateresultf = UpdateResultF #pickle.loads(self.updateresultfser) if self.updateresultfser else DefaultUpdateResultF
            updateresultf(self)
            
            # note that updateresultf can change the status
    
            if self.status == "failure":
                self._callOnFailure()
            elif self.status == "success":
                self._callOnSuccess()
                
    def _callOnSuccess(self):
        onsuccessf = pickle.loads(self.onsuccessfser) if self.onsuccessfser else None
        if onsuccessf:
            onsuccessf(self)
            
    def _callOnFailure(self):
        onfailuref = pickle.loads(self.onfailurefser) if self.onfailurefser else None
        if onfailuref:
            onfailuref(self)
                
    def _callOnProgress(self):
        onprogressf = pickle.loads(self.onprogressfser) if self.onprogressfser else None
        if onprogressf:
            onprogressf(self)
        else:
            OnProgressF(self)
            
    def get_runtime(self):
        if self.runtimesec:
            return datetime.timedelta(seconds = self.runtimesec)
        else:
            return datetime.datetime.utcnow() - self.stored             

    @ndb.non_transactional
    def set_success(self, result):
        selfkey = self.key
        @ndb.transactional
        def set_status_transactional():
            self = selfkey.get()
            didput = False
            if self.readyforresult and not self.status:
                self.status = "success"
                self.readyforresult = True
                self.resultser = yccloudpickle.dumps(result)
                self.runtimesec = self.get_runtime().total_seconds()
                didput = True
                self.put()
            return self, didput
        self, needcalls = set_status_transactional()
        if needcalls:
            self.set_progress(self.get_weight())
            self._callOnSuccess()

    @ndb.non_transactional
    def set_failure(self, exception):
        selfkey = self.key
        @ndb.transactional
        def set_status_transactional():
            self = selfkey.get()
            didput = False
            if not self.status:
                self.status = "failure"
                self.readyforresult = True
                self.exceptionser = yccloudpickle.dumps(exception)
                self.runtimesec = self.get_runtime().total_seconds()
                didput = True
                self.put()
            return self, didput
        self, needcalls = set_status_transactional()
        if needcalls:
            self._callOnFailure()
            
    @ndb.non_transactional
    def set_success_and_readyforesult(self, result):
        selfkey = self.key
        @ndb.transactional
        def set_status_transactional():
            self = selfkey.get()
            didput = False
            if not self.status:
                self.status = "success"
                self.readyforresult = True
                self.resultser = yccloudpickle.dumps(result)
                self.runtimesec = self.get_runtime().total_seconds()
                didput = True
                self.put()
            return self, didput
        self, needcalls = set_status_transactional()
        if needcalls:
            self.set_progress(self.get_weight())
            self._callOnSuccess()

    @ndb.non_transactional
    def set_readyforesult(self):
        selfkey = self.key
        @ndb.transactional
        def set_status_transactional():
            self = selfkey.get()
            didput = False
            if not self.readyforresult:
                self.readyforresult = True
                didput = True
                self.put()
            return self, didput
        self, needcalls = set_status_transactional()
        if needcalls:
            self.update_result()

    def set_progress(self, value):
        progressobj = self._get_progressobject()
        needcalls = False
        if progressobj.progress != value:
            progressobj.progress = value
            needcalls = True
            progressobj.put()
        if needcalls:
            self._callOnProgress()

    def set_weight(self, value):
        progressobj = self._get_progressobject()
        if progressobj.weight != value:
            progressobj.weight = value
            progressobj.put()
            
    def cancel(self):
        children = get_children(self.key)
        if children:
            taskkwargs = self.get_taskkwargs()
            
            @task(**taskkwargs)
            def cancelchild(child):
                child.cancel()
                
            for child in children:
                cancelchild(child)

        self.set_failure(FutureCancelled("cancelled by caller"))

    def to_dict(self, level=0, maxlevel = 5, recursive = True):
#         if not self.has_result():
#             self.update_result()

        progressobj = self._get_progressobject()
                     
        children = [child.to_dict(level = level + 1) for child in get_children(self.key)] if recursive and level+1 < maxlevel else None
        
        resultrep = None
        result = pickle.loads(self.resultser) if self.resultser else None
        if not result is None:
            try:
                resultrep = result.to_dict()
            except:
                try:
                    json.dumps(result)
                    resultrep = result
                except:
                    resultrep = str(result)
        
        return {
            "key": str(self.key) if self.key else None,
            "name": self.name,
            "level": level,
            "stored": str(self.stored) if self.stored else None,
            "updated": str(self.updated) if self.stored else None,
            "status": str(self.status) if self.status else "underway",
            "result": resultrep,
            "exception": str(pickle.loads(self.exceptionser)) if self.exceptionser else None,
            "runtimesec": self.get_runtime().total_seconds(),
            "progress": self.get_progress(progressobj),
            "weight": self.get_weight() if self.get_weight() else 0,
            "readyforresult": self.readyforresult,
            "zchildren": children
        }
        
def UpdateResultF(futureobj):
    if not futureobj.status and futureobj.get_runtime() > datetime.timedelta(seconds = futureobj.timeoutsec):
        futureobj.set_failure(FutureTimedOutError("timeout"))

    taskkwargs = futureobj.get_taskkwargs()

    @task(**taskkwargs)
    def UpdateChildren():
        for childfuture in get_children(futureobj.key):
#             logging.debug("update_result: %s" % childfuture.key)
            childfuture.update_result()
    UpdateChildren()

def OnProgressF(futureobj):
    if futureobj.parentkey:
        taskkwargs = futureobj.get_taskkwargs()
      
    #     logging.debug("Enter OnProgressF: %s" % futureobj)
        @task(**taskkwargs)
        def UpdateParent(parentkey):
    #         logging.debug("***************************************************")
    #         logging.debug("Enter UpdateParent: %s" % parentkey)
    #         logging.debug("***************************************************")
    
            parent = parentkey.get()
    #         logging.debug("1: %s" % parent)
            if parent:
    #             logging.debug("2")
                if parent.has_result():
                    progress = parent.get_weight()
                else:
                    progress = 0
                    for childfuture in get_children(parentkey):
    #                     logging.debug("3: %s" % childfuture)
                        progress += childfuture.get_progress()
    #             logging.debug("4: %s" % (progress))
                parent.set_progress(progress)
    
        UpdateParent(futureobj.parentkey)

def get_children(futurekey):
    if futurekey:
        ancestorkey = ndb.Key(futurekey.kind(), futurekey.id())
        return [childfuture for childfuture in _Future.query(ancestor=ancestorkey) if ancestorkey == childfuture.key.parent()]
    else:
        return []

def GenerateStableId(instring):
    return hashlib.md5(instring).hexdigest()

def future(f=None, parentkey=None, includefuturekey=False, 
           onsuccessf=None, onfailuref=None, onprogressf=None, weight = 1, timeoutsec = 1800, maxretries = None, futurename = None, **taskkwargs):
    
    if not f:
        return functools.partial(future, 
            parentkey=parentkey, includefuturekey=includefuturekey, 
            onsuccessf=onsuccessf, onfailuref=onfailuref, onprogressf=onprogressf, weight = weight, timeoutsec = timeoutsec, maxretries = maxretries, futurename = futurename,
            **taskkwargs)
    
#     logging.debug("includefuturekey: %s" % includefuturekey)
    
    @ndb.transactional
    @functools.wraps(f)
    def runfuture(*args, **kwargs):
#         logging.debug("runfuture: parentkey=%s" % parentkey)

        immediateancestorkey = ndb.Key(parentkey.kind(), parentkey.id()) if parentkey else None

        taskkwargscopy = dict(taskkwargs)
        if not "name" in taskkwargscopy:
            # can only set transactional if we're not naming the task
            taskkwargscopy["transactional"] = True
            newfutureId = str(uuid.uuid4()) # id doesn't need to be stable
        else:
            # if we're using a named task, we need the key to remain stable in case of transactional retries
            # what can happen is that the task is launched, but the transaction doesn't commit. 
            # retries will then always fail to launch the task because it is already launched.
            # therefore retries need to use the same future key id, so that once this transaction does commit,
            # the earlier launch of the task will match up with it.
            taskkwargscopy["transactional"] = False
            newfutureId = GenerateStableId(taskkwargs["name"])
            
        newkey = ndb.Key(_Future, newfutureId, parent = immediateancestorkey)
        
#         logging.debug("runfuture: ancestorkey=%s" % immediateancestorkey)
#         logging.debug("runfuture: newkey=%s" % newkey)

        futureobj = _Future(key=newkey) # just use immediate ancestor to keep entity groups at local level, not one for the entire tree
        
        futureobj.parentkey = parentkey # but keep the real parent key for lookups
        
        if onsuccessf:
            futureobj.onsuccessfser = yccloudpickle.dumps(onsuccessf)
        if onfailuref:
            futureobj.onfailurefser = yccloudpickle.dumps(onfailuref)
        if onprogressf:
            futureobj.onprogressfser = yccloudpickle.dumps(onprogressf)
        futureobj.taskkwargsser = yccloudpickle.dumps(taskkwargs)
        
#         futureobj.set_weight(weight if weight >= 1 else 1)
        
        futureobj.timeoutsec = timeoutsec
        
        futureobj.name = futurename
            
        futureobj.put()
#         logging.debug("runfuture: childkey=%s" % futureobj.key)
                
        futurekey = futureobj.key
        logging.debug("outer, futurekey=%s" % futurekey)
        
        @task(includeheaders = True, **taskkwargscopy)
        def _futurewrapper(headers):
            if maxretries:
                lretryCount = 0
                try:
                    lretryCount = int(headers.get("X-Appengine-Taskretrycount", 0)) if headers else 0 
                except:
                    logging.exception("Failed trying to get retry count, using 0")
                    
                if lretryCount > maxretries:
                    raise PermanentTaskFailure("Too many retries of Future")
            
            
            logging.debug("inner, futurekey=%s" % futurekey)
            futureobj = futurekey.get()
            if futureobj:
                futureobj.set_weight(weight if weight >= 1 else 1)
            else:
                raise Exception("Future not ready yet")

            try:
                if includefuturekey:
                    result = f(*args, futurekey = futurekey, **kwargs)
                else:
                    result = f(*args, **kwargs)

            except FutureReadyForResult:
                futureobj = futurekey.get()
                if futureobj:
                    futureobj.set_readyforesult()

            except FutureNotReadyForResult:
                pass
            
            else:
                futureobj = futurekey.get()
                if futureobj:
                    futureobj.set_success_and_readyforesult(result)

        try:
            # run the wrapper task, and if it fails due to a name clash just skip it (it was already kicked off by an earlier
            # attempt to construct this future).
            _futurewrapper()
        except taskqueue.TombstonedTaskError:
            logging.debug("skip adding task (already been run)")
        except taskqueue.TaskAlreadyExistsError:
            logging.debug("skip adding task (already running)")
        
        return futureobj

    return runfuture

def twostagefuture(createstage1futuref, createstage2futuref, onsuccessf=None, onfailuref=None, onprogressf=None, parentkey = None, **taskkwargs):
    @future(includefuturekey = True, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, parentkey = parentkey, **taskkwargs)
    def toplevel(futurekey):

        @future(parentkey=futurekey, **taskkwargs)
        def DoNothing():
            raise FutureNotReadyForResult("this does nothing")
         
        placeholderfuture = DoNothing()
        placeholderfuturekey = placeholderfuture.key

        def OnStage2Success(stage1future):
            stage1result = stage1future.get_result()
            placeholderfuture = placeholderfuturekey.get()
            if placeholderfuture:
                placeholderfuture.set_success(stage1result)
            toplevelfuture = futurekey.get()
            if toplevelfuture:
                toplevelfuture.set_success(stage1result)
        
        def OnStage1Success(stage1future):
            placeholderfuture = placeholderfuturekey.get()
            if placeholderfuture:
                stage2future = createstage2futuref(stage1future, parentkey = placeholderfuturekey, onsuccessf = OnStage2Success, onfailuref = StandardOnFailure, **taskkwargs)

                placeholderfuture.set_weight(stage2future.get_weight())
                toplevelfuture = futurekey.get()
                if toplevelfuture:
                    toplevelfuture.set_weight(toplevelfuture.get_weight() + stage2future.get_weight())

                # now that the second pass is actually constructed and running, we can let the placeholder accept a result.
                placeholderfuture.set_readyforesult()

        @ndb.transactional(xg=True)
        def StandardOnFailure(childfuture):
            parentfuture = childfuture.parentkey.get() if childfuture.parentkey else None
            if parentfuture and not parentfuture.has_result():
                try:
                    childfuture.get_result()
                except Exception, ex:
                    parentfuture.set_failure(ex)

        toplevelfuture = futurekey.get()
        if toplevelfuture:
            stage1future = createstage1futuref(parentkey = futurekey, onsuccessf = OnStage1Success, onfailuref = StandardOnFailure, **taskkwargs)
            toplevelfuture.set_weight(stage1future.get_weight())
        
        raise FutureReadyForResult("still going")
        
    return toplevel()
