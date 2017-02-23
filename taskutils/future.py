from google.appengine.ext import ndb
import functools
from taskutils import task
from yccloudpickle import yccloudpickle
import pickle
import datetime
import logging
import uuid

class FutureUnderwayError(Exception):
    pass

class FutureTimedOutError(Exception):
    pass

class _FutureProgress(ndb.model.Model):
    progress = ndb.FloatProperty()
    weight = ndb.IntegerProperty()

class _Future(ndb.model.Model):
    stored = ndb.DateTimeProperty(auto_now_add = True)
    updated = ndb.DateTimeProperty(auto_now = True)
    parentkey = ndb.KeyProperty()
    resultser = ndb.BlobProperty()
    exceptionser = ndb.BlobProperty()
    updateresultfser = ndb.BlobProperty()
    onsuccessfser = ndb.BlobProperty()
    onfailurefser = ndb.BlobProperty()
    onprogressfser = ndb.BlobProperty()
    updateresultfser = ndb.BlobProperty()
    taskkwargsser = ndb.BlobProperty()
    status = ndb.StringProperty()
    runtimesec = ndb.FloatProperty()

    def has_result(self):
        return not not self.status
    
    def get_result(self):
        if self.status == "failure":
            raise pickle.loads(self.exceptionser)
        elif self.status == "success":
            return pickle.loads(self.resultser)
        else:
            raise FutureUnderwayError("result not ready")

    def _get_progressobject(self):
        key = ndb.Key(_FutureProgress, self.key.id())
        progressobj = key.get()
        if not progressobj:
            progressobj = _FutureProgress(key = key)
        return progressobj
    
    def get_progress(self, progressobj = None):
        if self.status:
            return 1.0
        else:
            progressobj = progressobj if progressobj else self._get_progressobject()
            return progressobj.progress if progressobj and progressobj.progress else 0.0
        
    def get_weight(self, progressobj = None):
        progressobj = progressobj if progressobj else self._get_progressobject()
        return progressobj.weight if progressobj and progressobj.weight > 0 else 1

    def get_weightedprogress(self, progressobj = None):
        progressobj = progressobj if progressobj else self._get_progressobject()
        progress = self.get_progress(progressobj)
        weight = self.get_weight(progressobj)
        return progress * weight

    def update_result(self):
        updateresultf = pickle.loads(self.updateresultfser) if self.updateresultfser else DefaultUpdateResultF
        updateresultf(self)
        
        # note that updateresultf can change the status

        if self.status == "failure":
            self._callOnFailure()
        elif self.status == "success":
            self._callOnSuccess()
                
#     @ndb.non_transactional
    def _callOnSuccess(self):
        onsuccessf = pickle.loads(self.onsuccessfser) if self.onsuccessfser else None
        if onsuccessf:
            onsuccessf(self)
            
#     @ndb.non_transactional
    def _callOnFailure(self):
        onfailuref = pickle.loads(self.onfailurefser) if self.onfailurefser else None
        if onfailuref:
            onfailuref(self)
                
#     @ndb.non_transactional
    def _callOnProgress(self):
        onprogressf = pickle.loads(self.onprogressfser) if self.onprogressfser else DefaultOnProgressF
        if onprogressf:
            onprogressf(self)
            
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
            if not self.status:
                self.status = "success"
                self.progress = 1.0
                self.resultser = yccloudpickle.dumps(result)
                self.runtimesec = self.get_runtime().total_seconds()
                didput = True
                self.put()
            return self, didput
        self, needcalls = set_status_transactional()
        if needcalls:
            self._callOnProgress()
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
                self.exceptionser = yccloudpickle.dumps(exception)
                self.runtimesec = self.get_runtime().total_seconds()
                didput = True
                self.put()
            return self, didput
        self, needcalls = set_status_transactional()
        if needcalls:
            self._callOnFailure()
            
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
        needcalls = False
        if progressobj.weight != value:
            progressobj.weight = value
            needcalls = True
            progressobj.put()
        if needcalls:
            self._callOnProgress()

    def incr_weight(self, value):
        progressobj = self._get_progressobject()
        if progressobj.weight:
            progressobj.weight += value
        else:
            progressobj.weight = value
        progressobj.put()
        self._callOnProgress()

    def set_progress_and_weight(self, progress, weight):
        progressobj = self._get_progressobject()
        needput = False
        if progressobj.progress != progress:
            progressobj.progress = progress
            needput = True
        if progressobj.weight != weight:
            progressobj.weight = weight
            needput = True
        if needput:
            progressobj.put()
            self._callOnProgress()

#     def set_progress(self, value):
#         selfkey = self.key
#         @ndb.transactional
#         def set_value_transactional():
#             self = selfkey.get()
#             didput = False
#             if value != self.progress:
#                 self.progress = value
#                 didput = True
#                 self.put()
#             return self, didput
#         self, needcalls = set_value_transactional()
#         if needcalls:
#             self._callOnProgress()

#     def set_weight(self, value):
#         selfkey = self.key
#         @ndb.transactional
#         def set_value_transactional():
#             self = selfkey.get()
#             didput = False
#             if value != self.weight:
#                 self.weight = value
#                 didput = True
#                 self.put()
#             return self, didput
#         self, needcalls = set_value_transactional()
#         if needcalls:
#             self._callOnProgress()

    def to_dict(self):
        if not self.has_result():
            self.update_result()

        progressobj = self._get_progressobject()
                     
        return {
            "key": str(self.key) if self.key else None,
            "stored": str(self.stored) if self.stored else None,
            "updated": str(self.updated) if self.stored else None,
            "status": str(self.status) if self.status else "underway",
            "result": str(pickle.loads(self.resultser)) if self.resultser else None,
            "exception": str(pickle.loads(self.exceptionser)) if self.exceptionser else None,
            "runtimesec": self.get_runtime().total_seconds(),
            "progress": self.get_progress(progressobj),
            "weight": self.get_weight(progressobj),
            "weightedprogress": self.get_weightedprogress(progressobj)
        }
        
def DefaultUpdateResultFOld(futureobj):
    if not futureobj.status and futureobj.get_runtime() > datetime.timedelta(seconds = 1800): # 30 minute timeout
        futureobj.set_failure(FutureTimedOutError("timeout"))

def DefaultUpdateResultF(futureobj):
    if not futureobj.status and futureobj.get_runtime() > datetime.timedelta(seconds = 1800): # 30 min timeout
        futureobj.set_failure(FutureTimedOutError("timeout"))

    taskkwargs = pickle.loads(futureobj.taskkwargsser)

    @task(**taskkwargs)
    def UpdateChildren():
        for childfuture in get_children(futureobj.key):
            logging.debug("ur: %s" % childfuture.key)
            logging.debug("UPDATE")
            childfuture.update_result()
#             if futureobj.status:
#                 logging.debug("DELETE")
#                 childfuture.key.delete()
    UpdateChildren()

def DefaultOnProgressF(futureobj):
#     pass
    taskkwargs = pickle.loads(futureobj.taskkwargsser)
  
    logging.debug("Enter DefaultOnProgressF: %s" % futureobj)
    @task(**taskkwargs)
    def UpdateParent(parentkey):
        logging.debug("***************************************************")
        logging.debug("Enter UpdateParent: %s" % parentkey)
        logging.debug("***************************************************")

        parent = parentkey.get()
        logging.debug("1: %s" % parent)
        if parent:
            logging.debug("2")
            weightedprogress = 1.0 if parent.has_result() else 0.0
            weight = 1
            for childfuture in get_children(parentkey):
                logging.debug("3: %s" % childfuture)
                weightedprogress += childfuture.get_weightedprogress()
                weight += childfuture.get_weight()
            if weight <= 0:
                weight = 1
            logging.debug("4: %s, %s" % (weightedprogress, weight))
            parent.set_progress_and_weight(weightedprogress / weight, weight)

    if futureobj.parentkey:
        UpdateParent(futureobj.parentkey)

def get_children(futurekey):
    if futurekey:
        ancestorkey = ndb.Key(futurekey.kind(), futurekey.id())
        return [childfuture for childfuture in _Future.query(ancestor=ancestorkey) if ancestorkey == childfuture.key.parent()]
    else:
        return []

def future(f=None, parentkey=None, includefuturekey=False, 
           updateresultf = None, onsuccessf=None, onfailuref=None, onprogressf=None, **taskkwargs):
    
    if not f:
        return functools.partial(future, 
            parentkey=parentkey, includefuturekey=includefuturekey, updateresultf=updateresultf, 
            onsuccessf=onsuccessf, onfailuref=onfailuref, onprogressf=onprogressf,             
            **taskkwargs)
    
    logging.debug("includefuturekey: %s" % includefuturekey)
    
    @functools.wraps(f)
    def runfuture(*args, **kwargs):
        logging.debug("runfuture: parentkey=%s" % parentkey)

        immediateancestorkey = ndb.Key(parentkey.kind(), parentkey.id()) if parentkey else None
        newkey = ndb.Key(_Future, str(uuid.uuid4()), parent = immediateancestorkey)
        
        logging.debug("runfuture: ancestorkey=%s" % immediateancestorkey)
        logging.debug("runfuture: newkey=%s" % newkey)

        futureobj = _Future(key=newkey) # just use immediate ancestor to keep entity groups at local level, not one for the entire tree
        
        futureobj.parentkey = parentkey # but keep the real parent key for lookups
        
        if updateresultf:
            futureobj.updateresultfser = yccloudpickle.dumps(updateresultf)
        if onsuccessf:
            futureobj.onsuccessfser = yccloudpickle.dumps(onsuccessf)
        if onfailuref:
            futureobj.onfailurefser = yccloudpickle.dumps(onfailuref)
        if onprogressf:
            futureobj.onprogressfser = yccloudpickle.dumps(onprogressf)
        futureobj.taskkwargsser = yccloudpickle.dumps(taskkwargs)
        
        futureobj.weight = 1
        futureobj.progress = 0.0
            
        futureobj.put()
        logging.debug("runfuture: childkey=%s" % futureobj.key)
        
        parent = parentkey.get() if parentkey else None
        logging.debug("** parent: %s" % parent)
        if parent:
            try:
                parent.incr_weight(1)
            except Exception:
                logging.exception("failed to increment parent weight, skipping")
        
        futurekey = futureobj.key
        
        @task(**taskkwargs)
        def _futurewrapper():
            try:
                if includefuturekey:
                    result = f(*args, futurekey = futurekey, **kwargs)
                else:
                    result = f(*args, **kwargs)

                futureobj = futurekey.get()
                if futureobj:
                    futureobj.set_success(result)
            except FutureUnderwayError:
                logging.debug("not finished")
                pass # ran successfully, but the result isn't ready.
                
        _futurewrapper()
        
        return futureobj

    return runfuture