from google.appengine.ext import ndb
import functools
from taskutils import task
from yccloudpickle import yccloudpickle
import pickle
import datetime
import logging

class FutureUnderwayError(Exception):
    pass

class FutureTimedOutError(Exception):
    pass

class _Future(ndb.model.Model):
    stored = ndb.DateTimeProperty(auto_now_add = True)
    updated = ndb.DateTimeProperty(auto_now = True)
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
    progress = ndb.FloatProperty()
    weight = ndb.IntegerProperty()
    parentkey = ndb.KeyProperty()

    def has_result(self):
        return not not self.status
    
    def get_result(self):
        if self.status == "failure":
            raise pickle.loads(self.exceptionser)
        elif self.status == "success":
            return pickle.loads(self.resultser)
        else:
            raise FutureUnderwayError("result not ready")
    
    def get_progress(self):
        if self.status:
            return 1.0
        else:
            return self.progress if self.progress else 0.0
        
    def get_weight(self):
        return self.weight if self.weight > 0 else 1

    def get_weightedprogress(self):
        progress = self.get_progress()
        weight = self.get_weight()
        return progress * weight

    def update_result(self):
        updateresultf = pickle.loads(self.updateresultfser) if self.updateresultfser else DefaultUpdateResultF
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
        onprogressf = pickle.loads(self.onprogressfser) if self.onprogressfser else DefaultOnProgressF
        if onprogressf:
            onprogressf(self)
            
    def get_runtime(self):
        if self.runtimesec:
            return datetime.timedelta(seconds = self.runtimesec)
        else:
            return datetime.datetime.utcnow() - self.stored             

    def set_success(self, result):
        selfkey = self.key
        @ndb.transactional
        def set_status_transactional():
            self = selfkey.get()
            if not self.status:
                self.status = "success"
                self.progress = 1.0
                self.resultser = yccloudpickle.dumps(result)
                self.runtimesec = self.get_runtime().total_seconds()
                self.put()
            return self
        self = set_status_transactional()
        self._callOnProgress()
        self._callOnSuccess()

    def set_failure(self, exception):
        selfkey = self.key
        @ndb.transactional
        def set_status_transactional():
            self = selfkey.get()
            if not self.status:
                self.status = "failure"
                self.exceptionser = yccloudpickle.dumps(exception)
                self.runtimesec = self.get_runtime().total_seconds()
                self.put()
            return self
        self = set_status_transactional()
        self._callOnFailure()
            
    def set_progress(self, value):
        selfkey = self.key
        @ndb.transactional
        def set_value_transactional():
            self = selfkey.get()
            if value != self.progress:
                self.progress = value
                self.put()
            return self
        self = set_value_transactional()
        self._callOnProgress()

    def set_weight(self, value):
        selfkey = self.key
        @ndb.transactional
        def set_value_transactional():
            self = selfkey.get()
            if value != self.weight:
                self.weight = value
                self.put()
            return self
        self = set_value_transactional()
        self._callOnProgress()

    def to_dict(self):
        if not self.has_result():
            self.update_result()
             
        return {
            "key": str(self.key) if self.key else None,
            "stored": str(self.stored) if self.stored else None,
            "updated": str(self.updated) if self.stored else None,
            "status": str(self.status) if self.status else "underway",
            "result": str(pickle.loads(self.resultser)) if self.resultser else None,
            "exception": str(pickle.loads(self.exceptionser)) if self.exceptionser else None,
            "runtimesec": self.get_runtime().total_seconds(),
            "progress": self.get_progress(),
            "weight": self.get_weight(),
            "weightedprogress": self.get_weightedprogress()
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
        @ndb.transactional(xg=True)
        def DoUpdate():
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
                parent.set_progress(weightedprogress / weight)
                parent.set_weight(weight)
        DoUpdate()
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
        immediateancestorkey = ndb.Key(parentkey.kind(), parentkey.id()) if parentkey else None
        
        futureobj = _Future(parent = immediateancestorkey) # just use immediate ancestor to keep entity groups at local level, not one for the entire tree
        
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
        
        parent = parentkey.get() if parentkey else None
        logging.debug("** parent: %s" % parent)
        if parent:
            parent.set_weight(parent.get_weight() + 1)
        
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
                logging.exception("not finished")
                pass # ran successfully, but the result isn't ready.
                
        _futurewrapper()
        
        return futureobj

    return runfuture