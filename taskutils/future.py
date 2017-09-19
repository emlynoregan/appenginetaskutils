from google.appengine.ext import ndb
import functools
from taskutils import task
import cloudpickle
import pickle
import datetime
import logging
import uuid
import json
from taskutils.task import PermanentTaskFailure
import hashlib
from google.appengine.api import taskqueue
from google.appengine.api.datastore_errors import Timeout
import time

class FutureReadyForResult(Exception):
    pass

class FutureNotReadyForResult(Exception):
    pass

class FutureTimedOutError(Exception):
    pass

class FutureCancelled(Exception):
    pass

class _FutureProgress(ndb.model.Model):
    localprogress = ndb.IntegerProperty()
    calculatedprogress = ndb.IntegerProperty()
    weight = ndb.IntegerProperty()

class _Future(ndb.model.Model):
    stored = ndb.DateTimeProperty(auto_now_add = True)
    updated = ndb.DateTimeProperty(auto_now = True)
    parentkey = ndb.KeyProperty()
    resultser = ndb.BlobProperty()
    exceptionser = ndb.BlobProperty()
    onsuccessfser = ndb.BlobProperty()
    onfailurefser = ndb.BlobProperty()
    onallchildsuccessfser = ndb.BlobProperty()
    onprogressfser = ndb.BlobProperty()
    taskkwargsser = ndb.BlobProperty()
    status = ndb.StringProperty()
    runtimesec = ndb.FloatProperty()
    initialised = ndb.BooleanProperty()
    readyforresult = ndb.BooleanProperty()
    timeoutsec = ndb.IntegerProperty()
    name = ndb.StringProperty()

        

    def get_taskkwargs(self, deletename = True):
        taskkwargs = pickle.loads(self.taskkwargsser)
        
        if deletename and "name" in taskkwargs:
            del taskkwargs["name"]
            
        return taskkwargs

    def intask(self, nameprefix, f, *args, **kwargs):
        taskkwargs = self.get_taskkwargs()

        if nameprefix:
            name = "%s-%s" % (nameprefix, self.key.id())
            taskkwargs["name"] = name
        elif taskkwargs.get("name"):
            del taskkwargs["name"]
        taskkwargs["transactional"] = False
        
        @task(**taskkwargs)
        def dof():
            f(*args, **kwargs)
        
        try:
            # run the wrapper task, and if it fails due to a name clash just skip it (it was already kicked off by an earlier
            # attempt to construct this future).
#             logging.debug("about to run task %s" % name)
            dof()
        except taskqueue.TombstonedTaskError:
            logging.debug("skip adding task %s (already been run)" % name)
        except taskqueue.TaskAlreadyExistsError:
            logging.debug("skip adding task %s (already running)" % name)

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
    
    def get_calculatedprogress(self, progressobj = None):
        progressobj = progressobj if progressobj else self._get_progressobject()
        return progressobj.calculatedprogress if progressobj and progressobj.calculatedprogress else 0

    def get_weight(self, progressobj = None):
        progressobj = progressobj if progressobj else self._get_progressobject()
        return progressobj.weight if progressobj and progressobj.weight else None

    def get_localprogress(self, progressobj = None):
        progressobj = progressobj if progressobj else self._get_progressobject()
        return progressobj.localprogress if progressobj and progressobj.localprogress else 0

    def _calculate_progress(self, localprogress):
        newcalculatedprogress = localprogress
        @ndb.transactional()
        def get_children_trans():
            return get_children(self.key)
        children = get_children_trans()
        
        if children:
            for child in children:
                newcalculatedprogress += child.get_calculatedprogress()

        return newcalculatedprogress        
        
        
#     def update_result(self):
#         if self.readyforresult:
#             updateresultf = UpdateResultF #pickle.loads(self.updateresultfser) if self.updateresultfser else DefaultUpdateResultF
#             updateresultf(self)
#             
#             # note that updateresultf can change the status
#     
#             if self.status == "failure":
#                 self._callOnFailure()
#             elif self.status == "success":
#                 self._callOnSuccess()
                
    def GetParent(self):
        return self.parentkey.get() if self.parentkey else None

    def GetChildren(self):
        @ndb.transactional()
        def get_children_trans():
            return get_children(self.key)
        return get_children_trans()
        
    
    def _callOnSuccess(self):
        onsuccessf = pickle.loads(self.onsuccessfser) if self.onsuccessfser else None
        if onsuccessf:
            def doonsuccessf():
                onsuccessf(self.key)
            self.intask("onsuccess", doonsuccessf)
        
        if self.onallchildsuccessfser:
            lparent = self.GetParent()
            if lparent and all_children_success(self.parentkey):
                onallchildsuccessf = pickle.loads(self.onallchildsuccessfser) if self.onallchildsuccessfser else None
                if onallchildsuccessf:
                    def doonallchildsuccessf():
                        onallchildsuccessf()
                    self.intask("onallchildsuccess", doonallchildsuccessf)
            
    def _callOnFailure(self):
        onfailuref = pickle.loads(self.onfailurefser) if self.onfailurefser else None
        def doonfailuref():
            if onfailuref:
                onfailuref(self.key)
            else:
                DefaultOnFailure(self.key)
        self.intask("onfailure", doonfailuref)
                
    def _callOnProgress(self):
        onprogressf = pickle.loads(self.onprogressfser) if self.onprogressfser else None
        if onprogressf:
            def doonprogressf():
                onprogressf(self.key)
            self.intask(None, doonprogressf)
            
    def get_runtime(self):
        if self.runtimesec:
            return datetime.timedelta(seconds = self.runtimesec)
        else:
            return datetime.datetime.utcnow() - self.stored             

    def _set_local_progress_for_success(self):
        progressObj = self._get_progressobject()
        logging.debug("progressObj = %s" % progressObj)
        weight = self.get_weight(progressObj)
        weight = weight if not weight is None else 1
        logging.debug("weight = %s" % weight)
        localprogress = self.get_localprogress(progressObj)
        logging.debug("localprogress = %s" % localprogress)
        if localprogress < weight and not self.GetChildren():
            logging.debug("No children, we can auto set localprogress from weight")
            self.set_localprogress(weight)

    @ndb.non_transactional
    def set_success(self, result):
        selfkey = self.key
        @ndb.transactional
        def set_status_transactional():
            self = selfkey.get()
            didput = False
            if self.readyforresult and not self.status:
                self.status = "success"
                self.initialised = True
                self.readyforresult = True
                self.resultser = cloudpickle.dumps(result)
                self.runtimesec = self.get_runtime().total_seconds()
                didput = True
                self.put()
            return self, didput
        self, needcalls = set_status_transactional()
        if needcalls:
            self._set_local_progress_for_success()
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
                self.initialised = True
                self.readyforresult = True
                self.exceptionser = cloudpickle.dumps(exception)
                self.runtimesec = self.get_runtime().total_seconds()
                didput = True
                self.put()
            return self, didput
        self, needcalls = set_status_transactional()
        if needcalls:
            self._callOnFailure()
            
            if not self.parentkey:
                # top level. Fail everything below
                taskkwargs = self.get_taskkwargs()

                @task(**taskkwargs)
                def failchildren(futurekey):
                    children = get_children(futurekey)
                    if children:
                        for child in children:
                            child.set_failure(exception)
                            failchildren(child.key)
                
                failchildren(self.key)

            
    @ndb.non_transactional
    def set_success_and_readyforesult(self, result):
        selfkey = self.key
        @ndb.transactional
        def set_status_transactional():
            self = selfkey.get()
            didput = False
            if not self.status:
                self.status = "success"
                self.initialised = True
                self.readyforresult = True
                self.resultser = cloudpickle.dumps(result)
                self.runtimesec = self.get_runtime().total_seconds()
                didput = True
                self.put()
            return self, didput
        self, needcalls = set_status_transactional()
        if needcalls:
            self._set_local_progress_for_success()
            self._callOnSuccess()

    @ndb.non_transactional
    def set_readyforesult(self):
        selfkey = self.key
        @ndb.transactional
        def set_status_transactional():
            self = selfkey.get()
            didput = False
            if not self.readyforresult:
                self.initialised = True
                self.readyforresult = True
                didput = True
                self.put()
            return self, didput
        self, _ = set_status_transactional()

    @ndb.non_transactional
    def set_initialised(self):
        selfkey = self.key
        @ndb.transactional
        def set_status_transactional():
            self = selfkey.get()
            didput = False
            if not self.initialised:
                self.initialised = True
                didput = True
                self.put()
            return self, didput
        self, _ = set_status_transactional()

    def _calculate_parent_progress(self):
        parentkey = self.parentkey
        if parentkey:
            taskkwargs = self.get_taskkwargs()

#             @debouncedtask(repeatsec=60, **taskkwargs)
            @task(**taskkwargs)
            def docalculate_parent_progress():
                parent = parentkey.get()
                if parent:
                    parent.calculate_progress()
    
            docalculate_parent_progress()
        
    def set_localprogress(self, value):
        progressobj = self._get_progressobject()
        localprogress = self.get_localprogress(progressobj)
        calculatedprogress = self.get_calculatedprogress(progressobj)
        if localprogress != value:
#             haschildren = self.GetChildren()
#             logging.debug("haschildren: %s" % haschildren)

            progressobj.localprogress = value
            logging.debug("localprogress: %s" % value)
#             if not haschildren:
            lneedupd = value > calculatedprogress
            if lneedupd:
                logging.debug("setting calculated progress")
                progressobj.calculatedprogress = value
                
            progressobj.put()
            
            if lneedupd:
                logging.debug("kicking off calculate parent progress")
                self._calculate_parent_progress()

            self._callOnProgress()

    def calculate_progress(self):
        progressobj = self._get_progressobject()
        localprogress = self.get_localprogress(progressobj)
        calculatedprogress = self.get_calculatedprogress(progressobj)
        newcalculatedprogress = self._calculate_progress(localprogress)
        if calculatedprogress != newcalculatedprogress:
            progressobj.calculatedprogress = newcalculatedprogress
            progressobj.put()
            self._calculate_parent_progress()
            self._callOnProgress()

    def set_weight(self, value):
        if not value is None:
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

    def to_dict(self, level=0, maxlevel = 5, recursive = True, futuremapf=None):
#         if not self.has_result():
#             self.update_result()

        progressobj = self._get_progressobject()
                     
        children = [child.to_dict(level = level + 1, maxlevel = maxlevel, futuremapf=futuremapf) for child in get_children(self.key)] if recursive and level+1 < maxlevel else None
        
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
        
        if futuremapf:
            lkey = futuremapf(self, level)
        else:
            lkey = str(self.key) if self.key else None
        
        return {
            "key": lkey,
            "id": self.key.id() if self.key else None,
            "name": self.name,
            "level": level,
            "stored": str(self.stored) if self.stored else None,
            "updated": str(self.updated) if self.stored else None,
            "status": str(self.status) if self.status else "underway",
            "result": resultrep,
            "exception": repr(pickle.loads(self.exceptionser)) if self.exceptionser else None,
            "runtimesec": self.get_runtime().total_seconds(),
            "localprogress": self.get_localprogress(progressobj),
            "progress": self.get_calculatedprogress(progressobj),
            "weight": self.get_weight(),
            "initialised": self.initialised,
            "readyforresult": self.readyforresult,
            "zchildren": children
        }
        
# def UpdateResultF(futureobj):
#     if not futureobj.status and futureobj.get_runtime() > datetime.timedelta(seconds = futureobj.timeoutsec):
#         futureobj.set_failure(FutureTimedOutError("timeout"))
# 
#     taskkwargs = futureobj.get_taskkwargs()
# 
#     @task(**taskkwargs)
#     def UpdateChildren():
#         for childfuture in get_children(futureobj.key):
# #             logging.debug("update_result: %s" % childfuture.key)
#             childfuture.update_result()
#     UpdateChildren()

def DefaultOnFailure(futurekey):
    futureobj = futurekey.get() if futurekey else None
    parentfutureobj = futureobj.GetParent() if futureobj else None 
    if parentfutureobj and not parentfutureobj.has_result():
        if not parentfutureobj.initialised or not parentfutureobj.readyforresult:
            raise Exception("Parent not initialised, retry")
        try:
            futureobj.get_result()
        except Exception, ex:
            parentfutureobj.set_failure(ex)

def GenerateOnAllChildSuccess(parentkey, initialvalue, combineresultf, failonerror=True):
    def OnAllChildSuccess():
        logging.debug("Enter GenerateOnAllChildSuccess: %s" % parentkey)
        parentfuture = parentkey.get() if parentkey else None
        if parentfuture and not parentfuture.has_result():
            if not parentfuture.initialised or not parentfuture.readyforresult:
                raise Exception("Parent not initialised, retry")
            
            @ndb.transactional()
            def get_children_trans():
                return get_children(parentfuture.key)
            children = get_children_trans()
            
            logging.debug("children: %s" % [child.key for child in children])
            if children:
                result = initialvalue
                error = None
                finished = True
                for childfuture in children:
                    logging.debug("childfuture: %s" % childfuture.key)
                    if childfuture.has_result():
                        try:
                            childresult = childfuture.get_result()
                            logging.debug("childresult(%s): %s" % (childfuture.status, childresult))
                            result = combineresultf(result, childresult)
                            logging.debug("hasresult:%s" % result)
                        except Exception, ex:
                            logging.debug("haserror:%s" % repr(ex))
                            error = ex
                            break
                    else:
                        logging.debug("noresult")
                        finished = False
                         
                if error:
                    logging.warning("Internal error, child has error in OnAllChildSuccess: %s" % error)
                    if failonerror:
                        parentfuture.set_failure(error)
                    else:
                        raise error
                elif finished:
                    logging.debug("result: %s" % result)
                    parentfuture.set_success(result)#(result, initialamount, keyrange))
                else:
                    logging.debug("child not finished in OnAllChildSuccess, skipping")
            else:
                logging.warning("Internal error, parent has no children in OnAllChildSuccess")
                parentfuture.set_failure(Exception("no children found"))

    return OnAllChildSuccess
    
def generatefuturepagemapf(mapf, initialresult = None, oncombineresultsf = None, **taskkwargs):
    def futurepagemapf(futurekey, items):
        linitialresult = initialresult if not initialresult is None else 0
        loncombineresultsf = oncombineresultsf if oncombineresultsf else lambda a, b: a + b
    
        try:
            lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, linitialresult, loncombineresultsf)
            
            if len(items) > 5:
                leftitems = items[len(items) / 2:]
                rightitems = items[:len(items) / 2]
                future(futurepagemapf, parentkey=futurekey, futurename="split left %s" % len(leftitems), onallchildsuccessf=lonallchildsuccessf, weight = len(leftitems), **taskkwargs)(leftitems)
                future(futurepagemapf, parentkey=futurekey, futurename="split right %s" % len(rightitems), onallchildsuccessf=lonallchildsuccessf, weight = len(rightitems), **taskkwargs)(rightitems)
            else:
                for index, item in enumerate(items):
                    futurename = "ProcessItem %s" % index
                    future(mapf, parentkey=futurekey, futurename=futurename, onallchildsuccessf=lonallchildsuccessf, weight = 1, **taskkwargs)(item)
        except Exception, ex:
            raise PermanentTaskFailure(repr(ex))
        else:
            raise FutureReadyForResult()
    
    return futurepagemapf

def OnProgressF(futurekey):
    futureobj = futurekey.get() if futurekey else None
    if futureobj.parentkey:
        taskkwargs = futureobj.get_taskkwargs()
      
        logging.debug("Enter OnProgressF: %s" % futureobj)
        @task(**taskkwargs)
        def UpdateParent(parentkey):
            logging.debug("***************************************************")
            logging.debug("Enter UpdateParent: %s" % parentkey)
            logging.debug("***************************************************")
    
            parent = parentkey.get()
            logging.debug("1: %s" % parent)
            if parent:
                logging.debug("2")
#                 if not parent.has_result():
                progress = 0
                for childfuture in get_children(parentkey):
                    logging.debug("3: %s" % childfuture)
                    progress += childfuture.get_progress()
                logging.debug("4: %s" % (progress))
                parent.set_progress(progress)
    
        UpdateParent(futureobj.parentkey)

def get_children(futurekey):
    if futurekey:
        ancestorkey = ndb.Key(futurekey.kind(), futurekey.id())
        return [childfuture for childfuture in _Future.query(ancestor=ancestorkey).order(_Future.stored) if ancestorkey == childfuture.key.parent()]
    else:
        return []

def all_children_success(futurekey):
    lchildren = get_children(futurekey)
    retval = True
    for lchild in lchildren:
        if lchild.has_result():
            try:
                lchild.get_result()
            except Exception:
                retval = False
                break
        else:
            retval = False
            break
    return retval


def setlocalprogress(futurekey, value):
    future = futurekey.get() if futurekey else None
    if future:
        future.set_localprogress(value)
    
def GenerateStableId(instring):
    return hashlib.md5(instring).hexdigest()

def future(f=None, parentkey=None,  
           onsuccessf=None, onfailuref=None, 
           onallchildsuccessf=None,
           onprogressf=None, 
           weight = None, timeoutsec = 1800, maxretries = None, futurename = None, **taskkwargs):
    
    if not f:
        return functools.partial(future, 
            parentkey=parentkey,  
            onsuccessf=onsuccessf, onfailuref=onfailuref, 
            onallchildsuccessf=onallchildsuccessf,
            onprogressf=onprogressf, 
            weight = weight, timeoutsec = timeoutsec, maxretries = maxretries, futurename = futurename,
            **taskkwargs)
    
#     logging.debug("includefuturekey: %s" % includefuturekey)
    
    @functools.wraps(f)
    def runfuture(*args, **kwargs):
        @ndb.transactional()
        def runfuturetrans():
            logging.debug("runfuture: parentkey=%s" % parentkey)
    
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
                futureobj.onsuccessfser = cloudpickle.dumps(onsuccessf)
            if onfailuref:
                futureobj.onfailurefser = cloudpickle.dumps(onfailuref)
            if onallchildsuccessf:
                futureobj.onallchildsuccessfser = cloudpickle.dumps(onallchildsuccessf)
            if onprogressf:
                futureobj.onprogressfser = cloudpickle.dumps(onprogressf)
            futureobj.taskkwargsser = cloudpickle.dumps(taskkwargs)
    
    #         futureobj.onsuccessfser = yccloudpickle.dumps(onsuccessf) if onsuccessf else None
    #         futureobj.onfailurefser = yccloudpickle.dumps(onfailuref) if onfailuref else None
    #         futureobj.onallchildsuccessfser = yccloudpickle.dumps(onallchildsuccessf) if onallchildsuccessf else None
    #         futureobj.onprogressfser = yccloudpickle.dumps(onprogressf) if onprogressf else None
    #         futureobj.taskkwargsser = yccloudpickle.dumps(taskkwargs)
            
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
                    futureobj.set_weight(weight)# if weight >= 1 else 1)
                else:
                    raise Exception("Future not ready yet")
    
                try:
                    logging.debug("args, kwargs=%s, %s" % (args, kwargs))
                    result = f(futurekey, *args, **kwargs)
    
                except FutureReadyForResult:
                    futureobj = futurekey.get()
                    if futureobj:
                        futureobj.set_readyforesult()
    
                except FutureNotReadyForResult:
                    futureobj = futurekey.get()
                    if futureobj:
                        futureobj.set_initialised()
                
                except PermanentTaskFailure, ptf:
                    try:
                        futureobj = futurekey.get()
                        if futureobj:
                            futureobj.set_failure(ptf)
                    finally:
                        raise ptf
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

        manualRetries = 0
        while True: 
            try:
                return runfuturetrans()
            except Timeout, tex:
                if manualRetries < 10:
                    manualRetries += 1
                    time.sleep(manualRetries * 5)
                else:
                    raise tex
            else:
                break # do we need this? Don't think so

    logging.debug("about to call runfuture")
    return runfuture

def GetFutureAndCheckReady(futurekey):
    futureobj = futurekey.get() if futurekey else None
    if not (futureobj and futureobj.initialised and futureobj.readyforresult):
        raise Exception("Future not ready for result, retry")
    return futureobj

# fsf = futuresequencefunction
# different from future function because it has a "results" argument, a list.
def futuresequence(fsfseq, parentkey = None, onsuccessf=None, onfailuref=None, onallchildsuccessf=None, onprogressf=None, weight=None, timeoutsec=1800, maxretries=None, futurenameprefix=None, **taskkwargs):
    logging.debug("Enter futuresequence: %s" % len(fsfseq))
    
    flist = list(fsfseq)
     
    taskkwargs["futurename"] = "%s (top level)" % futurenameprefix if futurenameprefix else "sequence"
     
    @future(parentkey = parentkey, onsuccessf = onsuccessf, onfailuref = onfailuref, onallchildsuccessf=onallchildsuccessf, onprogressf = onprogressf, weight=weight, timeoutsec=timeoutsec, maxretries=maxretries, **taskkwargs)
    def toplevel(futurekey, *args, **kwargs):
        logging.debug("Enter futuresequence.toplevel: %s" % futurekey)
        def childonsuccessforindex(index, results):
            logging.debug("Enter childonsuccessforindex: %s, %s, %s" % (futurekey, index, json.dumps(results, indent=2)))
            def childonsuccess(childfuturekey):
                logging.debug("Enter childonsuccess: %s, %s, %s" % (futurekey, index, childfuturekey))
                logging.debug("results: %s" % json.dumps(results, indent=2))
                try:
                    childfuture = GetFutureAndCheckReady(childfuturekey)
                     
                    try:    
                        result = childfuture.get_result()
                    except Exception, ex:
                        toplevelfuture = futurekey.get()
                        if toplevelfuture:
                            toplevelfuture.set_failure(ex)
                        else:
                            raise Exception("Can't load toplevel future for failure")
                    else:
                        logging.debug("result: %s" % json.dumps(result, indent=2))
                        newresults = results + [result]
                        islast = (index == (len(flist) - 1))
                         
                        if islast:
                            logging.debug("islast")
                            toplevelfuture = futurekey.get()
                            if toplevelfuture:
                                logging.debug("setting top level success")
                                toplevelfuture.set_success_and_readyforesult(newresults)
                            else:
                                raise Exception("Can't load toplevel future for success")
                        else:
                            logging.debug("not last")
                            taskkwargs["futurename"] = "%s [%s]" % (futurenameprefix if futurenameprefix else "-", index+1)
                            future(flist[index+1], parentkey=futurekey, onsuccessf=childonsuccessforindex(index+1, newresults), weight=weight/len(flist) if weight else None, timeoutsec=timeoutsec, maxretries=maxretries, **taskkwargs)(newresults)
                finally:
                    logging.debug("Enter childonsuccess: %s, %s, %s" % (futurekey, index, childfuturekey))
            logging.debug("Leave childonsuccessforindex: %s, %s, %s" % (futurekey, index, json.dumps(results, indent=2)))
            return childonsuccess
 
        taskkwargs["futurename"] = "%s [0]" % (futurenameprefix if futurenameprefix else "sequence")
        future(flist[0], parentkey=futurekey, onsuccessf=childonsuccessforindex(0, []), weight=weight/len(flist) if weight else None, timeoutsec=timeoutsec, maxretries=maxretries, **taskkwargs)([]) # empty list of results 
                 
        logging.debug("Leave futuresequence.toplevel: %s" % futurekey)
        raise FutureNotReadyForResult("sequence started")
 
    return toplevel

def futureparallel(ffseq, parentkey = None, onsuccessf=None, onfailuref=None, onallchildsuccessf=None, onprogressf=None, weight=None, timeoutsec=1800, maxretries=None, futurenameprefix=None, **taskkwargs):
    logging.debug("Enter futureparallel: %s" % len(ffseq))
    flist = list(ffseq)
     
    taskkwargs["futurename"] = "%s (top level)" % futurenameprefix if futurenameprefix else "parallel"
     
    @future(parentkey = parentkey, onsuccessf = onsuccessf, onfailuref = onfailuref, onallchildsuccessf=onallchildsuccessf, onprogressf = onprogressf, weight=weight, timeoutsec=timeoutsec, maxretries=maxretries, **taskkwargs)
    def toplevel(futurekey, *args, **kwargs):
        logging.debug("Enter futureparallel.toplevel: %s" % futurekey)
        def OnAllChildSuccess():
            logging.debug("Enter OnAllChildSuccess: %s" % futurekey)
            parentfuture = futurekey.get() if futurekey else None
            if parentfuture and not parentfuture.has_result():
                if not parentfuture.initialised or not parentfuture.readyforresult:
                    raise Exception("Parent not initialised, retry")
                
                @ndb.transactional()
                def get_children_trans():
                    return get_children(parentfuture.key)
                children = get_children_trans()
                
                logging.debug("children: %s" % [child.key for child in children])
                if children:
                    result = []
                    error = None
                    finished = True
                    for childfuture in children:
                        logging.debug("childfuture: %s" % childfuture.key)
                        if childfuture.has_result():
                            try:
                                childresult = childfuture.get_result()
                                logging.debug("childresult(%s): %s" % (childfuture.status, childresult))
                                result += [childfuture.get_result()]
                                logging.debug("intermediate result:%s" % result)
                            except Exception, ex:
                                logging.debug("haserror:%s" % repr(ex))
                                error = ex
                                break
                        else:
                            logging.debug("noresult")
                            finished = False
                             
                    if error:
                        logging.warning("Internal error, child has error in OnAllChildSuccess: %s" % error)
                        parentfuture.set_failure(error)
                    elif finished:
                        logging.debug("result: %s" % result)
                        parentfuture.set_success(result)
                    else:
                        logging.debug("child not finished in OnAllChildSuccess, skipping")
                else:
                    logging.warning("Internal error, parent has no children in OnAllChildSuccess")
                    parentfuture.set_failure(Exception("no children found"))
        
        for ix, ff in enumerate(flist):
            taskkwargs["futurename"] = "%s [%s]" % (futurenameprefix if futurenameprefix else "parallel", ix)
            future(ff, parentkey=futurekey, onallchildsuccessf = OnAllChildSuccess, weight=weight/len(flist) if weight else None, timeoutsec=timeoutsec, maxretries=maxretries, **taskkwargs)()
                 
        logging.debug("Leave futureparallel.toplevel: %s" % futurekey)
        raise FutureReadyForResult("parallel started")
 
    return toplevel
