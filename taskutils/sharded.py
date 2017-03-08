from task import task, RetryTaskException
from google.appengine.ext.key_range import KeyRange
import logging
from future import future, get_children, FutureReadyForResult, FutureNotReadyForResult
from google.appengine.ext import ndb

def shardedpagemap(pagemapf=None, ndbquery=None, initialshards = 10, pagesize = 100, **taskkwargs):
    @task(**taskkwargs)
    def MapOverRange(keyrange, **kwargs):
        logging.debug("Enter MapOverRange: %s" % keyrange)
 
        _fixkeyend(keyrange, kind)
 
        filteredquery = keyrange.filter_ndb_query(ndbquery)
         
        logging.debug (filteredquery)
         
        keys, _, more = filteredquery.fetch_page(pagesize, keys_only=True)
         
        if pagemapf:
            pagemapf(keys)
                     
        if more and keys:
            newkeyrange = KeyRange(keys[-1], keyrange.key_end, keyrange.direction, False, keyrange.include_end)
            krlist = newkeyrange.split_range()
            logging.debug("krlist: %s" % krlist)
            for kr in krlist:
                MapOverRange(kr)
        logging.debug("Leave MapOverRange: %s" % keyrange)
 
    kind = ndbquery.kind
 
    krlist = KeyRange.compute_split_points(kind, initialshards)
    logging.debug("first krlist: %s" % krlist)
 
    for kr in krlist:
        MapOverRange(kr)

def shardedmap(mapf=None, ndbquery=None, initialshards = 10, pagesize = 100, **taskkwargs):
    @task(**taskkwargs)
    def InvokeMap(key, **kwargs):
        logging.debug("Enter InvokeMap: %s" % key)
        try:
            obj = key.get()
            if not obj:
                raise RetryTaskException("couldn't get object for key %s" % key)
    
            mapf(obj, **kwargs)
        finally:
            logging.debug("Leave InvokeMap: %s" % key)
    
    def ProcessPage(keys):
        for index, key in enumerate(keys):
            logging.debug("Key #%s: %s" % (index, key))
            InvokeMap(key)

    shardedpagemap(ProcessPage, ndbquery, initialshards, pagesize, **taskkwargs)


def futureshardedpagemap(pagemapf=None, ndbquery=None, pagesize=100, onsuccessf=None, onfailuref=None, onprogressf = None, weight = 1, parentkey=None, **taskkwargs):
    kind = ndbquery.kind
 
    krlist = KeyRange.compute_split_points(kind, 5)
    logging.debug("first krlist: %s" % krlist)
    logging.debug(taskkwargs)
 
    @future(includefuturekey = True, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, parentkey=parentkey, weight = weight, **taskkwargs)
    def dofutureshardedmap(futurekey):
        logging.debug(taskkwargs)
                 
        def OnSuccess(childfuture, keyrange, initialamount = 0):
            logging.debug("A: cfhasr=%s" % childfuture.has_result())

            parentfuture = childfuture.parentkey.get() if childfuture.parentkey else None
            logging.debug(childfuture)
            logging.debug(parentfuture)
            if parentfuture and not parentfuture.has_result():
                @ndb.transactional()
                def get_children_trans():
                    return get_children(parentfuture.key)
                children = get_children_trans()
                
                logging.debug("children: %s" % [child.key for child in children])
                if children:
                    result = initialamount
                    error = None
                    finished = True
                    for childfuture in children:
                        logging.debug("childfuture: %s" % childfuture.key)
                        if childfuture.has_result():
                            try:
                                result += childfuture.get_result()
                                logging.debug("hasresult:%s" % result)
                            except Exception, ex:
                                logging.debug("haserror:%s" % repr(ex))
                                error = ex
                                break
                        else:
                            logging.debug("noresult")
                            finished = False
                             
                    if error:
                        logging.debug("error: %s" % error)
                        parentfuture.set_failure(error)
                    elif finished:
                        logging.debug("result: %s" % result)
                        parentfuture.set_success(result)#(result, initialamount, keyrange))
                    else:
                        logging.debug("not finished")
                else:
                    parentfuture.set_failure(Exception("no children found"))
 
        @ndb.transactional(xg=True)
        def OnFailure(childfuture):
#             childfuture = childfuture.key.get()
            parentfuture = childfuture.parentkey.get() if childfuture.parentkey else None
            if parentfuture and not parentfuture.has_result():
                try:
                    childfuture.get_result()
                except Exception, ex:
                    parentfuture.set_failure(ex)
 
        def MapOverRange(keyrange, weight, futurekey, **kwargs):
            logging.debug("Enter MapOverRange: %s" % keyrange)
            try:
                _fixkeyend(keyrange, kind)
                
                filteredquery = keyrange.filter_ndb_query(ndbquery)
                
                logging.debug (filteredquery)
                 
                keys, _, more = filteredquery.fetch_page(pagesize, keys_only=True)
                         
                if pagemapf:
                    pagemapf(keys)
                                             
                if more and keys:
                    newkeyrange = KeyRange(keys[-1], keyrange.key_end, keyrange.direction, False, keyrange.include_end)
                    krlist = newkeyrange.split_range()
                    logging.debug("krlist: %s" % krlist)
                    newweight = (weight / len(krlist)) - len(keys)
                    for kr in krlist:
                        def OnSuccessWithInitialAmount(childfuture):
                            OnSuccess(childfuture, keyrange, len(keys))
                             
                        future(MapOverRange, parentkey=futurekey, includefuturekey = True, onsuccessf=OnSuccessWithInitialAmount, onfailuref=OnFailure, weight = newweight, **taskkwargs)(kr, weight = newweight)
                     
                    raise FutureReadyForResult("still going")
                else:
                    return len(keys)#(len(keys), 0, keyrange)
            finally:
                logging.debug("Leave MapOverRange: %s" % keyrange)
  
        for kr in krlist:
            def OnSuccessWithKR(childfuture):
                OnSuccess(childfuture, kr, 0)

            future(MapOverRange, parentkey=futurekey, includefuturekey = True, onsuccessf=OnSuccessWithKR, onfailuref=OnFailure, weight = weight / len(krlist), **taskkwargs)(kr, weight = weight / len(krlist))
 
        raise FutureReadyForResult("still going")
 
    return dofutureshardedmap()


def futureshardedmap(mapf=None, ndbquery=None, pagesize = 100, onsuccessf = None, onfailuref = None, onprogressf = None, weight= 1, parentkey = None, **taskkwargs):
    @task(**taskkwargs)
    def InvokeMap(key, **kwargs):
        logging.debug("Enter InvokeMap: %s" % key)
        try:
            obj = key.get()
            if not obj:
                raise RetryTaskException("couldn't get object for key %s" % key)
    
            mapf(obj, **kwargs)
        finally:
            logging.debug("Leave InvokeMap: %s" % key)

    def ProcessPage(keys):
        for index, key in enumerate(keys):
            logging.debug("Key #%s: %s" % (index, key))
            InvokeMap(key)

    return futureshardedpagemap(ProcessPage, ndbquery, pagesize, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = None, parentkey=parentkey, **taskkwargs)


def futureshardedpagemapwithcount(pagemapf=None, ndbquery=None, pagesize=100, onsuccessf=None, onfailuref=None, onprogressf=None, parentkey = None, **taskkwargs):
    
    @future(includefuturekey = True, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, parentkey = parentkey, weight = 200, **taskkwargs)
    def countthenpagemap(futurekey):
        @future(parentkey=futurekey, weight = 100, **taskkwargs)
        def DoNothing():
            raise FutureNotReadyForResult("this does nothing")
         
        placeholderfuture = DoNothing()
        placeholderfuturekey = placeholderfuture.key

        def OnPageMapSuccess(pagemapfuture):
            placeholderfuture = placeholderfuturekey.get()
            if placeholderfuture:
                placeholderfuture.set_success(pagemapfuture.get_result())
            future = futurekey.get()
            if future:
                future.set_success(pagemapfuture.get_result())
         
        def OnCountSuccess(countfuture):
            count = countfuture.get_result()
            placeholderfuture = placeholderfuturekey.get()
            if placeholderfuture:
                placeholderfuture.set_weight(count)
                future = futurekey.get()
                if future:
                    future.set_weight(count + 100)
                futureshardedpagemap(pagemapf, ndbquery, pagesize, onsuccessf = OnPageMapSuccess, weight = count, parentkey = placeholderfuturekey, **taskkwargs)

                # now that the second pass is actually constructed and running, we can let the placeholder accept a result.
                placeholderfuture.set_readyforesult()
         
        futureshardedpagemap(None, ndbquery, pagesize, onsuccessf = OnCountSuccess, parentkey = futurekey, weight = 100, **taskkwargs)
        
        raise FutureReadyForResult("still going")
        
    return countthenpagemap()

def futureshardedmapwithcount(mapf=None, ndbquery=None, pagesize = 100, onsuccessf = None, onfailuref = None, onprogressf = None, parentkey = None, **taskkwargs):
    @task(**taskkwargs)
    def InvokeMap(key, **kwargs):
        logging.debug("Enter InvokeMap: %s" % key)
        try:
            obj = key.get()
            if not obj:
                raise RetryTaskException("couldn't get object for key %s" % key)
    
            mapf(obj, **kwargs)
        finally:
            logging.debug("Leave InvokeMap: %s" % key)

    def ProcessPage(keys):
        for index, key in enumerate(keys):
            logging.debug("Key #%s: %s" % (index, key))
            InvokeMap(key)

    return futureshardedpagemapwithcount(ProcessPage, ndbquery, pagesize, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, parentkey = parentkey, **taskkwargs)
        

def _fixkeyend(keyrange, kind):
    if keyrange.key_start and not keyrange.key_end:
        endkey = KeyRange.guess_end_key(kind, keyrange.key_start)
        if endkey and endkey > keyrange.key_start:
            logging.debug("Fixing end: %s" % endkey)
            keyrange.key_end = endkey
    