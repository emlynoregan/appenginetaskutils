from task import task, RetryTaskException
from google.appengine.ext.key_range import KeyRange
import logging
from taskutils.future import future, get_children, FutureUnderwayError
from google.appengine.ext import ndb

def shardedmap(mapf=None, ndbquery=None, initialshards = 10, **taskkwargs):
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
    
    @task(**taskkwargs)
    def MapOverRange(keyrange, **kwargs):
        logging.debug("Enter MapOverRange: %s" % keyrange)

        filteredquery = keyrange.filter_ndb_query(ndbquery)
        
        logging.debug (filteredquery)
        
        keys, _, more = filteredquery.fetch_page(100, keys_only=True)

        lastkey = None       
        for index, key in enumerate(keys):
            logging.debug("Key #%s: %s" % (index, key))
            lastkey = key
            InvokeMap(key)
                    
        if more:
            newkeyrange = KeyRange(lastkey, keyrange.key_end, keyrange.direction, False, False)
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


def futureshardedmap(mapf=None, ndbquery=None, initialshards = 10, **taskkwargs):
    kind = ndbquery.kind

    krlist = KeyRange.compute_split_points(kind, initialshards)
    logging.debug("first krlist: %s" % krlist)
    logging.debug(taskkwargs)

    @future(includefuturekey = True, **taskkwargs)
    def dofutureshardedmap(futurekey):
        logging.debug(taskkwargs)
        
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
        
        @ndb.transactional(xg=True)
        def OnSuccess(childfuture, initialamount = 0):
            parentfuture = childfuture.parentkey.get() if childfuture.parentkey else None
            logging.debug(childfuture)
            logging.debug(parentfuture)
            if parentfuture and not parentfuture.has_result():
                children = get_children(parentfuture.key)
                logging.debug("children: %s" % children)
                if children:
                    result = initialamount
                    error = None
                    finished = True
                    for childfuture in children:
                        logging.debug("childfuture: %s" % childfuture)
                        if childfuture.has_result():
                            try:
                                result += childfuture.get_result()
                            except Exception, ex:
                                error = ex
                                break
                        else:
                            finished = False
                            
                    if error:
                        logging.debug("error: %s" % error)
                        parentfuture.set_failure(error)
                    elif finished:
                        logging.debug("result: %s" % result)
                        parentfuture.set_success(result)
                    else:
                        logging.debug("not finished")
                else:
                    parentfuture.set_failure(Exception("no children found"))

        @ndb.transactional(xg=True)
        def OnFailure(childfuture):
            parentfuture = childfuture.parentkey.get() if childfuture.parentkey else None
            if parentfuture and not parentfuture.has_result():
                try:
                    childfuture.get_result()
                except Exception, ex:
                    parentfuture.set_failure(ex)

        def MapOverRange(keyrange, futurekey, **kwargs):
            logging.debug("Enter MapOverRange: %s" % keyrange)
            try:
                filteredquery = keyrange.filter_ndb_query(ndbquery)
                
                logging.debug (filteredquery)
                
                keys, _, more = filteredquery.fetch_page(100, keys_only=True)
        
                lastkey = None       
                for index, key in enumerate(keys):
                    logging.debug("Key #%s: %s" % (index, key))
                    lastkey = key
                    InvokeMap(key)
                            
                if more:
                    newkeyrange = KeyRange(lastkey, keyrange.key_end, keyrange.direction, False, False)
                    krlist = newkeyrange.split_range()
                    logging.debug("krlist: %s" % krlist)
                    for kr in krlist:
                        def OnSuccessWithInitialAmount(childfuture):
                            OnSuccess(childfuture, len(keys))
                            
                        future(MapOverRange, parentkey=futurekey, includefuturekey = True, onsuccessf=OnSuccessWithInitialAmount, onfailuref=OnFailure, **taskkwargs)(kr)
                    
                    raise FutureUnderwayError("still going")
                else:
                    return len(keys)
            finally:
                logging.debug("Leave MapOverRange: %s" % keyrange)
            
 
        for kr in krlist:
            future(MapOverRange, parentkey=futurekey, includefuturekey = True, onsuccessf=OnSuccess, onfailuref=OnFailure, **taskkwargs)(kr)

        raise FutureUnderwayError("still going")

    return dofutureshardedmap()