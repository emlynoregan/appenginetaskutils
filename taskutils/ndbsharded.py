from task import task, RetryTaskException
from google.appengine.ext.key_range import KeyRange
import logging
from taskutils.future import future, FutureReadyForResult, FutureNotReadyForResult
from taskutils.future import GenerateOnAllChildSuccess, generatefuturepagemapf,\
    setlocalprogress, GetFutureAndCheckReady

def ndbshardedpagemap(pagemapf=None, ndbquery=None, initialshards = 10, pagesize = 100, **taskkwargs):
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

def ndbshardedmap(mapf=None, ndbquery=None, initialshards = 10, pagesize = 100, skipmissing = False, **taskkwargs):
    @task(**taskkwargs)
    def InvokeMap(key, **kwargs):
        logging.debug("Enter InvokeMap: %s" % key)
        try:
            obj = key.get()
            if not obj:
                if not skipmissing:
                    raise RetryTaskException("couldn't get object for key %s" % key)
                # else just skip
            else:
                mapf(obj, **kwargs)
        finally:
            logging.debug("Leave InvokeMap: %s" % key)
    
    def ProcessPage(keys):
        for index, key in enumerate(keys):
            logging.debug("Key #%s: %s" % (index, key))
            InvokeMap(key)

    ndbshardedpagemap(ProcessPage, ndbquery, initialshards, pagesize, **taskkwargs)


def futurendbshardedpagemap(pagemapf=None, ndbquery=None, pagesize=100, onsuccessf=None, onfailuref=None, onprogressf = None, onallchildsuccessf = None, initialresult = None, oncombineresultsf = None, weight = None, parentkey=None, **taskkwargs):
    kind = ndbquery.kind
 
    krlist = KeyRange.compute_split_points(kind, 5)
    logging.debug("first krlist: %s" % krlist)
    logging.debug(taskkwargs)
 
    @future(onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, onallchildsuccessf=onallchildsuccessf, parentkey=parentkey, weight = weight, **taskkwargs)
    def dofuturendbshardedmap(futurekey):
        logging.debug(taskkwargs)
 
        linitialresult = initialresult if not initialresult is None else 0
        loncombineresultsf = oncombineresultsf if oncombineresultsf else lambda a, b: a + b
    
        def MapOverRange(futurekey, keyrange, weight, **kwargs):
            logging.debug("Enter MapOverRange: %s" % keyrange)
            try:
                _fixkeyend(keyrange, kind)
                
                filteredquery = keyrange.filter_ndb_query(ndbquery)
                
                logging.debug (filteredquery)
                 
                keys, _, more = filteredquery.fetch_page(pagesize, keys_only=True)

                lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, 0 if pagemapf else len(keys), lambda a, b: a + b)
                         
                if pagemapf:
                    futurename = "pagemap %s of %s" % (len(keys), keyrange)
                    lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, linitialresult, loncombineresultsf)
                    future(pagemapf, parentkey=futurekey, futurename=futurename, onallchildsuccessf=lonallchildsuccessf, weight = len(keys), **taskkwargs)(keys)
                else:
                    setlocalprogress(futurekey, len(keys))

                if more and keys:
                    lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, linitialresult if pagemapf else len(keys), loncombineresultsf)
                    newkeyrange = KeyRange(keys[-1], keyrange.key_end, keyrange.direction, False, keyrange.include_end)
                    krlist = newkeyrange.split_range()
                    logging.debug("krlist: %s" % krlist)
                    newweight = (weight / len(krlist)) - len(keys) if weight else None
                    for kr in krlist:
                        futurename = "shard %s" % (kr)
                        future(MapOverRange, parentkey=futurekey, futurename=futurename, onallchildsuccessf = lonallchildsuccessf, weight = newweight, **taskkwargs)(kr, weight = newweight)
# 
                if pagemapf or (more and keys):
#                 if (more and keys):
                    raise FutureReadyForResult("still going")
                else:
                    return len(keys)#(len(keys), 0, keyrange)
#                 return len(keys)
            finally:
                logging.debug("Leave MapOverRange: %s" % keyrange)
  
        for kr in krlist:
            lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, linitialresult, loncombineresultsf)
            
            futurename = "shard %s" % (kr)

            newweight = weight / len(krlist) if weight else None
            future(MapOverRange, parentkey=futurekey, futurename=futurename, onallchildsuccessf=lonallchildsuccessf, weight = newweight, **taskkwargs)(kr, weight = newweight)
 
        raise FutureReadyForResult("still going")
 
    return dofuturendbshardedmap()

def generateinvokemapf(mapf):
    def InvokeMap(futurekey, key, **kwargs):
        logging.debug("Enter InvokeMap: %s" % key)
        try:
            obj = key.get()
            if not obj:
                raise RetryTaskException("couldn't get object for key %s" % key)
     
            return mapf(futurekey, obj, **kwargs)
        finally:
            logging.debug("Leave InvokeMap: %s" % key)
    return InvokeMap

def futurendbshardedmap(mapf=None, ndbquery=None, pagesize = 100, onsuccessf = None, onfailuref = None, onprogressf = None, onallchildsuccessf = None, initialresult = None, oncombineresultsf = None, weight = None, parentkey = None, **taskkwargs):
    invokeMapF = generateinvokemapf(mapf)
    pageMapF = generatefuturepagemapf(invokeMapF, initialresult, oncombineresultsf, **taskkwargs)
    return futurendbshardedpagemap(pageMapF, ndbquery, pagesize, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, onallchildsuccessf = onallchildsuccessf, initialresult = initialresult, oncombineresultsf = oncombineresultsf, parentkey=parentkey, weight=weight, **taskkwargs)

def futurendbshardedpagemapwithcount(pagemapf=None, ndbquery=None, pagesize=100, onsuccessf=None, onfailuref=None, onprogressf=None, onallchildsuccessf=None, initialresult = None, oncombineresultsf = None, parentkey = None, **taskkwargs):
    @future(onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, onallchildsuccessf=onallchildsuccessf, parentkey = parentkey, weight=None, **taskkwargs)
    def countthenpagemap(futurekey):
        @future(parentkey=futurekey, futurename="placeholder for pagemap", weight = None, **taskkwargs)
        def DoNothing(futurekey):
            raise FutureNotReadyForResult("waiting for count")
         
        placeholderfuture = DoNothing()
        placeholderfuturekey = placeholderfuture.key

        def OnPageMapSuccess(pagemapfuturekey):
            pagemapfuture = GetFutureAndCheckReady(pagemapfuturekey)
            placeholderfuture = GetFutureAndCheckReady(placeholderfuturekey)
            future = GetFutureAndCheckReady(futurekey)
            result = pagemapfuture.get_result()
            placeholderfuture.set_success(result)
            future.set_success(result)
         
        def OnCountSuccess(countfuturekey):
            countfuture = GetFutureAndCheckReady(countfuturekey)
            futureobj = GetFutureAndCheckReady(futurekey)
            count = countfuture.get_result() 
            placeholderfuture = placeholderfuturekey.get()
            if placeholderfuture:
                placeholderfuture.set_weight(count*2)
                futureobj.set_weight(count*2)
                futurendbshardedpagemap(pagemapf, ndbquery, pagesize, onsuccessf = OnPageMapSuccess, weight = count, parentkey = placeholderfuturekey, **taskkwargs)

                # now that the second pass is actually constructed and running, we can let the placeholder accept a result.
                placeholderfuture.set_readyforesult()
         
        futurendbshardedpagemap(None, ndbquery, pagesize, onsuccessf = OnCountSuccess, parentkey = futurekey, initialresult = initialresult, oncombineresultsf = oncombineresultsf, weight = None, **taskkwargs)
        
        raise FutureReadyForResult("still going")
    return countthenpagemap()

def futurendbshardedmapwithcount(mapf=None, ndbquery=None, pagesize = 100, onsuccessf = None, onfailuref = None, onprogressf = None, onallchildsuccessf = None, initialresult = None, oncombineresultsf = None, weight = None, parentkey = None, **taskkwargs):
    invokeMapF = generateinvokemapf(mapf)
    pageMapF = generatefuturepagemapf(invokeMapF, initialresult, oncombineresultsf, **taskkwargs)
    return futurendbshardedpagemapwithcount(pageMapF, ndbquery, pagesize, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, onallchildsuccessf=onallchildsuccessf, initialresult = initialresult, oncombineresultsf = oncombineresultsf, parentkey=parentkey, **taskkwargs)

def _fixkeyend(keyrange, kind):
    if keyrange.key_start and not keyrange.key_end:
        endkey = KeyRange.guess_end_key(kind, keyrange.key_start)
        if endkey and endkey > keyrange.key_start:
            logging.debug("Fixing end: %s" % endkey)
            keyrange.key_end = endkey
    