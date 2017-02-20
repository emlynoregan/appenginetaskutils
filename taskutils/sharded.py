from task import task, RetryTaskException
from google.appengine.ext.key_range import KeyRange
import logging

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
