import functools
import logging
from taskutils.flash import make_flash
from google.appengine.api import memcache

#decorator to add caching to a function
def memcacher(f = None, cachekey=None, expiresec = 600, debug=False):
    if not f:
        return functools.partial(memcacher, cachekey = cachekey, expiresec = expiresec)

    def getvalue(*args, **kwargs):
        lcachekey = cachekey if cachekey else make_flash(f, *args, **kwargs)
        
        retval = memcache.get(lcachekey) #@UndefinedVariable
        if retval is None:
            if debug:
                logging.debug("MISS: %s" % lcachekey)
            retval = f(*args, **kwargs)
            memcache.add(key=lcachekey, value=retval, time=expiresec) #@UndefinedVariable
        else:
            if debug: 
                logging.debug("HIT: %s" % lcachekey)

        return retval

    return getvalue