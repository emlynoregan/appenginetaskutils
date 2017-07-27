'''
Created on 26Jul.,2017

@author: emlyn
'''

from google.appengine.api import memcache
import yccloudpickle
from datetime import datetime, timedelta
import hashlib
import task
import functools
import logging

def GenerateStableId(instring):
    return hashlib.md5(instring).hexdigest()

def debouncedtask(f=None, initsec = 0, repeatsec = 10, debouncename = None, **taskkwargs):
    if not f:
        return functools.partial(debouncedtask, initsec = initsec, repeatsec = repeatsec, debouncename = debouncename, **taskkwargs)
    
    @functools.wraps(f)
    def rundebouncedtask(*args, **kwargs):
        logging.debug("enter rundebouncedtask")
        retval = None
        client = memcache.Client()
        cachekey = "dt%s" % (debouncename if debouncename else GenerateStableId(yccloudpickle.dumps(f)))
        logging.debug("cachekey: %s" % cachekey)
        eta = client.gets(cachekey)
        logging.debug("eta: %s" % eta)
        now = datetime.utcnow()
        logging.debug("now: %s" % now)
        if not eta or eta < now:
            logging.debug("A")
            if not eta:
                countdown = 0
            else:
                elapsedsectd = now - eta
                elapsedsec = elapsedsectd.total_seconds()
                if elapsedsec > repeatsec:
                    countdown = 0
                else:
                    countdown = repeatsec - elapsedsec
    
            if countdown < initsec:
                countdown = initsec

            logging.debug("countdown: %s" % countdown)
            
            nexteta = now + timedelta(seconds=countdown)
            
            logging.debug("nexteta: %s" % nexteta)

            if eta is None:
                casresult = client.add(cachekey, nexteta)
            else:
                casresult = client.cas(cachekey, nexteta)
            logging.debug("CAS result: %s" % casresult)
            if casresult:
                logging.debug("B")
                
                taskkwargscopy = dict(taskkwargs)
                if "countdown" in taskkwargscopy:
                    del taskkwargscopy["countdown"]
                if "eta" in taskkwargscopy:
                    del taskkwargscopy["etc"]
                taskkwargscopy["countdown"] = countdown
                retval = task.task(f, **taskkwargscopy)(*args, **kwargs) # if this fails, we'll get an exception back to the caller
            # else someone's already done this. So let's just stop.
        # else we're already scheduled to run in the future, So, let's just stop
        logging.debug("leave rundebouncedtask")
        return retval
    return rundebouncedtask
