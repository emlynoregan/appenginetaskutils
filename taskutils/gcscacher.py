import functools
from taskutils.flash import make_flash
from google.appengine.api import app_identity
import os
import cloudstorage as gcs
from taskutils.util import get_utcnow_unixtimestampusec, logdebug
import pickle
import cloudpickle

def gcscacher(f, bucketname=None, cachekey=None, expiresec = None):
    if not f:
        return functools.partial(gcscacher, expiresec=expiresec)

    def getvalue(*args, **kwargs):
        key = cachekey if cachekey else make_flash(f, args, kwargs)
        logdebug("Enter gcscacher.getvalue: %s" % key)

        bucket = bucketname if bucketname else os.environ.get(
                                                        'BUCKET_NAME',
                                                    app_identity.get_default_gcs_bucket_name())
        
        lpicklepath = "/%s/gcscache/%s.pickle" % (bucket, key)

        logdebug("picklepath: %s" % lpicklepath)

        lsaved = None
        try:
            #1: Get the meta info
            with gcs.open(lpicklepath) as picklefile:
                lsaved = pickle.load(picklefile)
        except gcs.NotFoundError:
            pass
        
        lexpireat = lsaved.get("expireat") if lsaved else None
        lcontent = None
        lcacheIsValid = False
        if lsaved and not (lexpireat and lexpireat < get_utcnow_unixtimestampusec()):
            lcontent = lsaved.get("content")
            lcacheIsValid = True

        if not lcacheIsValid:
            logdebug("GCS Cache miss")
            lcontent = f(*args, **kwargs)
            logdebug("write content back to gcs")
            ltosave = {
                "expireat": get_utcnow_unixtimestampusec() + (expiresec * 1000000) if expiresec else None,
                "content": lcontent
            }
            with gcs.open(lpicklepath, "w") as picklefilewrite:
                cloudpickle.dump(ltosave, picklefilewrite)
        else:
            logdebug("GCS Cache hit")

        logdebug("Leave gcscacher.getvalue: %s" % key)

        return lcontent

    return getvalue
    