import functools
from taskutils.flash import make_flash
import logging
from google.appengine.api import app_identity
import os
import cloudstorage as gcs
import json
from taskutils.util import get_utcnow_unixtimestampusec, logdebug

def gcscacher(f, bucketname=None, cachekey=None, expiresec = None, debug=False):
    if not f:
        return functools.partial(gcscacher, expiresec=expiresec)

    def getvalue(*args, **kwargs):
        key = make_flash(f, args, kwargs)
        logdebug(debug, "Enter gcscacher.getvalue: %s" % key)

        bucket = bucketname if bucketname else os.environ.get(
                                                        'BUCKET_NAME',
                                                    app_identity.get_default_gcs_bucket_name())
        
        metapath = "/%s/_gcscacher/%s/meta.json" % (bucket, key)
        contentpath = "/%s/_gcscacher/%s/content.json" % (bucket, key)

        logdebug(debug, "metapath: %s" % metapath)
        logdebug(debug, "contentpath: %s" % contentpath)

        meta = None
        try:
            #1: Get the meta info
            with gcs.open(metapath) as metafile:
                meta = json.load(metafile)
        except gcs.NotFoundError:
            pass
        except:
            logging.exception("loading metafile for %s" % key)
        
        content = None
        cacheIsValid = False
        if meta and not (meta.get("expireat") and meta.get("expireat") < get_utcnow_unixtimestampusec()):
            try:
                with gcs.open(contentpath) as contentfile:
                    content = contentfile.read()
                cacheIsValid = True
            except gcs.NotFoundError:
                pass

        if not cacheIsValid:
            logdebug(debug, "GCS Cache miss")
            content = f(*args, **kwargs)
        else:
            logdebug(debug, "GCS Cache hit")

        if not cacheIsValid:
            logdebug(debug, "write content back to gcs")
            with gcs.open(contentpath, "w") as contentfilewriter:
                contentfilewriter.write(content)
                
            lmeta = {
                "expireat": get_utcnow_unixtimestampusec() + (expiresec * 1000000) if expiresec else None
            }

            with gcs.open(metapath, "w") as metafilewriter:
                json.dump(lmeta, metafilewriter)

        logdebug(debug, "Leave gcscacher.getvalue: %s" % key)
        return content

    return getvalue
    