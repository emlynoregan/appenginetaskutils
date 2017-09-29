import cloudpickle
import hashlib
from taskutils.util import logdebug

def make_flash(f, *args, **kwargs):
    flash = hashlib.md5(
            cloudpickle.dumps((f, args, kwargs))
        ).hexdigest()
    logdebug(flash)
    return flash
