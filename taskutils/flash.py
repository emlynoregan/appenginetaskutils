import cloudpickle
import hashlib

def make_flash(f, *args, **kwargs):
    return hashlib.md5(
            cloudpickle.dumps((f, args, kwargs))
        ).hexdigest()
