import time
import datetime
import logging
import types
import cloudpickle
import taskutils

def datetime_to_unixtimestampusec(aDateTime):
    return long(time.mktime(aDateTime.timetuple()) * 1000000 + aDateTime.microsecond) if aDateTime else 0

def get_utcnow_unixtimestampusec():
    return datetime_to_unixtimestampusec(datetime.datetime.utcnow())

def logdebug(message):
    if taskutils.get_logging():
        logging.debug(message)
        
def logwarning(message):
    if taskutils.get_logging():
        logging.warning(message)

def logexception(message):
    if taskutils.get_logging():
        logging.exception(message)

def get_dump():
    return taskutils.get_dump()

def dumper(thing):
    if taskutils.get_dump():
        def printf(f, indent, foundf):
            logdebug("%s [%s] %s" % ("#" * (indent+1), len(cloudpickle.dumps(f)), f))
            logdebug("%s code size = %s" % ("#" * (indent+1), len(cloudpickle.dumps(f.func_code))))
#             print "%s closure size = %s" % ("#" * indent, len(cloudpickle.dumps(f.func_closure)))
            dodumpclosure(f, (indent+1), foundf + [f])
                    
        def printi(arg, indent):
            logdebug("%s [%s] %s" % ("#" * (indent+1), len(cloudpickle.dumps(arg)), arg))

        def printlen(obj, indent):
            logdebug("%s [%s] %s" % ("#" * (indent+1), len(obj), type(obj)))
    
        def printmsg(msg, indent):
            logdebug("%s %s" % ("*" * (indent+1), msg))
    
        def dodumpitem(item, indent, foundf):
            if isinstance(item, types.FunctionType):
                printf(item, indent, foundf)
            elif isinstance(item, dict):
                printlen(item, indent)
                for key, value in item.iteritems():
                    printmsg(key, indent)
                    dodumpitem(value, indent+1, foundf)
            elif isinstance(item, (list, set, tuple)):
                printlen(item, indent)
                for index, elem in enumerate(item):
                    printmsg(index, indent)
                    dodumpitem(elem, indent+1, foundf)
            else:
                printi(item, indent) # not a function
    
        def dodumpclosure(f, indent, foundf):
            if f.func_closure:
                for item in [lcell.cell_contents for lcell in f.func_closure]:
                    if isinstance(item, types.FunctionType):
                        if f == item:
                            printmsg("%s - Function in its own closure, stop traverse" % item, indent)
                        elif item in foundf:
                            printmsg("%s - Function in ancestor closure, stop traverse" % item, indent)
                        else:
                            dodumpitem(item, indent, foundf)
                    else:
                        dodumpitem(item, indent, foundf)
            else:
                printmsg("null closure", indent)
    
        dodumpitem(thing, 0, [])