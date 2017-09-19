import time
import datetime
import logging

def datetime_to_unixtimestampusec(aDateTime):
    return long(time.mktime(aDateTime.timetuple()) * 1000000 + aDateTime.microsecond) if aDateTime else 0

def get_utcnow_unixtimestampusec():
    return datetime_to_unixtimestampusec(datetime.datetime.utcnow())

def logdebug(debug, message):
    if debug:
        logging.debug(message)
        
