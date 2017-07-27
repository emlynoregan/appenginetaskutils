from task import task
import logging
import cloudstorage as gcs
from taskutils.future import future, FutureReadyForResult, get_children
from google.appengine.ext import ndb
from google.appengine.api import taskqueue

def gcsfileshardedpagemap(pagemapf=None, gcspath=None, initialshards = 10, pagesize = 100, **taskkwargs):
    @task(**taskkwargs)
    def MapOverRange(startpos, endpos, **kwargs):
        logging.debug("Enter MapOverRange: %s, %s" % (startpos, endpos))

        # open file at gcspath for read
        with gcs.open(gcspath) as gcsfile:
            page, ranges = hwalk(gcsfile, pagesize, initialshards, startpos, endpos) 

        if ranges:
            for arange in ranges:
                MapOverRange(arange[0], arange[1])

        if pagemapf:
            pagemapf(page)

        logging.debug("Leave MapOverRange: %s, %s" % (startpos, endpos))

    # get length of file in bytes
    filestat = gcs.stat(gcspath)
    
    MapOverRange(0, filestat.st_size)

 
def gcsfileshardedmap(mapf=None, gcspath=None, initialshards = 10, pagesize = 100, **taskkwargs):
    @task(**taskkwargs)
    def InvokeMap(line, **kwargs):
        logging.debug("Enter InvokeMap: %s" % line)
        try:
            mapf(line, **kwargs)
        finally:
            logging.debug("Leave InvokeMap: %s" % line)
     
    def ProcessPage(lines):
        for index, line in enumerate(lines):
            logging.debug("Line #%s: %s" % (index, line))
            InvokeMap(line)
 
    gcsfileshardedpagemap(ProcessPage, gcspath, initialshards, pagesize, **taskkwargs)


def futuregcsfileshardedpagemap(pagemapf=None, gcspath=None, pagesize=100, onsuccessf=None, onfailuref=None, onprogressf = None, weight = 1, parentkey=None, **taskkwargs):
    def OnSuccess(childfuture, arange, initialamount = 0):
        logging.debug("A: cfhasr=%s" % childfuture.has_result())
    
        parentfuture = childfuture.parentkey.get() if childfuture.parentkey else None
        logging.debug(childfuture)
        logging.debug(parentfuture)
        if parentfuture and not parentfuture.has_result():
            @ndb.transactional()
            def get_children_trans():
                return get_children(parentfuture.key)
            children = get_children_trans()
            
            logging.debug("children: %s" % [child.key for child in children])
            if children:
                result = initialamount
                error = None
                finished = True
                for childfuture in children:
                    logging.debug("childfuture: %s" % childfuture.key)
                    if childfuture.has_result():
                        try:
                            result += childfuture.get_result()
                            logging.debug("hasresult:%s" % result)
                        except Exception, ex:
                            logging.debug("haserror:%s" % repr(ex))
                            error = ex
                            break
                    else:
                        logging.debug("noresult")
                        finished = False
                         
                if error:
                    logging.debug("error: %s" % error)
                    parentfuture.set_failure(error)
                elif finished:
                    logging.debug("result: %s" % result)
                    parentfuture.set_success(result)#(result, initialamount, keyrange))
                else:
                    logging.debug("not finished")
            else:
                parentfuture.set_failure(Exception("no children found"))
    
    @ndb.transactional(xg=True)
    def OnFailure(childfuture):
        parentfuture = childfuture.parentkey.get() if childfuture.parentkey else None
        if parentfuture and not parentfuture.has_result():
            try:
                childfuture.get_result()
            except Exception, ex:
                parentfuture.set_failure(ex)
    
    def MapOverRange(startbyte, endbyte, weight, futurekey, **kwargs):
        logging.debug("Enter MapOverRange: %s, %s, %s" % (startbyte, endbyte, weight))
        try:
            # open file at gcspath for read
            with gcs.open(gcspath) as gcsfile:
                page, ranges = hwalk(gcsfile, pagesize, 2, startbyte, endbyte) 

            if ranges:
                newweight = (weight - len(page)) / len(ranges)
                for arange in ranges:
                    taskname = "%s-%s-%s-fgcsfspm" % (futurekey.id(), arange[0], arange[1])
    
                    def OnSuccessWithInitialAmount(childfuture):
                        OnSuccess(childfuture, arange, len(page))

                    try:
                        future(MapOverRange, parentkey=futurekey, includefuturekey = True, onsuccessf=OnSuccessWithInitialAmount, onfailuref=OnFailure, weight = newweight, futurename = taskname, **taskkwargs)(arange[0], arange[1], weight = newweight)
                    except taskqueue.TombstonedTaskError:
                        logging.debug("skip adding task (already been run)")
                    except taskqueue.TaskAlreadyExistsError:
                        logging.debug("skip adding task (already running)")
                
            if pagemapf:
                pagemapf(page)

            if ranges:
                raise FutureReadyForResult("still going")
            else:
                return len(page)
        finally:
            logging.debug("Leave MapOverRange: %s, %s, %s" % (startbyte, endbyte, weight))

    # get length of file in bytes
    filestat = gcs.stat(gcspath)

    filesizebytes = filestat.st_size    

    return future(MapOverRange, includefuturekey = True, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, parentkey=parentkey, weight = weight, **taskkwargs)(0, filesizebytes, weight)


 
def futuregcsfileshardedmap(mapf=None, gcspath=None, pagesize = 100, onsuccessf = None, onfailuref = None, onprogressf = None, weight= 1, parentkey = None, **taskkwargs):
    @task(**taskkwargs)
    def InvokeMap(line, **kwargs):
        logging.debug("Enter InvokeMap: %s" % line)
        try:
            mapf(line, **kwargs)
        finally:
            logging.debug("Leave InvokeMap: %s" % line)
     
    def ProcessPage(lines):
        for index, line in enumerate(lines):
            logging.debug("Line #%s: %s" % (index, line))
            InvokeMap(line)
 
    return futuregcsfileshardedpagemap(ProcessPage, gcspath, pagesize, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = None, parentkey=parentkey, weight=weight, **taskkwargs)


def hwalk(afile, pagesizeinlines, numranges, startbytes, endbytes):
    ## print(afile, pagesizeinlines, numranges, startbytes, endbytes)

    page = []
    ranges = []

    if startbytes <= 0:
        #1: we're at the start of the file, just start here
        afile.seek(0, 0)
    else:
        #2: skip first line if incomplete
        backcount = 1
        afile.seek(startbytes - backcount, 0)
        lbyte = afile.read(1)
        ## print("Byte: %s" % ord(lbyte))
        while ord(lbyte) >> 6 == 2:
            #the current byte is inside a multibyte UTF-8 character,
            #step back one byte and check again            
            ## print ("step back")
            if backcount >= startbytes:
                # we've reached the start of the file, can't go back any further.
                break  
            backcount += 1
            afile.seek(startbytes - backcount, 0)
            lbyte = afile.read(1)
            ## print("Byte: %s" % ord(lbyte))
        afile.seek(startbytes - backcount, 0)
        ## print ("before readline at %s" % afile.tell())
        afile.readline()


    ## print ("start: %s" % afile.tell())

    #3: get a page
    while len(page) < pagesizeinlines and afile.tell() < endbytes:
        line = afile.readline()
        page.append(line)

    rangesstartpos = afile.tell()
    ## print ("end: %s" % afile.tell())

    #4: calculate splits
    if rangesstartpos < endbytes:
        rangesize = float(endbytes - rangesstartpos) / numranges 
        ranges = [[int(rangesstartpos + rangeindex * rangesize), int(rangesstartpos + (rangeindex+1) * rangesize)] for rangeindex in range(numranges)]
        if ranges:
            ranges[-1][1] = endbytes # fixes possible floating point rounding errors at end of range
    ## print("ranges: %s" % ranges)

    return page, ranges
