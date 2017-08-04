from task import task
import logging
import cloudstorage as gcs
from future import future, FutureReadyForResult, GenerateOnAllChildSuccess #get_children
from future import setlocalprogress, generatefuturepagemapf

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


def futuregcsfileshardedpagemap(pagemapf=None, gcspath=None, pagesize=100, onsuccessf=None, onfailuref=None, onprogressf = None, onallchildsuccessf = None, initialresult = None, oncombineresultsf = None, weight = 1, parentkey=None, **taskkwargs):
    def MapOverRange(futurekey, startbyte, endbyte, weight, **kwargs):
        logging.debug("Enter MapOverRange: %s, %s, %s" % (startbyte, endbyte, weight))

        linitialresult = initialresult if not initialresult is None else 0
        loncombineresultsf = oncombineresultsf if oncombineresultsf else lambda a, b: a + b
    
        try:
            # open file at gcspath for read
            with gcs.open(gcspath) as gcsfile:
                page, ranges = hwalk(gcsfile, pagesize, 2, startbyte, endbyte) 

            if pagemapf:
                lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, linitialresult, loncombineresultsf)
                taskkwargs["futurename"] = "pagemap %s of %s,%s" % (len(page), startbyte, endbyte)
                future(pagemapf, parentkey=futurekey, onallchildsuccessf=lonallchildsuccessf, weight = len(page), **taskkwargs)(page)
            else:
                setlocalprogress(futurekey, len(page))

            if ranges:
                newweight = (weight - len(page)) / len(ranges)
                for arange in ranges:
                    taskkwargs["futurename"] = "shard %s" % (arange)

                    lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, linitialresult if pagemapf else len(page), loncombineresultsf)

                    future(MapOverRange, parentkey=futurekey, onallchildsuccessf=lonallchildsuccessf, weight = newweight, **taskkwargs)(arange[0], arange[1], weight = newweight)
                
            if ranges or pagemapf:
                raise FutureReadyForResult("still going")
            else:
                return len(page)
        finally:
            logging.debug("Leave MapOverRange: %s, %s, %s" % (startbyte, endbyte, weight))

    # get length of file in bytes
    filestat = gcs.stat(gcspath)

    filesizebytes = filestat.st_size    

    futurename = "top level 0 to %s" % (filesizebytes)

    taskkwargscopy = dict(taskkwargs)
    taskkwargscopy["futurename"] = taskkwargscopy.get("futurename", futurename)

    return future(MapOverRange, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, onallchildsuccessf=onallchildsuccessf, parentkey=parentkey, weight = weight, **taskkwargscopy)(0, filesizebytes, weight)

 
def generategcsinvokemapf(mapf):
    def InvokeMap(futurekey, line, **kwargs):
        logging.debug("Enter InvokeMap: %s" % line)
        try:
            return mapf(line, **kwargs)
        finally:
            logging.debug("Leave InvokeMap: %s" % line)
    return InvokeMap

def futuregcsfileshardedmap(mapf=None, gcspath=None, pagesize = 100, onsuccessf = None, onfailuref = None, onprogressf = None, onallchildsuccessf=None, initialresult = None, oncombineresultsf = None, weight= None, parentkey = None, **taskkwargs):
    invokeMapF = generategcsinvokemapf(mapf)
    pageMapF = generatefuturepagemapf(invokeMapF, initialresult, oncombineresultsf **taskkwargs)
    return futuregcsfileshardedpagemap(pageMapF, gcspath, pagesize, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, onallchildsuccessf=onallchildsuccessf, initialresult = initialresult, oncombineresultsf = oncombineresultsf, parentkey=parentkey, weight=weight, **taskkwargs)


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
