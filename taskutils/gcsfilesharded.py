from task import task
import logging
import cloudstorage as gcs

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
