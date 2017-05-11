from taskutils import task
import logging
import math

def TaskTest1():
    def Go():
        @task
        def Length(inp):
            logging.info("Post, Length: %s" % len(inp))
            
        for exponent in range(3):
            length = int(math.pow(10, exponent + 5))
            logging.info("Pre, Length: %s" % length)
            linput = "X" * length
            lf = Length
            logging.debug("about to call lf")
            lf(linput)
        
    return "Tasks increasingly larger (look in logs for results)", Go
