from taskutils.memcacher import memcacher
from taskutils.gcscacher import gcscacher

def CachingExperiment():
    def Go():
        def SimpleFunc(a):
            return a+1

        memcacher(SimpleFunc, debug = True)(1)
        memcacher(SimpleFunc, cachekey = "acachekey", debug = True)(2)
        memcacher(SimpleFunc, expiresec=20, debug = True)(3)
        memcacher(SimpleFunc, debug = True)(1)
        memcacher(SimpleFunc, cachekey = "acachekey", debug = True)(2)
        memcacher(SimpleFunc, expiresec=20, debug = True)(3)

        def SomeText():
            return "here is some text"
                
        gcscacher(SomeText, debug = True)()
        gcscacher(SomeText, cachekey="anothercachekey", debug = True)()
        gcscacher(SomeText, expiresec=60, debug=True)()
        gcscacher(SomeText, bucketname="somebucketname", debug=True)()
        gcscacher(SomeText, debug = True)()
        gcscacher(SomeText, cachekey="anothercachekey", debug = True)()
        gcscacher(SomeText, expiresec=60, debug=True)()
        gcscacher(SomeText, bucketname="somebucketname", debug=True)()
                
    return "Caching Experiment (results in log)", Go
