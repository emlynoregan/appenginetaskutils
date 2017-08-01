from model.account import Account
import logging
from taskutils.future import future, FutureReadyForResult, GenerateOnAllChildSuccess,\
    setlocalprogress

def CountAccountsWithFutureExperiment():
    def Go():
        def CountRemaining(futurekey, cursor):
            logging.debug("Got here")
            accounts, cursor, kontinue = Account.query().fetch_page(
                100, start_cursor = cursor
            )

            numaccounts = len(accounts)
            
            if kontinue:
                lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, numaccounts, lambda a, b: a + b)
                
                future(CountRemaining, parentkey=futurekey, queue="background", onallchildsuccessf = lonallchildsuccessf)(cursor)
                                
                logging.debug("raising")

            setlocalprogress(futurekey, numaccounts)
            
            if kontinue:
                raise FutureReadyForResult("still calculating")
            else:
                logging.debug("leaving")
                return numaccounts
        
        countfuture = future(CountRemaining, queue="background")(None)
        return countfuture.key
        
    return "Count Accounts With Future", Go
