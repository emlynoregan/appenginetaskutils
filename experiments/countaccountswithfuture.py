from model.account import Account
from taskutils import future, FutureUnderwayError
import logging

def CountAccountsWithFutureExperiment():
    def Go():
        def CountRemaining(cursor, futurekey):
            accounts, cursor, kontinue = Account.query().fetch_page(
                100, start_cursor = cursor
            )
            
            numaccounts = len(accounts)
            
            if kontinue:
                def OnSuccess(childfuture):
                    parentfuture = futurekey.get()
                    try:
                        if childfuture and parentfuture:
                            result = numaccounts + childfuture.get_result()
                            parentfuture.set_success(result)
                    except Exception, ex:
                        logging.exception(ex)
                        raise ex
#                     if childfuture:
#                         childfuture.key.delete()
                        
                def OnFailure(childfuture):
                    parentfuture = futurekey.get()
                    if childfuture and parentfuture:
                        try:
                            childfuture.get_result() # should throw
                        except Exception, ex:
                            parentfuture.set_failure(ex)
#                     if childfuture:
#                         childfuture.key.delete()
        
                future(CountRemaining, parentkey=futurekey, includefuturekey=True, queue="background", onsuccessf=OnSuccess, onfailuref=OnFailure)(cursor)
                                
                raise FutureUnderwayError("still calculating")
            else:
                return numaccounts
        
        countfuture = future(CountRemaining, includefuturekey=True, queue="background")(None)
        return countfuture.key
        
    return "Count Accounts With Future", Go
