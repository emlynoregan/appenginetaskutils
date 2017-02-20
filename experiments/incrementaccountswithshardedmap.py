from model.account import Account
from taskutils import shardedmap
import logging

def IncrementAccountsWithShardedMapExperiment():
    def Go():
        def AddFreeCredit(creditamount):
            def IncrementBalance(account, headers):
#                 headers = kwargs.get("headers")
                logging.debug("headers: %s" % headers)
                account.balance += creditamount
                account.put()
                
            shardedmap(IncrementBalance, Account.query(), includeheaders = True)
        AddFreeCredit(10)
    return "Increment Accounts With Sharded Map", Go
