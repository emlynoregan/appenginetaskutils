from model.account import Account
from google.appengine.ext import ndb
from taskutils import task
import logging
def MakeAccountsExperiment():
    def Go():
        @task(includeheaders=True)
        def MakeAccounts(numaccounts, headers):
            logging.debug(headers)
            logging.debug(numaccounts)
            if numaccounts <= 10:
                accounts = []
                for _ in range(numaccounts):
                    account = Account()
                    account.balance = 0
                    accounts.append(account)
                ndb.put_multi(accounts)
            else:
                doaccounts = numaccounts
                while doaccounts > 0:
                    batch = (numaccounts / 10) if ((numaccounts / 10) <= doaccounts) else doaccounts
                    MakeAccounts(batch)
                    doaccounts -= batch
        
        MakeAccounts(1000)
    return "Make Accounts", Go
