from model.account import Account
from google.appengine.ext import ndb

def IncrementAccountsExperimentNaive():
    def Go():
        def AddFreeCredit(creditamount):
            def ProcessOnePage(cursor):
                accounts, cursor, kontinue = Account.query().fetch_page(
                    100, start_cursor = cursor
                )
                for account in accounts:
                    account.balance += creditamount
                ndb.put_multi(accounts)
                if kontinue:
                    ProcessOnePage(cursor)
            ProcessOnePage(None)
        AddFreeCredit(10)
    return "Increment Accounts (Naive)", Go
