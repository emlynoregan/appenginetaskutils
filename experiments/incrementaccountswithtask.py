from model.account import Account
from google.appengine.ext import ndb
from taskutils import task

def IncrementAccountsWithTaskExperiment():
    def Go():
        def AddFreeCredit(creditamount):
            @task
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
    return "Increment Accounts With Task", Go


