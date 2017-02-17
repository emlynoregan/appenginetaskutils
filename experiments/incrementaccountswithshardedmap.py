from model.account import Account
from taskutils import shardedmap

def IncrementAccountsWithShardedMapExperiment():
    def Go():
        def AddFreeCredit(creditamount):
            def IncrementBalance(account):
                account.balance += creditamount
                account.put()
                
            shardedmap(IncrementBalance, Account.query())
        AddFreeCredit(10)
    return "Increment Accounts With Sharded Map", Go
