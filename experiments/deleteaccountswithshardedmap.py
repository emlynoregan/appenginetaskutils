from model.account import Account
from taskutils.ndbsharded import ndbshardedmap, futurendbshardedmap

def DeleteAccountsWithShardedMapExperiment():
    def Go():
        def DeleteAccount(account):
            account.key.delete()

        ndbshardedmap(DeleteAccount, ndbquery = Account.query())            
    return "Delete Accounts With Sharded Map", Go

def DeleteAccountsWithFutureShardedMapExperiment():
    def Go():
        def DeleteAccount(account):
            account.key.delete()

        return futurendbshardedmap(DeleteAccount, ndbquery = Account.query()).key
    return "Delete Accounts With Future Sharded Map", Go
