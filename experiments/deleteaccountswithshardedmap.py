from model.account import Account
from taskutils import shardedmap, futureshardedmap

def DeleteAccountsWithShardedMapExperiment():
    def Go():
        def DeleteAccount(account):
            account.key.delete()

        shardedmap(DeleteAccount, ndbquery = Account.query())            
    return "Delete Accounts With Sharded Map", Go

def DeleteAccountsWithFutureShardedMapExperiment():
    def Go():
        def DeleteAccount(account):
            account.key.delete()

        return futureshardedmap(DeleteAccount, ndbquery = Account.query()).key
    return "Delete Accounts With Future Sharded Map", Go
