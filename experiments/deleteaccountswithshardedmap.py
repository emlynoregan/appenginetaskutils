from model.account import Account
from taskutils import shardedmap

def DeleteAccountsWithShardedMapExperiment():
    def Go():
        def DeleteAccount(account):
            account.key.delete()

        shardedmap(DeleteAccount, ndbquery = Account.query())            
    return "Delete Accounts With Sharded Map", Go
