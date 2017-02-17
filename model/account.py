from google.appengine.ext import ndb

class Account(ndb.model.Model):
    stored = ndb.DateTimeProperty(auto_now_add=True) 
    updated = ndb.DateTimeProperty(auto_now=True) 
    balance = ndb.IntegerProperty()

    def to_dict(self):
        return {
            "key": self.key.urlsafe() if self.key else None,
            "stored": str(self.stored) if self.stored else None,
            "updated": str(self.updated) if self.stored else None,
            "balance": self.balance
        }
        