from flask import render_template, request, redirect

from experiments.incrementaccountsnaive import IncrementAccountsExperimentNaive
from experiments.incrementaccountswithtask import IncrementAccountsWithTaskExperiment
from experiments.incrementaccountswithshardedmap import IncrementAccountsWithShardedMapExperiment, IncrementAccountsWithFutureShardedMapExperiment
from experiments.deleteaccountswithshardedmap import DeleteAccountsWithShardedMapExperiment, DeleteAccountsWithFutureShardedMapExperiment
from experiments.makeaccounts import MakeAccountsExperiment
from experiments.countaccountswithfuture import CountAccountsWithFutureExperiment

def get_switchboard(app):
    experiments = [
        CountAccountsWithFutureExperiment(),
        IncrementAccountsWithFutureShardedMapExperiment(),
        IncrementAccountsWithShardedMapExperiment(),
        IncrementAccountsWithTaskExperiment(),
        IncrementAccountsExperimentNaive(),
        DeleteAccountsWithShardedMapExperiment(),
        DeleteAccountsWithFutureShardedMapExperiment(),
        MakeAccountsExperiment(),
    ]

    @app.route('/', methods=["GET", "POST"])
    def switchboard():
        if request.method == "GET":
            return render_template("switchboard.html", experiments = enumerate(experiments))
        else:
            if not request.form.get("run") is None:
                index = int(request.form.get("run"))
                experiment = experiments[index]
                resultkey = experiment[1]()
                reporturl = "report%s" % (("?key=%s" % resultkey.urlsafe()) if resultkey else "")
                return redirect(reporturl)
