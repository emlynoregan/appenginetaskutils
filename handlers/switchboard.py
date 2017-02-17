from flask import render_template, request, redirect

from experiments.incrementaccountsnaive import IncrementAccountsExperimentNaive
from experiments.incrementaccountswithtask import IncrementAccountsWithTaskExperiment
from experiments.incrementaccountswithshardedmap import IncrementAccountsWithShardedMapExperiment
from experiments.deleteaccountswithshardedmap import DeleteAccountsWithShardedMapExperiment
from experiments.makeaccounts import MakeAccountsExperiment

def get_switchboard(app):
    experiments = [
        IncrementAccountsWithShardedMapExperiment(),
        IncrementAccountsWithTaskExperiment(),
        IncrementAccountsExperimentNaive(),
        DeleteAccountsWithShardedMapExperiment(),
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
                resultobj = experiment[1]()
                reporturl = "report%s" % (("?key=%s" % resultobj.urlsafe()) if resultobj else "")
                return redirect(reporturl)
