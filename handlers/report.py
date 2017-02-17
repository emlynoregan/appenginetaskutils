from google.appengine.ext import ndb
from flask import request, render_template
import json

def get_report(app):
    @app.route('/report')
    def report():
        keystr = request.args.get('key')
        if keystr:
            key = ndb.Key(urlsafe=keystr)
            obj = key.get()
            return render_template(
                "report.html", 
                objjson = json.dumps(obj.to_dict(), indent=2, sort_keys=True),
                keystr = keystr
            )
        else:
            return render_template(
                "report.html"
            )
            

    return report
