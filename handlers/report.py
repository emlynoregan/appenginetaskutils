from google.appengine.ext import ndb
from flask import request, render_template
import json

def get_report(app):
    @app.route('/report')
    def report():
        keystr = request.args.get('key')
        level = int(request.args.get('level', 0))
        if keystr:
            key = ndb.Key(urlsafe=keystr)
            obj = key.get()
            def futuremap(future, flevel):
                if future:
                    urlsafe = future.key.urlsafe()
                    return "<a href='report?key=%s&level=%s'>%s</a>" % (urlsafe, flevel, urlsafe)
                else:
                    return None
                
            objjson = obj.to_dict(level = level, maxlevel = level + 5, futuremapf = futuremap)
            return render_template(
                "report.html", 
                objjson = json.dumps(objjson, indent=2, sort_keys=True),
                keystr = keystr
            )
        else:
            return render_template(
                "report.html"
            )

    return report

