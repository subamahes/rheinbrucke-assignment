#!/usr/bin/env python

from flask import Flask

from rheinbrucke.api import finding_percentage

from rheinbrucke.api import max_province



app = Flask(__name__)

app.register_blueprint(finding_percentage.blueprint)

app.register_blueprint(max_province.blueprint)

@app.after_request
def after_request(response):
    headers = response.headers
    headers['Access-Control-Allow-*'] = '*'
    headers['Access-Control-Allow-Credentials'] = True
    headers['Access-Control-Allow-Headers'] = '*'
    headers['Access-Control-Expose-Headers'] = '*'
    headers['Access-Control-Allow-Methods'] = '*'
    headers['Access-Control-Allow-Origin'] = '*'
    headers['node-cache'] = 'Missed node-cache'
    return response


if __name__ == '__main__':
    app.run()
