#!/usr/bin/env python

from enum import IntEnum

import flask
import json

class StatusCode(IntEnum):
    OK = 200
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    UNPROCESSABLE_ENTITY = 422
    SERVER_ERROR = 500

    def is_2xx_successful(self):
        value = int(self)
        return 200 <= value < 300

    def is_4xx_client_error(self):
        value = int(self)
        return 400 <= value < 500

    def is_5xx_server_error(self):
        value = int(self)
        return 500 <= value < 600

def build_json_response(code, body):
    body_json = json.dumps(body, sort_keys=True, default=str, indent=4)

    response = flask.make_response(body_json, int(code))
    response.mimetype = 'application/json'
    return response
