#!/usr/bin/env python

from flask import Blueprint
from flask import request

from rheinbrucke.utils.json_display import StatusCode
from rheinbrucke.service.max_province import *

from rheinbrucke.utils.json_display import *

blueprint = Blueprint('max_province', __name__)


#
# http://127.0.0.1:5000/
#

@blueprint.route('/max-province', methods=['GET'])
def max_province():
    try:

        result = service_max_province()
        code = StatusCode.OK

    except BaseException as ex:

        code = StatusCode.SERVER_ERROR
        result = dict(
            error_message='Unexpected error while finding the max - province.'
        )

    return build_json_response(code, result)
