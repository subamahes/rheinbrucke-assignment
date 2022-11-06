#!/usr/bin/env python

from flask import Blueprint
from flask import request

from rheinbrucke.utils.json_display import StatusCode
from rheinbrucke.service.finding_percentage import *

from rheinbrucke.utils.json_display import *

blueprint = Blueprint('finding_percentage', __name__)


#
# http://127.0.0.1:5000/
#

@blueprint.route('/finding-percentage', methods=['GET'])
def finding_percentage():
    try:

        result = service_finding_percentage()
        code = StatusCode.OK

    except BaseException as ex:

        code = StatusCode.SERVER_ERROR
        result = dict(
            error_message='Unexpected error while finding the percentage.'
        )

    return build_json_response(code, result)
