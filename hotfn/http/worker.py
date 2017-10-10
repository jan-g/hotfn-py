# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import functools
import io
import json
import os
import sys
import types

import traceback


from hotfn.http import errors
from hotfn.http import request
from hotfn.http import response
from hotfn.http import flow


def run(app, loop=None):
    """
    Request handler app dispatcher entry point
    :param app: request handler app
    :type app: types.Callable
    :param loop: asyncio event loop
    :type loop: asyncio.AbstractEventLoop
    :return: None
    """
    if not os.isatty(sys.stdin.fileno()):
        with os.fdopen(sys.stdin.fileno(), 'rb') as stdin:
            with os.fdopen(sys.stdout.fileno(), 'wb') as stdout:
                rq = request.RawRequest(stdin)
                while True:
                    print("Looping", file=sys.stderr)
                    try:
                        context, data = rq.parse_raw_request()
                        print("Headers for request are:", context.headers, file=sys.stderr)
                        if 'fnproject-flowid' in context.headers:
                            print("Dispatching continuation:", context.headers, file=sys.stderr)
                            rs = flow.dispatch(app, context, data=data, loop=loop)
                            rs.dump(stdout)
                        else:
                            rs = normal_dispatch(app, context,
                                                 data=data, loop=loop)
                            rs.dump(stdout)
                    except EOFError:
                        # The Fn platform has closed stdin; there's no way to
                        # get additional work.
                        return
                    except errors.DispatchException as ex:
                        # If the user's raised an error containing an explicit
                        # response, use that
                        ex.response().dump(stdout)
                    except Exception as ex:
                        traceback.print_exc(file=sys.stderr)
                        response.RawResponse(
                            (1, 1), 500, "Internal Server Error",
                            {}, str(ex)).dump(stdout)
                    sys.stderr.flush()


def normal_dispatch(app, context, data=None, loop=None):
    """
    Request handler app dispatcher
    :param app: request handler app
    :type app: types.Callable
    :param context: request context
    :type context: request.RequestContext
    :param data: request body
    :type data: io.BufferedIOBase
    :param loop: asyncio event loop
    :type loop: asyncio.AbstractEventLoop
    :return: raw response
    :rtype: response.RawResponse
    """
    try:
        flow.set_flow(function_id=context.headers['fn_app_name'] + context.headers['fn_path'])
        rs = app(context, data=data, loop=loop)
        if isinstance(rs, response.RawResponse):
            return rs
        elif isinstance(rs, types.CoroutineType):
            return loop.run_until_complete(rs)
        elif isinstance(rs, str):
            return response.RawResponse(context.version, 200, 'OK', {}, rs.encode('utf-8'))
        elif isinstance(rs, bytes):
            return response.RawResponse(
                context.version, 200, 'OK',
                {'content-type': 'application/octet-stream'}, rs)
        else:
            return response.RawResponse(
                context.version, 200, 'OK',
                {'content-type': 'application/json'}, json.dumps(rs))
    except errors.DispatchException as e:
        return e.response()
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        return response.RawResponse(
            context.version, 500, 'ERROR', {}, str(e))
    finally:
        flow.clear_flow(True)


def coerce_input_to_content_type(request_data_processor):

    @functools.wraps(request_data_processor)
    def app(context, data=None, loop=None):
        """
        Request handler app dispatcher decorator
        :param context: request context
        :type context: request.RequestContext
        :param data: request body
        :type data: io.BufferedIOBase
        :param loop: asyncio event loop
        :type loop: asyncio.AbstractEventLoop
        :return: raw response
        :rtype: response.RawResponse
        :return:
        """
        # TODO(jang): The content-type header has some internal structure;
        # actually provide some parsing for that
        content_type = context.headers.get("content-type")
        try:
            request_body = io.TextIOWrapper(data)
            # TODO(denismakogon): XML type to add
            if content_type == "application/json":
                body = json.load(request_body)
            elif content_type in ["text/plain"]:
                body = request_body.read()
            else:
                body = request_body.read()
        except Exception as ex:
            raise errors.DispatchException(
                500, "Unexpected error: {}".format(str(ex)))

        return request_data_processor(context, data=body, loop=loop)

    return app
