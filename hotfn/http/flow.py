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

from cgi import parse_header
import dill
import inspect
import io
import os
import requests
import threading
import sys

from hotfn.http.request import read_headers
from hotfn.http import response


_LOCK = threading.Lock()

_FLOW = None
_FUNCTION_ID = None


def _flow():
    global _FLOW
    with _LOCK:
        if _FLOW is None:
            _FLOW = FlowClient()
        return _FLOW


def set_flow(function_id=None, thread_id=None):
    global _FLOW, _FUNCTION_ID
    with _LOCK:
        _FUNCTION_ID = function_id
        if thread_id is None:
            _FLOW = None
        else:
            _FLOW = FlowClient(thread=thread_id)
        return _FLOW


def clear_flow(commit=False):
    global _FLOW, _FUNCTION_ID
    with _LOCK:
        _FUNCTION_ID = None
        if commit and _FLOW is not None:
            _FLOW.commit()
        _FLOW = None


def _request(method, path):
    path


class FlowClient(object):
    def __init__(self, thread=None):
        self.base_url = os.environ['COMPLETER_URL']
        self.function_id = _FUNCTION_ID
        if thread is None:
            self.createThread()
        else:
            self.thread_id = thread

    def post(self, path, headers=None, body=None, **kwargs):
        return self.request('POST', path, headers=headers, body=body, **kwargs)

    def get_stream(self, path, body=None, **kwargs):
        return self.request_stream('GET', path, body=body, **kwargs)

    def request(self, method, path, headers=None, body=None, **kwargs):
        if '{' in path:
            path = path.format(flow=self.thread_id, **kwargs)
        response = requests.request(method, self.base_url + path, headers=headers, data=body)
        response.raise_for_status()
        try:
            return response.headers, response.json()
        except:
            return response.headers, None

    def request_stream(self, method, path, headers=None, body=None, **kwargs):
        if '{' in path:
            path = path.format(flow=self.thread_id, **kwargs)
        response = requests.request(method, self.base_url + path, headers=headers, data=body)
        response.raise_for_status()
        try:
            return response.headers, BytesIO(response.content)
        except:
            return response.headers, None

    def createThread(self):
        resp_h, resp_b = self.post("/graph?functionId={}".format(self.function_id))
        print(resp_h, file=sys.stderr)
        self.thread_id = resp_h['fnproject-flowid']
        return self

    def commit(self):
        resp_h, resp_b = self.post("/graph/{flow}/commit")
        print("commit:", resp_h, file=sys.stderr)
        return self


class BytesIO(io.BytesIO):
    def readall(self):
        b = bytes()
        while True:
            part = self.read()
            if part is None:
                continue
            if len(part) == 0:
                return b
            b += part


def supply(fn):
    fns = dill.dumps(fn)
    print("dumping function, len(fns) is", len(fns), "and fns is", fns, file=sys.stderr)
    h, b = _flow().post('/graph/{flow}/supply',
                        headers={'FnProject-DatumType': 'blob',
                                 'content-type': 'application/python-serialized',
                                 'content-length': str(len(fns)),
                                 'FnProject-CodeLocation': '-',
                                 },
                        body=fns)

    print("response:", h, b, file=sys.stderr)
    return Stage(fn, stage_id=h['fnproject-stageid'])


def all_of(*stages):
    h, b = _flow().post("/graph/{flow}/allOf?cids={stage_ids}",
                        stage_ids=",".join(stage.stage_id for stage in stages))

    return Stage(stage_id=h['fnproject-stageid'])


class Stage(object):
    def __init__(self, fn=None, stage_id=None):
        self.fn = fn
        self.stage_id = stage_id

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def get(fn):
        h, s = _flow().get_stream('/graph/{flow}/stage/{stage_id}', stage_id=fn.stage_id)
        if h.get('fnproject-resultstatus', 'success') == 'failure':
            raise CompletionError(read_datum(h, s))
        return read_datum(h, s)

    def then_run(fn, f):
        return fn._then(f, type='thenRun')

    def then_apply(fn, f):
        return fn._then(f, type='thenApply')

    def then(fn, f):
        if len(inspect.signature(f).parameters) == 0:
            return fn.then_run(f)
        else:
            return fn.then_apply(f)

    def _then(fn, f, type):
        fns = dill.dumps(f)
        print("dumping function, len(fns) is", len(fns), "and fns is", fns,
              file=sys.stderr)
        h, b = _flow().post('/graph/{flow}/stage/{stage_id}/{type}',
                            headers={'FnProject-DatumType': 'blob',
                                     'content-type': 'application/python-serialized',
                                     'content-length': str(len(fns)),
                                     'FnProject-CodeLocation': '-',
                                     },
                            body=fns,
                            stage_id=fn.stage_id,
                            type=type)

        print("response:", h, b, file=sys.stderr)
        return Stage(fn=f, stage_id=h['fnproject-stageid'])


class CompletionError(Exception):
    pass


def read_datum(headers, body_stream):
    print("Reading datum ", headers, file=sys.stderr)
    mime_type, params = parse_header(headers.get('content-type', 'application/octet-stream'))
    datum_type = headers.get('fnproject-datumtype')
    try:
        if mime_type == 'multipart/form-data':
            # The completer marshals calls wrapped like this
            items = []
            reader = BoundaryStreamGenerator(body_stream, params['boundary'])
            reader.current_stream().close()  # Skip the preamble
            while not reader.at_end:
                item_headers, item_stream = read_headers(reader.current_stream())
                print("item-headers are", item_headers, file=sys.stderr)
                items.append(read_datum(item_headers, item_stream))
            return items
        elif datum_type == 'blob' and mime_type == 'application/python-serialized':
            all = body_stream.readall()
            print("Deserialising object - read", len(all), "bytes", file=sys.stderr)
            return dill.loads(all)
        elif datum_type == 'empty':
            return None
        else:
            print("Unknown datum:", mime_type, params, datum_type, file=sys.stderr)
            raise OSError("Unknown datum")
    finally:
        # Consume post-amble
        body_stream.close()


class BoundaryStreamGenerator(object):
    def __init__(self, stream, boundary):
        self.stream = stream
        self.boundary = b'\r\n--' + bytes(boundary, 'ascii')
        self._current_stream = None
        self.at_end = False

    def current_stream(self):
        if self.at_end:
            raise OSError("Multipart stream exhausted")
        print("DEBUG: current_stream is ", self._current_stream, file=sys.stderr)
        if self._current_stream is None:
            self._current_stream = BoundaryStream(self.stream, self.boundary, self)
            print("DEBUG:     ... updated to ", self.current_stream, file=sys.stderr)
        return self._current_stream

    def _clear_stream(self):
        self._current_stream = None


class BoundaryStream(object):

    def __init__(self, stream, boundary, generator):
        self.stream = stream
        self.boundary = boundary
        self.at_start = True
        self.at_end = False
        self.closed = False
        self.chars = None
        self.generator = generator

    def close(self):
        """
        Close the stream.

        That means reading until the next boundary.
        """
        if not self.closed:
            while self.read() != b'':
                pass
            self.generator._clear_stream()
        self.closed = True

    def read(self, size=-1):
        """
        Read at most size bytes, returned as bytes.

        Return an empty bytes object at EOF.
        """
        if self.closed:
            raise OSError("Operation on closed stream")
        if self.at_end:
            return bytes()

        if self.chars is not None:
            if size == -1 or size > len(self.chars):
                size = len(self.chars)
            result = self.chars[:size if size > 0 else len(self.chars)]
            self.chars = self.chars[size:]
            if len(self.chars) == 0:
                self.chars = None
            return result

        c = self.stream.read(1)
        # print("read: ", c, file=sys.stderr)
        if c is None:
            return c
        if len(c) == 0:
            if False:
                self.at_end = True
                raise OSError("Unbounded multipart stream closed before end boundary")
            else:
                # Permit a softer approach to stream handling
                print("WARNING: Unbounded multipart stream closed before end boundary")
                self.at_end = True
                self.generator._clear_stream()
                self.generator.at_end = True
                return bytes()

        if c == b'\r':
            index = 0
        elif c == b'-' and self.at_start:
            index = 2
        else:
            self.at_start = False
            return c

        # Do we have a boundary marker?
        chars = c
        while index < len(self.boundary):
            if ord(c) != self.boundary[index]:
                print("boundary not read, char is ", c, " index is ", index, " and self.boundary=", self.boundary,
                      "character at index =", self.boundary[index], file=sys.stderr)
                break
            index += 1
            c = self.stream.read(1)
            if c is None or len(c) == 0:
                raise OSError("Unbounded multipart stream closed during boundary scan")
            chars += c
        else:
            print("read boundary, checking for end", file=sys.stderr)
            # No break, must have found the end.
            # Are we followed by '\r\n' or '--\r\n' ?
            if c == b'\r':
                c = self.stream.read(1)
                chars += c
                if c == b'\n':
                    # We've found a part boundary
                    print("   part boundary", file=sys.stderr)
                    self.at_end = True
                    self.generator._clear_stream()
                    return bytes()
            elif c == b'\-':
                c = self.stream.read(1)
                chars += c
                if c == b'\-':
                    c = self.stream.read(1)
                    chars += c
                    if c == b'\r':
                        c = self.stream.read(1)
                        chars += c
                        if c == b'\n':
                            # We've found the terminal boundary
                            print("   stream boundary", file=sys.stderr)
                            self.at_end = True
                            self.generator._clear_stream()
                            self.generator.at_end = True
                            return bytes()
        print("no boundary detected, chars=", chars, file=sys.stderr)

        # We found some discrepancy in the boundary, save what we have
        self.chars = chars[1:]
        return chars[0:1]

    def readline(self):
        print("READLINE called", file=sys.stderr)
        l = bytes()
        while not self.at_end:
            c = self.read(1)
            if c is None:
                continue
            if len(c) == 0:
                return l
            l += c
            if c == b'\n':
                return l
        print("  READLINE returned with eof before newline", file=sys.stderr)
        return l

    def readable(self):
        """
        True if file was opened in a read mode.
        """
        return True

    def readall(self, *args, **kwargs):
        """
        Read all data from the file, returned as bytes.

        In non-blocking mode, returns as much as is immediately available,
        or None if no data is available.  Return an empty bytes object at EOF.
        """
        if self.closed:
            raise OSError("Operation on closed stream")
        val = bytes()
        while not self.at_end:
            more = self.read()
            if more == b'':
                break
            val += more
        return val

    def seek(self):
        """
        Move to new file position and return the file position.

        Argument offset is a byte count.  Optional argument whence defaults to
        SEEK_SET or 0 (offset from start of file, offset should be >= 0);
        other values are SEEK_CUR or 1 (move relative to current position,
         positive or negative),
        and SEEK_END or 2 (move relative to end of file,
         usually negative, although
        many platforms allow seeking beyond the end of a file).

        Note that not all file objects are seekable.
        """
        raise OSError()

    def seekable(self):
        """ True if file supports random-access. """
        return False

    def tell(self):
        """
        Current file position.

        Can raise OSError for non seekable files.
        """
        raise OSError()

    def truncate(self):
        """
        Truncate the file to at most size bytes and return
         the truncated size.

        Size defaults to the current file position, as returned by tell().
        The current file position is changed to the value of size.
        """
        raise OSError()

    def writable(self):
        """ True if file was opened in a write mode. """
        return False

    def write(self):
        """
        Write buffer b to file, return number of bytes written.

        Only makes one system call, so not all of the data may be written.
        The number of bytes actually written is returned.
          In non-blocking mode,
        returns None if the write would block.
        """
        raise OSError()

    def __repr__(self):
        """ Return repr(self). """
        return "BoundaryStream({!r})".format(
            self.boundary)

    @property
    def closefd(self):
        """True if the file descriptor will be closed by close()."""
        return False

    @property
    def mode(self):
        """String giving the file mode"""
        return "rb"


def dispatch(app, context, data=None, loop=None):
    print("Dispatching continuation:", context.headers, file=sys.stderr)
    set_flow(function_id=context.headers['fn_app_name'] + context.headers['fn_path'],
             thread_id=context.headers['fnproject-flowid'])

    args = read_datum(context.headers, data)

    print("Args are", args, file=sys.stderr)
    try:
        result = args[0](*args[1:])
        print("Result is", result, file=sys.stderr)

        return frame_response(context, 200, {'fnproject-datumtype': 'blob',
                                             'fnproject-resultstatus': 'success',
                                             'content-type': 'application/python-serialized',
                                             },
                              result)
    except Exception as e:
        return frame_response(context, 500, {'fnproject-datumtype': 'blob',
                                             'fnproject-resultstatus': 'failure',
                                             'content-type': 'application/python-serialized',
                                             },
                              e)
    finally:
        clear_flow()


def frame_response(context, status, headers, result):
    if result is None:
        body = bytes()
        headers.update({'fnproject-datumtype': 'empty',
                        })
    else:
        body = dill.dumps(result)
        headers.update({'fnproject-datumtype': 'blob',
                        'content-type': 'application/python-serialized',
                        })

    h = "\r\n".join(name + ": " + headers[name] for name in headers)
    result = "HTTP/1.1 {status} INVOKED\r\n" \
             "Content-Length: {cl}\r\n" \
             "{headers}\r\n" \
             "\r\n".format(status=status,
                           cl=len(body),
                           headers=h,
                           ).encode("ascii")
    print(str(result), file=sys.stderr)
    result += body
    return response.RawResponse(
                context.version, 200, 'INVOKED',
                {'content-length': str(len(result))}, result)
