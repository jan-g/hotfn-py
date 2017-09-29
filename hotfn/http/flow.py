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

import os
import requests
import threading

_LOCK = threading.Lock()

_FLOW = None


def _flow():
    global _FLOW
    with _LOCK:
        if _FLOW is None:
            _FLOW = FlowClient()
        return _FLOW


def _request(method, path):
    path


class FlowClient(object):
    def __init__(self):
        self.base_url = os.environ['COMPLETER_URL']
        self.function_id = os.environ
        self.createThread()

    def post(self, path, body=None):
        return self.request('POST', path, body=body)

    def get(self, path, body=None):
        return self.request('GET', path, body=body)

    def request(self, method, path, body=None):
        response = requests.request(method, self.base_URL + path, body=body)
        response.raise_for_status()
        return response.headers, response.json()

    def createThread(self):
        self.post("/graph?functionId={}".format())


def supply(fn):
    _flow().