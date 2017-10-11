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

from hotfn.http import worker, flow


@worker.coerce_input_to_content_type
def app(context, data=None, **kwargs):
    """
    This function demonstrates the asynchronous invocation of multiple
    continuations using the Fn Flow invoker.

    Run with:
        echo '"baz"' | fn call app_name /hotfnpy-hot
    """
    @flow.supply
    def fn1():
        return "foo"

    @fn1.then
    def fn1b(arg):
        return arg + " "

    @flow.supply
    def fn2():
        return "bar"

    @fn2.then
    def fn2b(arg):
        return arg + "; "

    both = flow.all_of(fn1b, fn2b)

    @both.then
    def fn3():
        return fn1b.get() + fn2b.get() + data

    # thenCompose constructs graph stages later on
    stage = flow.value(1)

    @stage.then_compose
    def stage_2(value):
        @flow.supply
        def supplier():
            return value

        return supplier

    return fn3.get() + str(stage_2.get())


if __name__ == "__main__":
    worker.run(app)
