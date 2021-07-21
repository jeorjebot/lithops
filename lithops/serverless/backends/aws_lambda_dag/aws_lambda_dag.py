#
# Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import functools
import json

# ----------------------------------------------------------------
class Orchestrator:
    def __init__(self):
        self.functions = dict()
        self.dag = ''
        self.dipendencies = dict() # keep track of the inputs needed to run a task, and their availability
        self.task_queue = list() # tasks that can be executed
        self.task_count = 0 # total number of tasks remaining


def register(func):
    """Register a function to be available to the Orchestrator"""
    if not('FUNCTIONS' in globals()):
        global FUNCTIONS
        FUNCTIONS = dict()
        FUNCTIONS[func.__name__] = func
    else:
        FUNCTIONS = globals()['FUNCTIONS']
        FUNCTIONS[func.__name__] = func
    return func



def DAG(dag_path):
    
    # read the DAG
    keys = list()
    f = open(dag_path, "r")
    for line in f:
        keys.append(line.strip())

    # TODO create data structures for task execution
    # - dictionary with the dependencies ( chi dipende da questo? questo)
    # - counter delle dipendenze, quando raggiunge lo zero può essere messo in coda 
    # - scheduler che guarda la coda e schedula cose

    

    def decorator_DAG(func):
        @functools.wraps(func)
        def wrapper_DAG(*args, **kwargs):
            # a = []
            # if args:
            #     a.append("args:")
            #     for x in args:
            #         a.append(x)
            # if kwargs:
            #     a.append("kwargs_keys:")
            #     for y in kwargs:
            #         a.append(y)

            #     a.append("kwargs_values:")
            #     for z in kwargs.values():
            #         a.append(z)
            #     #a.append([(*kwargs.values(),)])
            # return a

            

            returns = dict()
            for key in keys:

                value = FUNCTIONS[key](*kwargs.values())
                returns[key] = value

            return returns
        return wrapper_DAG
    return decorator_DAG
