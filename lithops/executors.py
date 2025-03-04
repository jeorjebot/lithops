#
# (C) Copyright IBM Corp. 2020
# (C) Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import copy
import logging
import atexit
import pickle
import tempfile
import numpy as np
import subprocess as sp
from datetime import datetime
from lithops import constants
from lithops.invokers import create_invoker
from lithops.storage import InternalStorage
from lithops.wait import wait, ALL_COMPLETED, THREADPOOL_SIZE, WAIT_DUR_SEC
from lithops.job import create_map_job, create_reduce_job
from lithops.config import default_config, \
    extract_localhost_config, extract_standalone_config, \
    extract_serverless_config, get_log_info, extract_storage_config
from lithops.constants import LOCALHOST, CLEANER_DIR, \
    CLEANER_LOG_FILE, SERVERLESS, STANDALONE
from lithops.utils import is_notebook, setup_lithops_logger, \
    is_lithops_worker, create_executor_id, create_futures_list
from lithops.localhost.localhost import LocalhostHandler
from lithops.standalone.standalone import StandaloneHandler
from lithops.serverless.serverless import ServerlessHandler
from lithops.storage.utils import create_job_key
from lithops.monitor import JobMonitor
from lithops.utils import FuturesList


logger = logging.getLogger(__name__)


class FunctionExecutor:
    """
    Executor abstract class that contains the common logic
    for the Localhost, Serverless and Standalone executors
    """

    def __init__(self,
                 mode=None,
                 config=None,
                 backend=None,
                 storage=None,
                 runtime=None,
                 runtime_memory=None,
                 monitoring=None,
                 workers=None,
                 remote_invoker=None,
                 log_level=False):
        """ Create a FunctionExecutor Class """

        self.is_lithops_worker = is_lithops_worker()

        # setup lithops logging
        if not self.is_lithops_worker:
            # if is lithops worker, logging has been set up in entry_point.py
            if log_level:
                setup_lithops_logger(log_level)
            elif log_level is False and logger.getEffectiveLevel() == logging.WARNING:
                # Set default logging from config
                setup_lithops_logger(*get_log_info(config))

        # overwrite user-provided parameters
        config_ow = {'lithops': {}}
        if runtime is not None:
            config_ow['runtime'] = runtime
        if runtime_memory is not None:
            config_ow['runtime_memory'] = int(runtime_memory)
        if remote_invoker is not None:
            config_ow['remote_invoker'] = remote_invoker
        if mode is not None:
            config_ow['lithops']['mode'] = mode
        if backend is not None:
            config_ow['lithops']['backend'] = backend
        if storage is not None:
            config_ow['lithops']['storage'] = storage
        if workers is not None:
            config_ow['lithops']['workers'] = workers
        if monitoring is not None:
            config_ow['lithops']['monitoring'] = monitoring

        self.config = default_config(copy.deepcopy(config), config_ow)

        self.executor_id = create_executor_id()

        self.data_cleaner = self.config['lithops'].get('data_cleaner', True)
        if self.data_cleaner and not self.is_lithops_worker:
            spawn_cleaner = int(self.executor_id.split('-')[1]) == 0
            atexit.register(self.clean, spawn_cleaner=spawn_cleaner,
                            clean_cloudobjects=False)

        storage_config = extract_storage_config(self.config)
        self.internal_storage = InternalStorage(storage_config)
        self.storage = self.internal_storage.storage

        self.futures = []
        self.cleaned_jobs = set()
        self.total_jobs = 0
        self.last_call = None

        if self.config['lithops']['mode'] == LOCALHOST:
            localhost_config = extract_localhost_config(self.config)
            self.compute_handler = LocalhostHandler(localhost_config)
        elif self.config['lithops']['mode'] == SERVERLESS:
            serverless_config = extract_serverless_config(self.config)
            self.compute_handler = ServerlessHandler(serverless_config, self.internal_storage)
        elif self.config['lithops']['mode'] == STANDALONE:
            standalone_config = extract_standalone_config(self.config)
            self.compute_handler = StandaloneHandler(standalone_config)

        # Create the monitoring system
        monitoring_backend = self.config['lithops']['monitoring'].lower()
        monitoring_config = self.config.get(monitoring_backend)
        self.job_monitor = JobMonitor(monitoring_backend, monitoring_config)

        # Create the invokder
        self.invoker = create_invoker(self.config,
                                      self.executor_id,
                                      self.internal_storage,
                                      self.compute_handler,
                                      self.job_monitor)

        logger.debug('Function executor for {} created with ID: {}'
                     .format(self.config['lithops']['backend'], self.executor_id))

        self.log_path = None

    def __enter__(self):
        """ Context manager method """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ Context manager method """
        self.job_monitor.stop()
        self.invoker.stop()

    def _create_job_id(self, call_type):
        job_id = str(self.total_jobs).zfill(3)
        self.total_jobs += 1
        return '{}{}'.format(call_type, job_id)

    def call_async(self, func, data, extra_env=None, runtime_memory=None,
                   timeout=None, include_modules=[], exclude_modules=[]):
        """
        For running one function execution asynchronously

        :param func: the function to map over the data
        :param data: input data
        :param extra_env: Additional env variables for action environment
        :param runtime_memory: Memory to use to run the function
        :param timeout: Time that the functions have to complete their
                        execution before raising a timeout
        :param include_modules: Explicitly pickle these dependencies
        :param exclude_modules: Explicitly keep these modules from pickled
                                dependencies

        :return: future object.
        """
        job_id = self._create_job_id('A')
        self.last_call = 'call_async'

        runtime_meta = self.invoker.select_runtime(job_id, runtime_memory)

        job = create_map_job(config=self.config,
                             internal_storage=self.internal_storage,
                             executor_id=self.executor_id,
                             job_id=job_id,
                             map_function=func,
                             iterdata=[data],
                             runtime_meta=runtime_meta,
                             runtime_memory=runtime_memory,
                             extra_env=extra_env,
                             include_modules=include_modules,
                             exclude_modules=exclude_modules,
                             execution_timeout=timeout)

        futures = self.invoker.run_job(job)
        self.futures.extend(futures)

        return futures[0]

    def map(self, map_function, map_iterdata, chunksize=None, worker_processes=None,
            extra_args=None, extra_env=None, runtime_memory=None, chunk_size=None,
            chunk_n=None, obj_chunk_size=None, obj_chunk_number=None, timeout=None,
            invoke_pool_threads=None, include_modules=[], exclude_modules=[]):
        """
        For running multiple function executions asynchronously

        :param map_function: the function to map over the data
        :param map_iterdata: An iterable of input data
        :param chunksize: Split map_iteradata in chunks of this size.
                          Lithops spawns 1 worker per resulting chunk. Default 1
        :param worker_processes: Number of concurrent/parallel processes in each worker. Default 1
        :param extra_args: Additional args to pass to the function activations
        :param extra_env: Additional env variables for action environment
        :param runtime_memory: Memory to use to run the function
        :param obj_chunk_size: the size of the data chunks to split each object.
                           'None' for processing the whole file in one function
                           activation.
        :param obj_chunk_number: Number of chunks to split each object. 'None' for
                                 processing the whole file in one function activation
        :param remote_invocation: Enable or disable remote_invocation mechanism
        :param timeout: Time that the functions have to complete their execution
                        before raising a timeout
        :param invoke_pool_threads: Number of threads to use to invoke
        :param include_modules: Explicitly pickle these dependencies
        :param exclude_modules: Explicitly keep these modules from pickled
                                dependencies

        :return: A list with size `len(iterdata)` of futures.
        """

        job_id = self._create_job_id('M')
        self.last_call = 'map'

        runtime_meta = self.invoker.select_runtime(job_id, runtime_memory)

        job = create_map_job(config=self.config,
                             internal_storage=self.internal_storage,
                             executor_id=self.executor_id,
                             job_id=job_id,
                             map_function=map_function,
                             iterdata=map_iterdata,
                             chunksize=chunksize,
                             worker_processes=worker_processes,
                             runtime_meta=runtime_meta,
                             runtime_memory=runtime_memory,
                             extra_env=extra_env,
                             include_modules=include_modules,
                             exclude_modules=exclude_modules,
                             execution_timeout=timeout,
                             extra_args=extra_args,
                             chunk_size=chunk_size,
                             chunk_n=chunk_n,
                             obj_chunk_size=obj_chunk_size,
                             obj_chunk_number=obj_chunk_number,
                             invoke_pool_threads=invoke_pool_threads)

        futures = self.invoker.run_job(job)
        self.futures.extend(futures)

        if isinstance(map_iterdata, FuturesList):
            for fut in map_iterdata:
                fut._produce_output = False

        return create_futures_list(futures, self)

    def map_reduce(self, map_function, map_iterdata, reduce_function, chunksize=None,
                   worker_processes=None, extra_args=None, extra_env=None,
                   map_runtime_memory=None, obj_chunk_size=None, obj_chunk_number=None,
                   reduce_runtime_memory=None, chunk_size=None, chunk_n=None,
                   timeout=None, invoke_pool_threads=None, reducer_one_per_object=False,
                   reducer_wait_local=False, include_modules=[], exclude_modules=[]):
        """
        Map the map_function over the data and apply the reduce_function across all futures.
        This method is executed all within CF.

        :param map_function: the function to map over the data
        :param map_iterdata:  An iterable of input data
        :param chunksize: Split map_iteradata in chunks of this size.
                          Lithops spawns 1 worker per resulting chunk. Default 1
        :param worker_processes: Number of concurrent/parallel processes in each worker Default 1
        :param reduce_function:  the function to reduce over the futures
        :param extra_env: Additional environment variables for action environment. Default None.
        :param extra_args: Additional arguments to pass to function activation. Default None.
        :param map_runtime_memory: Memory to use to run the map function. Default None (loaded from config).
        :param reduce_runtime_memory: Memory to use to run the reduce function. Default None (loaded from config).
        :param obj_chunk_size: the size of the data chunks to split each object. 'None' for processing
                               the whole file in one function activation.
        :param obj_chunk_number: Number of chunks to split each object. 'None' for processing the whole
                                 file in one function activation.
        :param remote_invocation: Enable or disable remote_invocation mechanism. Default 'False'
        :param timeout: Time that the functions have to complete their execution before raising a timeout.
        :param reducer_one_per_object: Set one reducer per object after running the partitioner
        :param reducer_wait_local: Wait for results locally
        :param invoke_pool_threads: Number of threads to use to invoke.
        :param include_modules: Explicitly pickle these dependencies.
        :param exclude_modules: Explicitly keep these modules from pickled dependencies.

        :return: A list with size `len(map_iterdata)` of futures.
        """
        self.last_call = 'map_reduce'
        map_job_id = self._create_job_id('M')

        runtime_meta = self.invoker.select_runtime(map_job_id, map_runtime_memory)

        map_job = create_map_job(config=self.config,
                                 internal_storage=self.internal_storage,
                                 executor_id=self.executor_id,
                                 job_id=map_job_id,
                                 map_function=map_function,
                                 iterdata=map_iterdata,
                                 chunksize=chunksize,
                                 worker_processes=worker_processes,
                                 runtime_meta=runtime_meta,
                                 runtime_memory=map_runtime_memory,
                                 extra_args=extra_args,
                                 extra_env=extra_env,
                                 chunk_size=chunk_size,
                                 chunk_n=chunk_n,
                                 obj_chunk_size=obj_chunk_size,
                                 obj_chunk_number=obj_chunk_number,
                                 include_modules=include_modules,
                                 exclude_modules=exclude_modules,
                                 execution_timeout=timeout,
                                 invoke_pool_threads=invoke_pool_threads)

        map_futures = self.invoker.run_job(map_job)
        self.futures.extend(map_futures)

        if isinstance(map_iterdata, FuturesList):
            for fut in map_iterdata:
                fut._produce_output = False

        if reducer_wait_local:
            self.wait(map_futures)

        reduce_job_id = map_job_id.replace('M', 'R')

        runtime_meta = self.invoker.select_runtime(reduce_job_id, reduce_runtime_memory)

        reduce_job = create_reduce_job(config=self.config,
                                       internal_storage=self.internal_storage,
                                       executor_id=self.executor_id,
                                       reduce_job_id=reduce_job_id,
                                       reduce_function=reduce_function,
                                       map_job=map_job,
                                       map_futures=map_futures,
                                       runtime_meta=runtime_meta,
                                       runtime_memory=reduce_runtime_memory,
                                       reducer_one_per_object=reducer_one_per_object,
                                       extra_env=extra_env,
                                       include_modules=include_modules,
                                       exclude_modules=exclude_modules)

        reduce_futures = self.invoker.run_job(reduce_job)
        self.futures.extend(reduce_futures)

        for f in map_futures:
            f._produce_output = False

        return create_futures_list(map_futures + reduce_futures, self)

    def wait(self, fs=None, throw_except=True, return_when=ALL_COMPLETED,
             download_results=False, timeout=None, threadpool_size=THREADPOOL_SIZE,
             wait_dur_sec=WAIT_DUR_SEC):
        """
        Wait for the Future instances (possibly created by different Executor instances)
        given by fs to complete. Returns a named 2-tuple of sets. The first set, named done,
        contains the futures that completed (finished or cancelled futures) before the wait
        completed. The second set, named not_done, contains the futures that did not complete
        (pending or running futures). timeout can be used to control the maximum number of
        seconds to wait before returning.

        :param fs: Futures list. Default None
        :param throw_except: Re-raise exception if call raised. Default True.
        :param return_when: One of `ALL_COMPLETED`, `ANY_COMPLETED`, `ALWAYS`
        :param download_results: Download results. Default false (Only get statuses)
        :param timeout: Timeout of waiting for results.
        :param threadpool_size: Number of threads to use. Default 64
        :param wait_dur_sec: Time interval between each check.

        :return: `(fs_done, fs_notdone)`
            where `fs_done` is a list of futures that have completed
            and `fs_notdone` is a list of futures that have not completed.
        :rtype: 2-tuple of list
        """
        futures = fs or self.futures
        if type(futures) != list and type(futures) != FuturesList:
            futures = [futures]

        # Start waiting for results
        try:
            wait(fs=futures,
                 internal_storage=self.internal_storage,
                 job_monitor=self.job_monitor,
                 download_results=download_results,
                 throw_except=throw_except,
                 return_when=return_when,
                 timeout=timeout,
                 threadpool_size=threadpool_size,
                 wait_dur_sec=wait_dur_sec)

        except Exception as e:
            self.invoker.stop()
            if not fs and is_notebook():
                del self.futures[len(self.futures) - len(futures):]
            if self.data_cleaner and not self.is_lithops_worker:
                self.clean(clean_cloudobjects=False, force=True)
            raise e

        finally:
            present_jobs = {f.job_key for f in futures}
            self.job_monitor.stop(present_jobs)
            if self.data_cleaner and not self.is_lithops_worker:
                self.compute_handler.clear(present_jobs)
                self.clean(clean_cloudobjects=False)

        if download_results:
            fs_done = [f for f in futures if f.done]
            fs_notdone = [f for f in futures if not f.done]
        else:
            fs_done = [f for f in futures if f.success or f.done]
            fs_notdone = [f for f in futures if not f.success and not f.done]

        return create_futures_list(fs_done, self), create_futures_list(fs_notdone, self)

    def get_result(self, fs=None, throw_except=True, timeout=None,
                   threadpool_size=THREADPOOL_SIZE, wait_dur_sec=WAIT_DUR_SEC):
        """
        For getting the results from all function activations

        :param fs: Futures list. Default None
        :param throw_except: Reraise exception if call raised. Default True.
        :param verbose: Shows some information prints. Default False
        :param timeout: Timeout for waiting for results.
        :param THREADPOOL_SIZE: Number of threads to use. Default 128
        :param WAIT_DUR_SEC: Time interval between each check.
        :return: The result of the future/s
        """
        fs_done, _ = self.wait(fs=fs, throw_except=throw_except,
                               timeout=timeout, download_results=True,
                               threadpool_size=threadpool_size,
                               wait_dur_sec=wait_dur_sec)
        result = []
        fs_done = [f for f in fs_done if not f.futures and f._produce_output]
        for f in fs_done:
            if fs:
                # Process futures provided by the user
                result.append(f.result(throw_except=throw_except,
                                       internal_storage=self.internal_storage))
            elif not fs and not f._read:
                # Process internally stored futures
                result.append(f.result(throw_except=throw_except,
                                       internal_storage=self.internal_storage))
                f._read = True

        logger.debug("ExecutorID {} Finished getting results"
                     .format(self.executor_id))

        if len(result) == 1 and self.last_call != 'map':
            return result[0]

        return result

    def plot(self, fs=None, dst=None):
        """
        Creates timeline and histogram of the current execution in dst_dir.

        :param dst_dir: destination folder to save .png plots.
        :param dst_file_name: prefix name of the file.
        :param fs: list of futures.
        """
        ftrs = self.futures if not fs else fs

        if type(ftrs) != list:
            ftrs = [ftrs]

        ftrs_to_plot = [f for f in ftrs if (f.success or f.done) and not f.error]

        if not ftrs_to_plot:
            logger.debug('ExecutorID {} - No futures ready to plot'
                         .format(self.executor_id))
            return

        logging.getLogger('matplotlib').setLevel(logging.WARNING)
        from lithops.plots import create_timeline, create_histogram

        logger.info('ExecutorID {} - Creating execution plots'.format(self.executor_id))

        create_timeline(ftrs_to_plot, dst)
        create_histogram(ftrs_to_plot, dst)

    def clean(self, fs=None, cs=None, clean_cloudobjects=True, spawn_cleaner=True, force=False):
        """
        Deletes all the temp files from storage. These files include the function,
        the data serialization and the function invocation results. It can also clean
        cloudobjects.

        :param fs: list of futures to clean
        :param cs: list of cloudobjects to clean
        :param clean_cloudobjects: true/false
        :param spawn_cleaner true/false
        """

        os.makedirs(CLEANER_DIR, exist_ok=True)

        def save_data_to_clean(data):
            with tempfile.NamedTemporaryFile(dir=CLEANER_DIR, delete=False) as temp:
                pickle.dump(data, temp)

        if cs:
            data = {'cos_to_clean': list(cs),
                    'storage_config': self.internal_storage.get_storage_config()}
            save_data_to_clean(data)
            if not fs:
                return

        futures = fs or self.futures
        futures = [futures] if type(futures) != list else futures
        present_jobs = {create_job_key(f.executor_id, f.job_id) for f in futures
                        if (f.executor_id.count('-') == 1 and f.done) or force}
        jobs_to_clean = present_jobs - self.cleaned_jobs

        if jobs_to_clean:
            logger.info("ExecutorID {} - Cleaning temporary data"
                        .format(self.executor_id))
            data = {'jobs_to_clean': jobs_to_clean,
                    'clean_cloudobjects': clean_cloudobjects,
                    'storage_config': self.internal_storage.get_storage_config()}
            save_data_to_clean(data)
            self.cleaned_jobs.update(jobs_to_clean)

        if (jobs_to_clean or cs) and spawn_cleaner:
            log_file = open(CLEANER_LOG_FILE, 'a')
            cmdstr = [sys.executable, '-m', 'lithops.scripts.cleaner']
            sp.Popen(' '.join(cmdstr), shell=True, stdout=log_file, stderr=log_file)

    def job_summary(self, cloud_objects_n=0):
        """
        logs information of a job executed by the calling function executor.
        currently supports: code_engine, ibm_vpc and ibm_cf.
        on future commits, support will extend to code_engine and ibm_vpc :

        :param cloud_objects_n: number of cloud object used in COS, declared by user.
        """
        import pandas as pd

        def init():
            headers = ['Job_ID', 'Function', 'Invocations', 'Memory(MB)', 'AvgRuntime', 'Cost', 'CloudObjects']
            pd.DataFrame([], columns=headers).to_csv(self.log_path, index=False)

        def append(content):
            """ appends job information to log file."""
            pd.DataFrame(content).to_csv(self.log_path, mode='a', header=False, index=False)

        def append_summary():
            """ add a summary row to the log file"""
            df = pd.read_csv(self.log_path)
            total_average = sum(df.AvgRuntime * df.Invocations) / df.Invocations.sum()
            total_row = pd.DataFrame([['Summary', ' ', df.Invocations.sum(), df['Memory(MB)'].sum(),
                                       round(total_average, 10), df.Cost.sum(), cloud_objects_n]])
            total_row.to_csv(self.log_path, mode='a', header=False, index=False)

        def get_object_num():
            """returns cloud objects used up to this point, using this function executor. """
            df = pd.read_csv(self.log_path)
            return float(df.iloc[-1].iloc[-1])

        # Avoid logging info unless chosen computational backend is supported.
        if hasattr(self.compute_handler.backend, 'calc_cost'):

            if self.log_path:  # retrieve cloud_objects_n from last log file
                cloud_objects_n += get_object_num()
            else:
                self.log_path = os.path.join(constants.LOGS_DIR, datetime.now().strftime("%Y-%m-%d_%H:%M:%S.csv"))
            # override current logfile
            init()

            futures = self.futures
            if type(futures) != list:
                futures = [futures]

            memory = []
            runtimes = []
            curr_job_id = futures[0].job_id
            job_func = futures[0].function_name  # each job is conducted on a single function

            for future in futures:
                if curr_job_id != future.job_id:
                    cost = self.compute_handler.backend.calc_cost(runtimes, memory)
                    append([[curr_job_id, job_func, len(runtimes), sum(memory),
                             np.round(np.average(runtimes), 10), cost, ' ']])

                    # updating next iteration's variables:
                    curr_job_id = future.job_id
                    job_func = future.function_name
                    memory.clear()
                    runtimes.clear()

                memory.append(future.runtime_memory)
                runtimes.append(future.stats['worker_exec_time'])

            # appends last Job-ID
            cost = self.compute_handler.backend.calc_cost(runtimes, memory)
            append([[curr_job_id, job_func, len(runtimes), sum(memory),
                     np.round(np.average(runtimes), 10), cost, ' ']])
            # append summary row to end of the dataframe
            append_summary()

        else:  # calc_cost() doesn't exist for chosen computational backend.
            logger.warning("Could not log job: {} backend isn't supported by this function."
                           .format(self.compute_handler.backend.name))
            return
        logger.info("View log file logs at {}".format(self.log_path))


class LocalhostExecutor(FunctionExecutor):

    def __init__(self,
                 config=None,
                 runtime=None,
                 storage=None,
                 monitoring=None,
                 log_level=False):
        """
        Initialize a LocalhostExecutor class.

        :param config: Settings passed in here will override those in config file.
        :param runtime: Runtime name to use.
        :param storage: Name of the storage backend to use.
        :param monitoring: monitoring system.
        :param log_level: log level to use during the execution.

        :return `LocalhostExecutor` object.
        """
        super().__init__(backend=LOCALHOST,
                         config=config,
                         runtime=runtime,
                         storage=storage or LOCALHOST,
                         log_level=log_level,
                         monitoring=monitoring)


class ServerlessExecutor(FunctionExecutor):

    def __init__(self,
                 config=None,
                 runtime=None,
                 runtime_memory=None,
                 backend=None,
                 storage=None,
                 workers=None,
                 monitoring=None,
                 remote_invoker=None,
                 log_level=False):
        """
        Initialize a ServerlessExecutor class.

        :param config: Settings passed in here will override those in config file.
        :param runtime: Runtime name to use.
        :param runtime_memory: memory to use in the runtime.
        :param backend: Name of the serverless compute backend to use.
        :param storage: Name of the storage backend to use.
        :param workers: Max number of concurrent workers.
        :param monitoring: monitoring system.
        :param log_level: log level to use during the execution.

        :return `ServerlessExecutor` object.
        """

        backend = backend or constants.SERVERLESS_BACKEND_DEFAULT

        super().__init__(config=config,
                         runtime=runtime,
                         runtime_memory=runtime_memory,
                         backend=backend,
                         storage=storage,
                         workers=workers,
                         monitoring=monitoring,
                         log_level=log_level,
                         remote_invoker=remote_invoker)


class StandaloneExecutor(FunctionExecutor):

    def __init__(self,
                 config=None,
                 backend=None,
                 runtime=None,
                 storage=None,
                 workers=None,
                 monitoring=None,
                 log_level=False):
        """
        Initialize a StandaloneExecutor class.

        :param config: Settings passed in here will override those in config file.
        :param runtime: Runtime name to use.
        :param backend: Name of the standalone compute backend to use.
        :param storage: Name of the storage backend to use.
        :param workers: Max number of concurrent workers.
        :param monitoring: monitoring system.
        :param log_level: log level to use during the execution.

        :return `StandaloneExecutor` object.
        """

        backend = backend or constants.STANDALONE_BACKEND_DEFAULT

        super().__init__(config=config,
                         runtime=runtime,
                         backend=backend,
                         storage=storage,
                         workers=workers,
                         monitoring=monitoring,
                         log_level=log_level)

    def create(self):
        runtime_key, runtime_meta = self.compute_handler.create()
        self.internal_storage.put_runtime_meta(runtime_key, runtime_meta)
