import pathlib
import shutil
import json
import sys
from multiprocessing import Process

import netshare.ray as ray
from netshare import Generator

from zeek_processor import parse2csv
from pre_post_processor_mixed import pre_processor, post_processor


class Driver:
    """
    Path variable naming convention:
    * <dir_name>_dir -> directory path
    * <file_name>_file -> file path
    * <file_name>_fd -> opened file descriptor -> e.g. open(dataset_file) as dataset_fd
    """
    # netshare_dir = '.../NetShare'
    netshare_dir = pathlib.Path(__file__).parents[1]
    # results_dir = '.../NetShare/results'
    results_dir = netshare_dir.joinpath('results')

    def __init__(self, working_dir_name, dataset_file, config_file,
                 overwrite_existing_working_dir=False,
                 redirect_stdout_stderr=False, separate_stdout_stderr_log=False,
                 ray_enabled=False, local_web=True, local_web_port=8050):
        """
        Arguments:
        :param working_dir_name: create a working directory `.../NetShare/results/<working_dir_name>` and work there
        :type working_dir_name: string

        :param dataset_file: a path string to dataset file
        :type dataset_file: string

        :param config_file: a path string to config.json file
        :type config_file: string

        :param overwrite_existing_working_dir: `True` to delete old existing working directory and create a new one
        :type overwrite_existing_working_dir: boolean, default `False`

        TODO: there is still something printed out in the terminal
        :param log_stdout_stderr: log stdout and stderr to `.../NetShare/results/<working_dir_name>/logs/stdout_stderr.log`
        :type stderr: boolean

        :param separate_stdout_stderr_log: log stdout to `.../NetShare/results/<working_dir_name>/logs/stdout.log`,
        and log stderr to `.../NetShare/results/<working_dir_name>/logs/stderr.log`,
        valid only when `log_stdout_stderr` is `True`
        :type stderr: boolean

        :param ray_enabled: `True` to enable Ray
        :type ray_enabled: boolean

        :param local_web: `True` to visualize results in a local website
        :type local_web: boolean

        :param local_web_port: local website port, useful to visualize results of multiple parallel drivers
        :type local_web_port: int
        """
        self.ray_enabled = ray_enabled
        self.local_web = local_web
        self.local_web_port = local_web_port

        # working_dir = '.../NetShare/results/<working_dir_name>'
        self.working_dir = self.results_dir.joinpath(working_dir_name)
        if self.working_dir.is_dir() and overwrite_existing_working_dir:
            shutil.rmtree(self.working_dir)
        self.working_dir.mkdir(parents=True, exist_ok=True)

        # logs_dir = '.../NetShare/results/<working_dir_name>/logs'
        self.logs_dir = self.working_dir.joinpath('logs')
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        # stdout_stderr_log_file = '.../NetShare/results/<working_dir_name>/logs/stdout_stderr.log
        self.stdout_stderr_log_file = self.logs_dir.joinpath(
            'stdout_stderr.log'
        )
        # stdout_log_file = '.../NetShare/results/<working_dir_name>/logs/stdout.log
        self.stdout_log_file = self.logs_dir.joinpath('stdout.log')
        # stderr_log_file = '.../NetShare/results/<working_dir_name>/logs/stderr.log
        self.stderr_log_file = self.logs_dir.joinpath('stderr.log')
        if redirect_stdout_stderr:
            if separate_stdout_stderr_log:
                sys.stdout = open(self.stdout_log_file, 'w')
                sys.stderr = open(self.stderr_log_file, 'w')
            else:
                stdout_stderr_log_fd = open(self.stdout_stderr_log_file, 'w')
                sys.stdout = stdout_stderr_log_fd
                sys.stderr = stdout_stderr_log_fd

        self.dataset_file = pathlib.Path(dataset_file)
        self.dataset_file_name = self.dataset_file.name

        self.config_file = pathlib.Path(config_file)
        self.config_file_name = self.config_file.name
        with open(self.config_file) as self.config_fd:
            self.config = json.load(self.config_fd)

    def preprocess(self):
        # copy dataset and config.json
        self.preprocess_dir = self.working_dir.joinpath('pre_processed_data')
        self.preprocess_dir.mkdir(parents=True, exist_ok=True)
        self.moved_dataset_file = self.preprocess_dir.joinpath(
            self.dataset_file_name
        )
        self.preprocessed_dataset_file = self.preprocess_dir.joinpath(
            'pre_processed.csv'
        )
        self.preprocessed_config_file = self.preprocess_dir.joinpath(
            self.config_file_name
        )
        shutil.copy2(
            src=self.dataset_file,
            dst=self.moved_dataset_file
        )
        shutil.copy2(
            src=self.config_file,
            dst=self.preprocessed_config_file
        )
        # preprocess dataset and config.json
        if self.config['processors']['zeek']:
            parse2csv.parse_to_csv(
                config_path=self.preprocessed_config_file,
                input_path=self.moved_dataset_file,
                output_path=self.preprocessed_dataset_file
            )
        else:
            self.preprocessed_dataset_file = self.moved_dataset_file
        if self.config['processors']['bowen']:
            bowen_preprocessor = pre_processor.Pre_processor(
                input_dataset=self.preprocessed_dataset_file,
                input_default_configs='default.json',
                input_field_configs=self.preprocessed_config_file,
                output_dataset=self.preprocessed_dataset_file,
                output_config=self.preprocessed_config_file
            )
            bowen_preprocessor.processor()

    def run(self):
        self.preprocess()
        config_file_abs_path = str(self.preprocessed_config_file.resolve())
        working_dir_abs_path = str(self.working_dir.resolve())
        ray.config.enabled = self.ray_enabled
        ray.init(address="auto")
        generator = Generator(config=config_file_abs_path)
        generator.train(work_folder=working_dir_abs_path)
        generator.generate(work_folder=working_dir_abs_path)
        generator.visualize(
            work_folder=working_dir_abs_path,
            local_web=self.local_web,
            local_web_port=self.local_web_port
        )
        ray.shutdown()

    def run_in_a_process(self):
        process = Process(target=self.run)
        process.start()
