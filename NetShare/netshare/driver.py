import pathlib
import shutil
import json

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
                 overwrite_existing_working_dir=False):
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
        """

        # working_dir = '.../NetShare/results/<working_dir_name>'
        self.working_dir = self.results_dir.joinpath(working_dir_name)
        if self.working_dir.is_dir() and overwrite_existing_working_dir:
            shutil.rmtree(self.working_dir)
        self.working_dir.mkdir(parents=True, exist_ok=True)

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

    def run(self, ray_enabled=False, local_web=True):
        """
        Train, generate and visualize. Result json and images will be stored in 
        `.../NetShare/results/<working_dir_name>/result` which can be visualized by a local website.

        Arguments:
        :param ray_enabled: `True` to enable Ray
        :type ray_enabled: boolean

        :param local_web: `True` to visualize results in a local website
        :type local_web: boolean
        """
        self.preprocess()
        config_file_abs_path = str(self.preprocessed_config_file.resolve())
        working_dir_abs_path = str(self.working_dir.resolve())
        ray.config.enabled = ray_enabled
        ray.init(address="auto")
        generator = Generator(config=config_file_abs_path)
        generator.train(work_folder=working_dir_abs_path)
        generator.generate(work_folder=working_dir_abs_path)
        generator.visualize(
            work_folder=working_dir_abs_path,
            local_web=local_web
        )
        ray.shutdown()


class WebDriver(Driver):
    def __init__(self, working_dir_name, dataset_file_name, config_file_name):
        # working_dir = '.../NetShare/results/<working_dir_name>'
        self.working_dir = self.results_dir.joinpath(working_dir_name)
        self.working_dir.mkdir(parents=True, exist_ok=True)

        # src_dir stores original dataset and config.json uploaded by the user (only for WebDriver)
        # src_dir = '.../NetShare/results/<working_dir_name>/src'
        self.src_dir = self.working_dir.joinpath('src')

        self.dataset_file = self.src_dir.joinpath(dataset_file_name)
        self.config_file = self.src_dir.joinpath(config_file_name)

        super().__init__(
            working_dir_name,
            str(self.dataset_file.resolve()),
            str(self.config_file.resolve())
        )
