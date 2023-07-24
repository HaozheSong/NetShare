import pathlib
import shutil
import json

import netshare.ray as ray
from netshare import Generator

from zeek_processor import parse2csv
from pre_post_processor_mixed import pre_processor, post_processor


class Driver:
    def __init__(self, working_dir_name, dataset_file, config_file):
        """
        Arguments:
        * working_dir_name: create a working folder .../NetShare/results/<working_dir_name> and work there
        * dataset_file
        * config_file
        * src_dir

        Path variable naming convention:
        * <dir_name>_dir -> directory path
        * <file_name>_file -> file path
        * <file_name>_fd -> opened file descriptor -> e.g. open(dataset_file) as dataset_fd
        """
        # netshare_dir = '.../NetShare'
        self.netshare_dir = pathlib.Path(__file__).parents[1]
        # results_dir = '.../NetShare/results'
        self.results_dir = self.netshare_dir.joinpath('results')
        # working_dir = '.../NetShare/results/<working_dir_name>'
        self.working_dir = self.results_dir.joinpath(working_dir_name)
        self.working_dir.mkdir(parents=True, exist_ok=True)
        # src directory stores files uploaded by the user
        # src_dir = '.../NetShare/results/<working_dir_name>/src'
        self.src_dir = self.working_dir.joinpath('src')

        if pathlib.Path(dataset_file).is_file():
            self.dataset_file = pathlib.Path(dataset_file)
            self.dataset_file_name = self.dataset_file.name
        else:
            self.dataset_file_name = dataset_file
            self.dataset_file = self.src_dir.joinpath(self.dataset_file_name)

        if pathlib.Path(config_file).is_file():
            self.config_file = pathlib.Path(config_file)
            self.config_file_name = self.config_file.name
        else:
            self.config_file_name = config_file
            self.config_file = self.src_dir.joinpath(self.config_file_name)
        with open(self.config_file) as self.config_fd:
            self.config = json.load(self.config_fd)

    def preprocess(self):
        # copy dataset and user.json
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
        # preprocess dataset and user.json
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

    def run(self, config_file=None, ray_enabled=False):
        self.preprocess()
        if config_file is None:
            config_file = self.preprocessed_config_file
        elif isinstance(config_file, str):
            config_file = pathlib.Path(config_file)
        config_file_abs_path = str(config_file.resolve())
        working_dir_abs_path = str(self.working_dir.resolve())
        ray.config.enabled = ray_enabled
        ray.init(address="auto")
        generator = Generator(config=config_file_abs_path)
        generator.train(work_folder=working_dir_abs_path)
        generator.generate(work_folder=working_dir_abs_path)
        generator.visualize(work_folder=working_dir_abs_path)
        ray.shutdown()
