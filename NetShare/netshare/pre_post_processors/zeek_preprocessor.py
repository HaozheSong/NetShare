import json
import os
import shutil
import subprocess

from .preprocessor import Preprocessor


class ZeekPreprocessor(Preprocessor):
    def __init__(self, input_dataset_path, output_dataset_path, input_config_path, output_config_path):
        """
        Initiate a customizable format preprocessor.
        :param input_dataset_path: Path to the input dataset file.
        :param output_dataset_path: Path to the output dataset file.
        :param input_config_path: Path to the input configuration file.
        :param output_config_path: Path to the output configuration file.
        """
        super().__init__(input_dataset_path, output_dataset_path, input_config_path, output_config_path)

    @staticmethod
    def get_preprocessor_config(config):
        """
        Extract the specific preprocessor's config from the whole config JSON.
        :param config: The whole configuration JSON.
        :return: The specific preprocessor's config.
        """
        default = {'target_protocol': 'http'}
        preprocessors = config['processors']['preprocessors']
        for preprocessor in preprocessors:
            if preprocessor['class'] != 'ZeekPreprocessor':
                continue
            return preprocessor.get('config', default)

    @staticmethod
    def run_command(command):
        """
        Run a shell command.
        :param command: The command to be executed.
        :return: The output from the command execution.
        """
        result = subprocess.run(command, text=True, shell=True)
        return result.stdout

    def _preprocess(self):
        with open(self._input_config_path, 'r') as input_config_file:
            config = json.load(input_config_file)
        protocol = ZeekPreprocessor.get_preprocessor_config(config).get('target_protocol', 'http')

        zeek_dir = os.path.join(os.path.dirname(self._output_dataset_path), 'zeek')
        os.makedirs(zeek_dir, exist_ok=True)

        command = 'cd ' + zeek_dir + '; zeek -C -r ' + str(self._input_dataset_path) + ' LogAscii::use_json=T'
        ZeekPreprocessor.run_command(command)

        self._output_dataset_path = os.path.join(zeek_dir, protocol + '.log')

        # Copy config file
        if self._output_config_path != self._input_config_path:
            shutil.copy2(
                src=self._input_config_path,
                dst=self._output_config_path
            )
