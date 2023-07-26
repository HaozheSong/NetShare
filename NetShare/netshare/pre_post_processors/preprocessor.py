from abc import ABC, abstractmethod


class Preprocessor(ABC):
    def __init__(self, input_dataset_path, output_dataset_path, input_config_path, output_config_path):
        """
        Initiate a preprocessor.
        :param input_dataset_path: Path to the input dataset file.
        :param output_dataset_path: Path to the output dataset file.
        :param input_config_path: Path to the input configuration file.
        :param output_config_path: Path to the output configuration file.
        """
        self._input_dataset_path = input_dataset_path
        self._output_dataset_path = output_dataset_path
        self._input_config_path = input_config_path
        self._output_config_path = output_config_path

    @abstractmethod
    def _preprocess(self):
        ...

    def preprocess(self):
        """
        Run the preprocessor to process the given data.
        """
        return self._preprocess()

    def get_output_dataset_path(self):
        """
        :return: Path to the output dataset file.
        """
        return self._output_dataset_path

    def get_output_config_path(self):
        """
        :return: Path to the output configuration file.
        """
        return self._output_config_path
