from abc import ABC, abstractmethod

class Postprocessor(ABC):
    def __init__(self, input_dataset_path, output_dataset_path, input_config_path):
        """
        Initiate a preprocessor.
        :param input_dataset_path: Path to the input dataset file.
        :param output_dataset_path: Path to the output dataset file.
        :param input_config_path: Path to the input configuration file.
        """
        self._input_dataset_path = input_dataset_path
        self._output_dataset_path = output_dataset_path
        self._input_config_path = input_config_path
    
    @abstractmethod
    def _postprocess(self):
        ...

    def postprocess(self):
        """
        Run the preprocessor to process the given data.
        """
        return self._postprocess()

    def get_output_dataset_path(self):
        """
        :return: Path to the output dataset file.
        """
        return self._output_dataset_path
