import json
import shutil

import pandas as pd
from . import parse_func
from .preprocessor import Preprocessor
import os


class Stage1Preprocessor(Preprocessor):
    def __init__(self, input_dataset_path, output_dataset_path, input_config_path, output_config_path):
        """
        Initiate a stage 1 preprocessor.
        :param input_dataset_path: Path to the input dataset file.
        :param output_dataset_path: Path to the output dataset file.
        :param input_config_path: Path to the input configuration file.
        :param output_config_path: Path to the output configuration file.
        """
        super().__init__(input_dataset_path, output_dataset_path, input_config_path, output_config_path)
        self._result = pd.DataFrame({})

    @staticmethod
    def parse_format(field_format):
        """
        Parse format in the config file to Python type.
        :param field_format: Field format in the config file.
        :return: Python type in str.
        """
        if field_format is None:
            return 'str'
        if field_format == 'string' or field_format == 'IP' or field_format == 'list':
            return 'str'
        if field_format == 'integer' or field_format == 'timestamp':
            return 'int'
        return field_format

    @staticmethod
    def get_fields(fields):
        """
        Extract field configurations from the origin fields config and fill in default values.
        :param fields: Origin fields config.
        :return: Extracted fields with filled default values.
        """
        result = []
        for data_type in fields:
            for field in fields[data_type]:
                field['type'] = data_type
                # Default for to_name: origin name
                if 'to' not in field:
                    field['to'] = field['name']
                # Default for format: str
                field['format'] = Stage1Preprocessor.parse_format(field.get('format', 'str'))
                # Default for abnormal: false
                if 'abnormal' not in field:
                    field['abnormal'] = False
                result.append(field)
        return result

    @staticmethod
    def parse_field(fields, df, result):
        """
        Parse all the fields one by one according to the config.
        :param fields: Fields configs.
        :param df: Origin dataframe.
        :param result: Parsed dataframe.
        """
        for field in fields:
            result[field['to']] = df[field['name']].apply(getattr(parse_func, 'parse', None), field=field,
                                                          if_handle_abnormal=field['abnormal'],
                                                          func_name=field.get('parse', None))
            result[field['to']] = result[field['to']].astype(
                field['format'], errors='ignore')

    @staticmethod
    def to_dataframe(input_path, input_config):
        """
        Extract origin input file to pandas dataframe.
        :param input_path: Path to the input file.
        :param input_config: Input config with file format.
        :return: Extracted pandas dataframe.
        """
        input_format = input_config.get('format', 'zeek_log_json')  # Default: 'zeek_log_json'
        if input_format == 'csv':
            return pd.read_csv(input_path)

        # Zeek log in JSON format
        data = []
        with open(input_path) as input_file:
            line = input_file.readline()
            while line:
                packet = json.loads(line)
                data.append(packet)
                line = input_file.readline()
        return pd.DataFrame(data)

    def _preprocess(self):
        with open(self._input_config_path, 'r') as input_config_file:
            config = json.load(input_config_file)
        fields_configs = config['fields']
        fields = Stage1Preprocessor.get_fields(fields_configs)

        input_config = config['input_file']
        df = Stage1Preprocessor.to_dataframe(self._input_dataset_path, input_config)
        Stage1Preprocessor.parse_field(fields, df, self._result)

        os.makedirs(os.path.dirname(self._output_dataset_path), exist_ok=True)
        self._result.to_csv(self._output_dataset_path, index=True)

        # Copy config file
        if self._output_config_path != self._input_config_path:
            shutil.copy2(
                src=self._input_config_path,
                dst=self._output_config_path
            )
