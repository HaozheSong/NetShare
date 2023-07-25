import json
import pandas as pd
from . import parse_func
import os


class Stage1Preprocessor(object):
    def __init__(self):
        self.result = pd.DataFrame({})

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

    def parse_to_csv(self, config_path, input_path, output_path='./result.csv'):
        """
        Parse an input file to csv file according to a configuration file.
        :param config_path: Path to the configuration file.
        :param input_path: Path to the input file.
        :param output_path: Path to the output csv file.
        """
        with open(config_path, 'r') as openfile:
            config = json.load(openfile)
        input_config = config['input_file']
        fields_configs = config['fields']
        fields = Stage1Preprocessor.get_fields(fields_configs)

        df = Stage1Preprocessor.to_dataframe(input_path, input_config)
        Stage1Preprocessor.parse_field(fields, df, self.result)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        self.result.to_csv(output_path, index=True)
