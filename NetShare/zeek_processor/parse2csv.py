import json
import pandas as pd
from zeek_processor import parse_func
from config_io import Config


def parse_format(field_format):
    """
    Parse format in the config file to Python type.
    :param field_format: Field format in the config file.
    :return: Python type in str.
    """
    if field_format is None:
        return 'str'
    if field_format == 'string' or field_format == 'IP' or field_format == 'list' or field_format == 'timestamp':
        return 'str'
    if field_format == 'integer':
        return 'int'
    return field_format


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
            field['format'] = parse_format(field.get('format', 'str'))
            # Default for abnormal: false
            if 'abnormal' not in field:
                field['abnormal'] = False
            result.append(field)
    return result


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
        result[field['to']] = result[field['to']].astype(field['format'], errors='ignore')


def to_dataframe(input_config):
    """
    Extract origin input file to pandas dataframe.
    :param input_config: Input config with file path and format.
    :return: Extracted pandas dataframe.
    """
    input_format = input_config.get('format', 'zeek_log_json')  # Default: 'zeek_log_json'
    path = input_config.get('path')
    if input_format == 'csv':
        return pd.read_csv(path)

    # Zeek log in Json format
    data = []
    with open(path) as input_file:
        line = input_file.readline()
        while line:
            packet = json.loads(line)
            data.append(packet)
            line = input_file.readline()
    return pd.DataFrame(data)


def parse_to_csv(config_file):
    """
    Parse an input file to csv file according to a configuration file.
    :param config_file: Path to the configuration file.
    """
    config = Config.load_from_file(config_file)
    input_config = config['input_file']
    output_file = config.get('output_file', './result.csv')  # Default: './result.csv'

    fields_configs = config['fields']
    fields = get_fields(fields_configs)

    df = to_dataframe(input_config)
    result = pd.DataFrame({})
    parse_field(fields, df, result)

    result.to_csv(output_file, index=True)
    return output_file


if __name__ == '__main__':
    parse_to_csv('./modbus_config.json')