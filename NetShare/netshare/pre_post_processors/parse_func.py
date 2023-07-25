import socket
import sys
import pandas as pd

current_module = sys.modules[__name__]


def parse(value, field, if_handle_abnormal, func_name=None):
    """
    Entrance of all the parse functions.
    :param value: The value of the field.
    :param field: The field configuration.
    :param if_handle_abnormal: If the field need abnormal handling.
    :param func_name: The name of the parse function, or None if no need to parse.
    :return: The value after abnormal handling and parsing.
    """
    # Handle abnormal
    if if_handle_abnormal:
        old = value
        new = handle_abnormal(value, field)
        if old != new:
            return new

    # Apply parsing function
    if func_name is not None:
        return getattr(current_module, func_name, None)(value)
    else:
        return value


def second2micro(second):
    """
    :param second: Time in second
    :return: Time in microsecond
    """
    return second * 1000000


def ip_quad2int(ip_quad_string):
    """
    Parse a dotted-quad string IP address (e.g., 192.168.0.0) to a unsigned int
    by first convert the IP address back to 32-bit binary format and then covert the binary to decimal.

    :param ip_quad_string: IP address in dotted-quad string format (e.g., 192.168.0.0)
    :return: Converted unsigned int result
    """
    # Bytes format of the IP address (192.168.0.0 -> b'\xc0\xa8\x00\x00)
    ip_bytes = socket.inet_aton(ip_quad_string)
    # Convert the bytes to a decimal with big endian byte order
    return int.from_bytes(ip_bytes, 'big')


def modbus_func2code(func_name):
    """
    :param func_name: Modbus function name
    :return: Corresponding modbus function code
    """
    if func_name == 'READ_COILS':
        return 1
    elif func_name == 'READ_DISCRETE_INPUTS':
        return 2
    elif func_name == 'READ_HOLDING_REGISTERS':
        return 3
    elif func_name == 'READ_INPUT_REGISTERS':
        return 4
    elif func_name == 'WRITE_SINGLE_COIL':
        return 5
    elif func_name == 'WRITE_SINGLE_REGISTER':
        return 6
    else:
        return 0


def handle_abnormal(value, field):
    """
    Abnormal handling function.
    Definition of abnormal:
        * Value of numbers less than 0;
        * Empty field.
    If detected abnormal, will return `unavailable` or `0` according to the desired output format
    in field configuration;
    If not, return the original value.
    :param value: The value to be handled.
    :param field: The field configuration.
    :return: The value after abnormal handling.
    """
    if field.get('format', 'str') == 'str':
        abnormal = 'unavailable'
    else:
        abnormal = 0
    if value is None:
        return abnormal
    if isinstance(value, str):
        value = value.strip()
        if value == '':
            return abnormal
        else:
            return value
    if isinstance(value, (int, float)):
        if value < 0 or pd.isna(value):
            return abnormal
        else:
            return value
    return value
