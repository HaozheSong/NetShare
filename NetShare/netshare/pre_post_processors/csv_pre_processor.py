import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
import collections
import ipaddress
from datetime import datetime
import json
import re

class csv_pre_processor(object):
    def __init__(self, input_dataset, input_field_configs, output_dataset, output_config):
        self.df = pd.read_csv(input_dataset, index_col=0) 
        ## special fields store list, IP, time columns and its encoding method. 
        self.special_fields = {}
        self.changed_columns = {}
        self.fields = []
        self.deleted_columns = []
        self.input_field_configs = input_field_configs
        self.name_lists = collections.defaultdict(list)
        self.output_path = output_dataset
        self.output_config = output_config


    def create_configuration_file(self):
        ##1. Open the default config file 
        with open(self.input_field_configs, 'r') as openfile:
            json_object = json.load(openfile)
        openfile.close()
        for field in ["metadata", "timeseries", "timestamp"]: 
            for col in json_object["fields"][field]: 
                name = col["name"]
                format = col["format"]
                encoding = col["encoding"]
                if "type" in col:
                    type = col["type"]
                else:
                    type = None 
                if "names" in col:
                    names = col["names"]
                else:
                    names = None
                if "delimiter" in col:
                    delimiter = col["delimiter"]
                else:
                    delimiter = None
                if "time_format" in col: 
                    time_format = col["time_format"]
                else: 
                    time_format = None
                if "normalization" in col:
                    normalization = col["normalization"]
                else: 
                    normalization = None

                res = self.create_json_obj(json_object, field, name, format, encoding, type, names, delimiter, time_format, 
                                            normalization)
                if res == False: 
                    col_info = {"name": name, "fields": field}
                    self.deleted_columns.append(col_info)
        
        json_object["global_config"]["original_data_file"] = str(self.output_path)

        #3. Serializing json
        json_object = json.dumps(json_object, indent=4)
        

        #4. Writing to sample.json
        with open(self.output_config, "w") as outfile:
            outfile.write(json_object)

    def get_obj(self, column, encoding):
        if encoding == "bit":
            obj = {
                    "column": column, 
                    "type": "integer", 
                    "encoding": "bit", 
                    "n_bits": 32,
                    "categorical_mapping": False
                    } 
        elif encoding == "word_proto": 
            obj = {
                    "column": column,
                    "type": "integer",
                    "encoding": "word2vec_proto"
                }
        elif encoding == "categorical":
            obj = {
                "column": column,
                "type": "string",
                "encoding": "categorical"
            }
        elif encoding == "float":
            obj =  {
                    "column": column,
                    "type": "float",
                    "normalization": "ZERO_ONE",
                    "log1p_norm": True
                }
        elif encoding == "timestamp":
            obj = {
                    "column": column,
                    "generation": True,
                    "encoding": "interarrival",
                    "normalization": "ZERO_ONE"
                } 
        else: 
            obj = {
                    "column": column,
                    "type": "integer",
                    "encoding": "word2vec_port"
                }
        return obj

    def create_json_obj(self, json_object, Fields, column, format, encoding, type, names, delimiter, time_format, 
                        normalization):
        # Fields: "metadata", "timestamp", "timeseries"
        # column: column name in dataset 
        # format: data format {integer, float, string, timestamp, IP, list}
        # encoding: {bit, word_port, word_proto, float, list_attribute, list_value}
        # type: None or specific value for IP or timestamp (IP: IPv4, IPv6 Timestamp: processed, unprocessed)
        # names: values in the list         
        if format == "integer":
            ## format is integer: encoding will include { bit, word_proto, word_port, categorical }
            if self.df.dtypes[column] == object: 
                print('Column ', column,' may not be integer, so we ignore it.')
                return False
            obj = self.get_obj(column, encoding)
            self.fields.append(column)
            json_object["pre_post_processor"]["config"][Fields].append(obj)
        elif format == "string":
                ## format is string: encoding will include { categorical }
            if self.df.dtypes[column] != object: 
                print('Column ', column,' may not be string, so we ignore it.')
                return False
            obj = self.get_obj(column, encoding)
            self.fields.append(column)
            json_object["pre_post_processor"]["config"][Fields].append(obj)
        elif format == "float":
        ## format is string: encoding will include { float }
            obj = self.get_obj(column, encoding)
            self.fields.append(column)
            json_object["pre_post_processor"]["config"][Fields].append(obj)
        elif format == "timestamp":
             if type == "unprocessed": 
                self.special_fields[column] = "timestamp"
                self.changed_columns[column] = {}
                self.changed_columns[column]["encoding"] = "timestamp"
                self.changed_columns[column]["time_format"] = time_format
             obj = self.get_obj(column, "timestamp")
             self.fields.append(column)
             json_object["pre_post_processor"]["config"][Fields] = (obj)
        elif format == "IP":
             if self.df.dtypes[column] != object: 
                print('Column ', column,' may not be IP address, so we ignore it.')
                return False
             if type == "IPv4":
                  self.special_fields[column] = "IPv4"
                  self.changed_columns[column] = "IPv4"
             else: 
                  self.special_fields[column] = "IPv6"
                  self.changed_columns[column] = "IPv6"
             obj = self.get_obj(column, "bit")
             self.fields.append(column)
             json_object["pre_post_processor"]["config"][Fields].append(obj)
        elif format == "list":
            ## name_lists =  {"packet__layers": ["IP", "TCP", ....]}
            ## format is list --> encoding is "list_attributes", "list_values"
            if encoding == "list_attributes": 
                for name in names: 
                    ## special_fields = {"packet__layers": "list__attributes", "IP__src_s": "IPv4"}
                    obj = self.get_obj(column + "_" + name, "categorical")
                    self.fields.append(column + "_" + name)
                    json_object["pre_post_processor"]["config"][Fields].append(obj)
                    self.name_lists[column].append(name)
                    self.df[column + "_" + name] = "No"
                    if column not in self.changed_columns:
                        self.changed_columns[column] = {}
                        self.changed_columns[column]["encoding"] = "list_attributes"
                        self.changed_columns[column]["new_columns"] = []
                        self.changed_columns[column]["delimiter"] = delimiter 
                    self.changed_columns[column]["new_columns"].append(column + "_" + name)
                    self.special_fields[column] = "list_attributes"
            else: 
                ##  current supported version for list values: xxx = xxxx 
                for name, encoding in names.items():
                    self.fields.append(column + "_" + name)
                    self.name_lists[column].append(name)
                    obj = self.get_obj(column + "_" + name, encoding)
                    if column not in self.changed_columns:
                        self.changed_columns[column] = {}
                        self.changed_columns[column]["encoding"] = "list_values"
                        self.changed_columns[column]["new_columns"] = {}
                        self.changed_columns[column]["delimiter"] = delimiter 
                    self.changed_columns[column]["new_columns"][column + "_" + name] = {"encoding": encoding, "origin": name}
                    json_object["pre_post_processor"]["config"][Fields].append(obj)
                self.special_fields[column] = "list_values"
        return True



    def handle_special_fields(self):
        for i in range(len(self.df.index)):
            for col, encoding in self.special_fields.items():
                if encoding == "list_attributes" or encoding == "list_values":
                    if encoding == "list_attributes":
                        delimiter = self.changed_columns[col]["delimiter"]
                        for item in self.df.loc[i, col].split(delimiter):
                            if col in self.name_lists and item in self.name_lists[col]:  
                                self.df.loc[i, col + "_" + item] = "Yes"
                    elif encoding == "list_values":
                        origin_strings = self.df.loc[i, col]
                        delimiter = self.changed_columns[col]["delimiter"]
                        for new_col, attrs in self.changed_columns[col]["new_columns"].items():
                            if origin_strings == -1 or origin_strings == "unavailable": 
                                if attrs["encoding"] == "string":
                                    self.df.loc[i, new_col] = "Unavailable"
                                else: 
                                    self.df.loc[i, new_col] = 0
                            else: 
                                print("origin string is ", origin_strings)
                                match = re.search(attrs["origin"], origin_strings)
                                strs = origin_strings[match.end() + 1:].split(delimiter)[1].strip(' ')
                                if strs == "":
                                    if attrs["encoding"] == "categorical":
                                        self.df.loc[i, new_col] = "Unavailable" 
                                    else: 
                                        self.df.loc[i, new_col] = 0
                                elif " " in strs: 
                                    if attrs["encoding"] == "categorical":
                                        if strs.split("\n")[0] == "": 
                                            self.df.loc[i, new_col] = "Unavailable" 
                                        else:
                                            self.df.loc[i, new_col] = strs.split("\n")[0] 
                                    else: 
                                        if strs.split("\n")[0] == "": 
                                            self.df.loc[i, new_col] = 0
                                        else:
                                            self.df.loc[i, new_col] = int(strs.split("\n")[0])
                                else: 
                                    if attrs["encoding"] == "categorical":
                                        if strs.split("\n")[0] == "": 
                                            self.df.loc[i, new_col] = "Unavailable" 
                                        else:
                                            self.df.loc[i, new_col] = strs
                                    else: 
                                        if strs.split("\n")[0] == "": 
                                            self.df.loc[i, new_col] = 0
                                        else:
                                            self.df.loc[i, new_col] = int(strs)
                elif encoding == "IPv4" or encoding == "IPv6":
                    self.convert_IP_to_int(i, self.df.loc[i, col], col, encoding)
                else: 
                    ## timestamp 
                    time_format = self.changed_columns[col]["time_format"]
                    self.convert_time_to_ns(i, col, time_format)
        
    def convert_IP_to_int(self, i, orig_ip, colname, type):
        if '.' in orig_ip and type == "IPv4":
            IP__addr = ipaddress.IPv4Address(orig_ip)
            self.df.loc[i, colname] = int(IP__addr)
        elif '.' in orig_ip and type == "IPv6":
            IP__addr = ipaddress.IPv4Address(orig_ip)
            self.df.loc[i, colname] = int(IP__addr)
        else: 
            self.df.loc[i, colname] = 0

    def convert_time_to_ns(self, i, col, time_format):
        datetime_str = self.df.loc[i, col]
        ##'%Y-%m-%d %H:%M:%S.%f'
        strs = datetime.strptime(datetime_str, time_format)
        date64 = np.datetime64(strs)
        ts = (date64 - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's') * 100000 
        ts = int(ts)
        self.df.loc[i, col] = ts


    ##check if one column in df exists -1 or NaN. if exists, then add to abnormal column list
    def detect_abnormal_value(self):
        abnormal_col_lists = []
        col_lists = list(self.df.columns.values)
        for col in col_lists:
            val_lists = self.df[col].tolist()
            if -1 in val_lists or "-1" in val_lists or self.df[col].isnull().sum() > 0: 
                abnormal_col_lists.append(col)
        return abnormal_col_lists
    
    def change_abnormal_value(self, col_lists): 
        for col in col_lists:
            self.df[col] = self.df[col].replace(-1, 0)
            self.df[col] = self.df[col].fillna(0)

    def processor(self):
        self.create_configuration_file()
        self.handle_special_fields()

        abnormal_lists = self.detect_abnormal_value() 
        print("abnormal lists are ", abnormal_lists)
        self.change_abnormal_value(abnormal_lists)
        for col, v in self.special_fields.items():
            if v == "IPv4" or v == "IPv6" or v == "timestamp":
                self.df[[col]] = self.df[[col]].apply(pd.to_numeric) 
            if v == "list_values":
                for new_col, attrs in self.changed_columns[col]["new_columns"].items():
                    if attrs["encoding"] == "bit":
                        self.df[new_col] = self.df[new_col].astype(int)

        self.df = self.df[[i for i in self.fields]]
        

        with open(self.input_field_configs) as json_file:
            data = json.load(json_file)
        data['changed_fields'] = self.changed_columns

        for col in self.deleted_columns:
            for element in data["fields"][col['fields']]: 
                if element["name"] == col["name"]:
                    data["fields"][col['fields']].remove(element)

        with open(self.input_field_configs, 'w') as json_file:
            json.dump(data, json_file)

        self.df.to_csv(self.output_path, index = False)
            


    