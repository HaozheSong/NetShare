import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
import ipaddress
from datetime import datetime
import glob 
import json 
import ipaddress
pd.set_option('display.max_columns', None)


class Post_processor(object):
    def __init__(self, input_path, output_path, configs):
        self.input_path = input_path
        self.output_path = output_path 
        self.metadata = []
        with open(configs) as json_file:
            json_object = json.load(json_file)
            self.changed_fields = json_object["changed_fields"]
            self.columns = []
            for col in json_object["fields"]["metadata"]:
                self.columns.append(col["name"])
                self.metadata.append(col["name"])
            for col in json_object["fields"]["timeseries"]:
                self.columns.append(col["name"])
            for col in json_object["fields"]["timestamp"]:
                self.columns.append(col["name"])
        filenames = glob.glob(self.input_path + "/*.csv")
        print(filenames)
        self.df = pd.read_csv(filenames[0]) 

    def convert_int_to_IP(self, i, int_ip, colname, ip_type):
        int_ip = int(int_ip)
        if int_ip == 0:
            self.df.loc[i, colname] = int(int_ip)
        elif ip_type == "IPv4": 
            IP__addr = str(ipaddress.IPv4Address(int_ip))
            self.df.loc[i, colname] = IP__addr
        elif ip_type == "IPv6":
            IP__addr = str(ipaddress.IPv6Address(int_ip))
            self.df.loc[i, colname] = IP__addr

    def convert_ns_to_time(self, i, colname):
        ts = self.df.loc[i, colname] 
        date64 = (ts / 100000) * np.timedelta64(1, 's') + np.datetime64('1970-01-01T00:00:00Z')
        ##%Y-%m-%d %H:%M:%S.%f
        datetime = date64.item().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.df.loc[i, colname] = datetime

    def generate_flow_id(self):
        self.df = self.df.sort_values(self.metadata)
        s = self.df.duplicated(self.metadata)
        self.df["flow_id"] = (~s).cumsum()
        
            

    def processor(self):
        #col_lists = ["IP__len", "IP__p", "IP__ttl", "TCP__seq", "TCP__flags", "TCP__sport", "TCP__dport",
         #            "IP__type", "DNS__query", "DNS__dlen", "DNS__an", "DNS__ttl" ,              
          #          "DNS__opcode", "DNS__type", "IEEE__type", "IEEE__dsr", "MQTT__mlen", "UDP__dport", 
           #         "UDP__sport"]
        # "changed_fields": {"IP__src_s": "IPv4", "IP__dst_s": "IPv4",
        #  "packet__layers": {"encoding": "list_attributes", 
        # "new_columns": ["packet__layers", "packet__layers", "packet__layers"]}, "packet__time": "timestamp"}
        for col, encoding in self.changed_fields.items():
            if encoding == "IPv4" or encoding == "IPv6": 
                for i in range(len(self.df.index)):
                    self.convert_int_to_IP(i, self.df.loc[i, col], col, encoding)
            elif encoding == "timestamp": 
                for i in range(len(self.df.index)):
                    self.convert_ns_to_time(i, col)
            else:
                self.df[col], cols = "", []
                if encoding["encoding"] == "list_attributes": 
                    for i in range(len(self.df.index)):
                        categories = encoding["new_columns"]
                        for c in categories:
                            if c not in cols: 
                                cols.append(c)
                            if self.df.loc[i, c] == "Yes":
                                label = c.split("_")[-1]
                                if len(self.df.loc[i, col]) == 0:
                                    self.df.loc[i, col] += label
                                else:
                                    self.df.loc[i, col] += encoding["delimiter"] + label
                    self.df = self.df.drop(columns = cols)
                else: 
                    for i in range(len(self.df.index)):
                        columns = encoding["new_columns"]
                        string_lists, cols = [], []
                        print(columns)
                        for item in columns: 
                            print("item is ", item)
                            print(self.df.loc[i, item])
                            print(columns[item]["origin"])
                            if item not in cols: 
                                cols.append(item)
                            string_lists.append(columns[item]["origin"] + encoding["delimiter"] + str(self.df.loc[i, item]))
                        final_string = "\n".join(string_lists)
                        self.df.loc[i, col] = final_string
                    self.df = self.df.drop(columns = cols)
        
        #self.df = self.df[self.columns]
        self.generate_flow_id()
        self.df.to_csv(self.output_path, index = False)