# NetShare (Practical GAN-based Synthetic IP Header Trace Generation)

NetShare is an end-to-end framework that utilizes Generative Adversarial Networks (GANs) to automatically generate synthetic packet and flow header traces for various networking purposes such as telemetry, anomaly detection, and provisioning.

# Set Up
#### Step1. Clone the repository
```
git clone https://github.com/netsharecmu/NetShare_Summer2023_Internship
```
#### Step2. Set up virtual environment
```
python3.9 -m venv venv
source venv/bin/activate
```
#### Step3. Install NetShare/SDMetrics packages
```
pip install -e NetShare/
pip install -e SDMetrics_timeseries/
```
# Getting Started
## Prepare Data
NetShare supports the following file formats:
* *.csv
* *.pcap
* *.log
## Install (optional)
If you're using Zeek, refer to this [link](https://github.com/netsharecmu/NetShare_Summer2023_Internship/blob/main/NetShare/examples/modbus/Modbus_NetShare.ipynb) for additional install requirements.
## Prepare Configuration File
Create a configuration file in `JSON` format. Follow the steps below, take the syslogs configuration file for reference.
#### Step1. Config *processors*
Config `"processors"` when need specific preprocessors and the postprocessor for raw data, leave it empty if not.

##### Example1. Need specific processor for syslog dataset
```
  "processors": {
    "preprocessors": [
      {
        "class": "ZeekPreprocessor",
        "config": {
          "target_protocol": "syslog"
        }
      },
      {
        "class": "CustomizableFormatPreprocessor",
        "config": {
          "input_file_format": "zeek_log_json"
        }
      },
      {
        "class": "csv_pre_processor"
      }
    ],
    "postprocessors": [
      {
        "class": "csv_post_processor"
      }
    ]
  }
```

##### Example2. Need no processor for raw data
```
  "processors": {
    "preprocessors": [
    ],
    "postprocessors": [
    ]
  },
```
Supported Processors
* preprocessors:
    * `ZeekPreprocessor`: capable of calling `Zeek` to convert `PCAP` file to `Zeek` `.log` format. 
        * `target_protocol`: protocol of pcap/log file (eg. `syslog` and `modbus`)
    * `CustomizableFormatPreprocessor`: capable of converting raw file in different file formats (e.g., `Zeek` `.log` format) into `CSV` file. Also capable of parsing each field respectively according to corresponding parse functions (allow customize).
       * `input_file_format`: set to `csv` for .csv input file, set to `zeek_log_json` for .pcap or .log input file.
    * `csv_pre_processor`: handle the data format that is incompatible with NetShare like IP address, timestamp and list format dataset.
* postprocessors：
    * `csv_post_processor`: convert the data format back to original format, must required if having "csv_pre_processor".

#### Step2. Config *global*
```
  "global_config": {
    "overwrite": true,
    "dataset_type": "netflow",
    "n_chunks": 1,
    "dp": false
  },
```
* overwrite: default is `false`, change to `true` to overwrite existing working directory.
* dataset_type: default is `netflow`, change to `pcap` if need to convert pcap to csv file.
* n_chunks: number of valid chunks, set to 1 will reduce to plain DoppelGANger.
* dp: default is `false`, set to `true` to enable differentially-private training.
#### Step3. Config *default*
```
"default": "single_event_per_row.json"
```
* default: change to `dg_table_row_per_sample.json` for multi-event.
#### Step4. Config *pre_post_processor*
```
"pre_post_processor": {
    "class": "NetsharePrePostProcessor",
    "config": {
      "word2vec": {
        "vec_size": 10,
        "model_name": "word2vec_vecSize",
        "annoy_n_trees": 100,
        "pretrain_model_path": null
      },
      "metadata": [],
      "timeseries": []
    }
  },
  ```
  Add to the config file, keep the defult configuration setting.
#### Step5. Config *model*
```
"model": {
        "class": "DoppelGANgerTorchModel",
        "config": {
            "batch_size": 100,
            "sample_len": [
                10
            ],
            "sample_len_expand": true,
            "epochs": 40,
            "extra_checkpoint_freq": 1,
            "epoch_checkpoint_freq": 5
        }
}
    
```
* batch_size: number of samples used in each iteration of the training process.
* sample_len: length of the input data samples model will generate.
* sample_len_expand: set to `true` when generated samples will be expanded.
* epochs:  number of epochs.
* extra_checkpoint_freq: frequency for checkpoints saved during training.
#### Step6. Config *fields*
Add timestamp, metadata, timeseries fields.
```
 "fields": {
    "timestamp": [
    ],
    "metadata": [
    ],
    "timeseries": [  
    ]
  }
```
Example for a field:
```
      {
        "name": "id.resp_h",
        "parse": "ip_quad2int",
        "format": "integer",
        "abnormal": true,
        "encoding": "bit"
      }
```
* `name`: **required**, key of the field that appeared in the input file.
* `parse:` **optional**, name of the parsing function to be applied on the field defined in parse_func.py. Must ensure that the function exist in parse_func.py. Default is `None`. Supported parsing functions:
  * second2micro
  * ip_quad2int
  * modbus_func2code
  * syslog_facility2code
  * syslog_severity2code
* `format`: **optional**, output format of the field (after parsing). Default is `string`. Supported formats:
  * string
  * float
  * integer
  * list
  * timestamp
  * IP
* `abnormal`: **optional**, set to `true` if this field needs abnormal handling. Default is `false`.
* `encoding`: **required**, the encoding method to be used on the field when training NetShare model. Supported encoding methods:
  * bit
  * word_proto
  * word_port
  * categorical
  * list_attributes
  * float

#### Examples for some special data types
eg. list
```
      {
        "name": "DNS__answers",
        "format": "list",
        "abnormal": true,
        "encoding": "list_values",
        "names": {
          "name": "categorical",
          "type": "float",
          "cls": "float",
          "ttl": "float",
          "dlen": "float",
          "address": "categorical"
        },
        "delimiter": "="
      }
```
eg. IP
```
      {
        "name": "IP__src_s",
        "format": "IP",
        "encoding": "bit",
        "type": "IPv4"
      }
```
eg. timestamp
```
      {
        "name": "ts",
        "parse": "second2micro",
        "format": "timestamp",
        "encoding": "timestamp"
      }

```

Refer to this [link](https://github.com/netsharecmu/NetShare_Summer2023_Internship/blob/modbus_processors/NetShare/examples/syslog/config.json) for the entire syslog example config file.

## Run with NetShare
Create a drive.py file
```
from netshare.driver import Driver

if __name__ == '__main__':
    driver = Driver(
        working_dir_name='syslog',
        dataset_file='syslog.pcap',
        config_file='config.json',
        local_web_port=8060
    )
    driver.run()
```
* working_dir_name: change to your working directory name.
* dataset_file: change to dataset file name.
* config_file: change to the configuration file name.
* local_web_port: choose a port number.

Run the driver file


```
python3 driver.py
```

# Example Usages
provide the example dir path
*   [Syslog](https://github.com/netsharecmu/NetShare_Summer2023_Internship/tree/main/NetShare/examples/syslog)
> A short description of this dataset
*   [Modbus](https://github.com/netsharecmu/NetShare_Summer2023_Internship/tree/main/NetShare/examples/modbus)
> A short description of this dataset
*   [Rahul's Dataset](https://github.com/netsharecmu/NetShare_Summer2023_Internship/tree/main/NetShare/examples/rahul_dataset)
> A short description of this dataset
*   [Alibaba Microservices Traces](https://github.com/netsharecmu/NetShare_Summer2023_Internship/tree/modbus_processors/NetShare/examples/Alibaba-Cluster-Trace)
> The released traces contain the detailed runtime metrics of nearly twenty thousand microservices. They are collected from Alibaba production clusters of over ten thousand bare-metal nodes during twelve hours in 2021.
*   [Cyber Security Events](https://github.com/netsharecmu/NetShare_Summer2023_Internship/tree/main/NetShare/examples/Cyber-Security-Events)
> This data set represents 58 consecutive days of de-identified event data collected from five sources within Los Alamos National Laboratory’s corporate, internal computer network.
*   [JPM Customer Journey Trace](https://github.com/netsharecmu/NetShare_Summer2023_Internship/tree/main/NetShare/examples/JPM-Customer-Journey)
> Customer journey events represent sequences of lower level retail banking clients’ interactions with the bank. Example types of events include login to a web application, making payments, withdrawing money from ATM machines.  The data was generated by running an AI planning-execution simulator and translating the output planning traces into tabular format.






