import random
import json
import netshare.ray as ray
from netshare import Generator
from pre_post_processor_mixed import pre_processor, post_processor
from zeek_processor import parse2csv

if __name__ == '__main__':
    # Change to False if you would not like to use Ray
    ray.config.enabled = False
    ray.init(address="auto")

    # pre processor for the log file 
    preprocess_stage1_output = parse2csv.parse_to_csv('rahul_config.json')

    # pre processor for the csv file 
    Pre_processor = pre_processor.Pre_processor(filename = "traces/rahul.csv", 
                            default_configs = "config_default.json",
                            input_field_configs = "rahul_config.json",
                            output_path = "traces/dataset.csv", 
                            output_config = "sample.json")
    Pre_processor.processor()                            
    # configuration file
    generator = Generator(config="sample.json")

    # `work_folder` should not exist o/w an overwrite error will be thrown.
    # Please set the `worker_folder` as *absolute path*
    # if you are using Ray with multi-machine setup
    # since Ray has bugs when dealing with relative paths.
    generator.train(work_folder=f'../../results/test-dataset')
    generator.generate(work_folder=f'../../results/test-dataset')

    Post_processor = post_processor.Post_processor(
                                input_path = "../../results/test-dataset/post_processed_data",
                                output_path = "../../results/test-dataset/post_processed_data/final_output.csv", 
                                configs = "rahul_config.json")
    Post_processor.processor() 
    generator.visualize(work_folder=f'../../results/test-dataset')

    ray.shutdown()