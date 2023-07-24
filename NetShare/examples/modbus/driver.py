import netshare.ray as ray
from netshare import Generator
from pre_post_processor_mixed import pre_processor, post_processor
from zeek_processor import parse2csv

if __name__ == '__main__':
    # Change to False if you would not like to use Ray
    ray.config.enabled = False
    ray.init(address='auto')

    # pre-processor for the log file
    preprocess_stage1_output_path = '../../results/modbus/pre_processed_data/zeek_preprocessed.csv'
    parse2csv.parse_to_csv(config_path='fields.json', input_path='original.log',
                           output_path=preprocess_stage1_output_path)

    # pre-processor for the csv file
    Pre_processor = pre_processor.Pre_processor(
        filename=preprocess_stage1_output_path,
        default_configs='default.json',
        input_field_configs='fields.json',
        output_path='../../results/modbus/pre_processed_data/final_preprocessed.csv',
        output_config='../../results/modbus/pre_processed_data/final.json'
    )
    Pre_processor.processor()
    # configuration file
    generator = Generator(
        config='../../results/modbus/pre_processed_data/final.json'
    )

    # `work_folder` should not exist o/w an overwrite error will be thrown.
    # Please set the `worker_folder` as *absolute path*
    # if you are using Ray with multi-machine setup
    # since Ray has bugs when dealing with relative paths.
    generator.train(work_folder=f'../../results/modbus')
    generator.generate(work_folder=f'../../results/modbus')

    Post_processor = post_processor.Post_processor(
        input_path='../../results/modbus/post_processed_data',
        output_path='../../results/modbus/post_processed_data/final_output.csv',
        configs='fields.json'
    )
    Post_processor.processor()
    generator.visualize(work_folder=f'../../results/modbus')

    ray.shutdown()
