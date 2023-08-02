from netshare.driver import Driver

if __name__ == '__main__':
    driver = Driver(
        working_dir_name='proc',
        dataset_file='proc.csv',
        config_file='config.json',
    )
    driver.run()
