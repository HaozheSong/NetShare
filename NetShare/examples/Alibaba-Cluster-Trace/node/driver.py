from netshare.driver import Driver

if __name__ == '__main__':
    driver = Driver(
        working_dir_name='node',
        dataset_file='raw.csv',
        config_file='config.json',
    )
    driver.run()