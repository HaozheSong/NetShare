from netshare.driver import Driver

if __name__ == '__main__':
    driver = Driver(
        working_dir_name='JPM-Customer-Journey',
        dataset_file='journey-small.csv',
        config_file='config.json',
    )
    driver.run()