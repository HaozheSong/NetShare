from netshare.driver import Driver

if __name__ == '__main__':
    driver = Driver(
        working_dir_name='dns',
        dataset_file='dns.csv',
        config_file='config.json',
    )
    driver.run()
