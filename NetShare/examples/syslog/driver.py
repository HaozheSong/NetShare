from netshare.driver import Driver

if __name__ == '__main__':
    driver = Driver(
        working_dir_name='syslog',
        dataset_file='original.log',
        config_file='config.json',
    )
    driver.run()
