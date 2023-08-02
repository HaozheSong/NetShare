from netshare.driver import Driver

if __name__ == '__main__':
    driver = Driver(
        working_dir_name='Cyber-Security-Events-flows',
        dataset_file='flows.csv',
        config_file='config.json',
    )
    driver.run()