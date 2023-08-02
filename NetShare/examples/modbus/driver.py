from netshare.driver import Driver

if __name__ == '__main__':
    driver = Driver(
        working_dir_name='modbus',
        dataset_file='run8.pcap',
        config_file='config.json'
    )
    driver.run()
