from netshare.driver import Driver

if __name__ == '__main__':
    driver = Driver(
        working_dir_name='syslog',
        dataset_file='syslog.pcap',
        config_file='config.json'
    )
    driver.run()
