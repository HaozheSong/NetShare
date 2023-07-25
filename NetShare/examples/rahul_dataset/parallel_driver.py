from netshare.driver import Driver

driver1 = Driver(
    working_dir_name='rahul-dataset-1',
    overwrite_existing_working_dir=True,
    dataset_file='rahul.csv',
    config_file='config.json',
    redirect_stdout_stderr=True,
    local_web=False
)
driver1.run_in_a_process()

driver2 = Driver(
    working_dir_name='rahul-dataset-2',
    overwrite_existing_working_dir=True,
    dataset_file='rahul.csv',
    config_file='config.json',
    redirect_stdout_stderr=True,
    local_web=False
)
driver2.run_in_a_process()
