from netshare.driver import Driver


class GrpcDriver(Driver):
    def __init__(self, working_dir_name, dataset_file_name, config_file_name):
        # working_dir = '.../NetShare/results/<working_dir_name>'
        self.working_dir = self.results_dir.joinpath(working_dir_name)
        self.working_dir.mkdir(parents=True, exist_ok=True)

        # src_dir stores original dataset and config.json uploaded by the user (only for WebDriver)
        # src_dir = '.../NetShare/results/<working_dir_name>/src'
        self.src_dir = self.working_dir.joinpath('src')
        self.src_dir.mkdir(parents=True, exist_ok=True)

        # dataset_file = '.../NetShare/results/<working_dir_name>/src/<dataset_file_name>'
        self.dataset_file = self.src_dir.joinpath(dataset_file_name)
        # config_file = '.../NetShare/results/<working_dir_name>/src/<config_file_name>'
        self.config_file = self.src_dir.joinpath(config_file_name)

        super().__init__(
            working_dir_name,
            str(self.dataset_file.resolve()),
            str(self.config_file.resolve()),
            overwrite_existing_working_dir=False,
            redirect_stdout_stderr=True,
            separate_stdout_stderr_log=False,
            local_web=False
        )

    def read_stdout_stderr_log(self):
        with open(self.stdout_stderr_log_file) as log_fd:
            log_content = log_fd.read()
        return {'log_file_name': self.stdout_stderr_log_file.name, 'log_file_content': log_content}
