import grpc
from concurrent import futures
import logging
import sys

from netshare.driver import Driver
from grpc_server import task_pb2, task_pb2_grpc

from grpc_server.grpc_driver import GrpcDriver

logger = logging.getLogger('NetShareGrpcServer')
logger.setLevel(logging.INFO)
logger.propagate = False
formatter = logging.Formatter('[%(asctime)s:%(levelname)s] %(message)s')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
file_handler = logging.FileHandler(
    'grpc_server.log', mode='a', encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


class TaskServicer(task_pb2_grpc.TaskServicer):
    def __init__(self):
        super().__init__()
        self.tasks = {}

    def StartTask(self, request_iterator, context):
        logger.info(f'Request to start a task from {context.peer()}')
        task = None
        for request in request_iterator:
            if task is None:
                working_dir_name = generate_working_dir_name(request.task_id,
                                                             request.task_kind,
                                                             request.created_at)
                task_name = working_dir_name
                task = GrpcDriver(
                    task_id=request.task_id,
                    working_dir_name=working_dir_name,
                    dataset_file_name=request.dataset_file_name,
                    config_file_name=request.config_file_name
                )
                self.tasks[request.task_id] = task
                dataset_fd = open(task.dataset_file, 'ab')
                config_fd = open(task.config_file, 'ab')
            dataset_fd.write(request.dataset_file_content)
            config_fd.write(request.config_file_content)
        dataset_fd.close()
        config_fd.close()
        task.run_in_a_process()
        return task_pb2.StartingStatus(
            task_id=request.task_id,
            task_name=task_name,
            is_successful=True
        )

    def QueryStatus(self, request, context):
        logger.info(
            f'Request to query the status of the task from {context.peer()}'
        )
        if request.task_id in self.tasks:
            task = self.tasks[request.task_id]
            log = task.read_stdout_stderr_log()
            if task.process.is_alive():
                is_completed = False
            else:
                is_completed = True
            return task_pb2.RunningStatus(
                task_id=request.task_id,
                task_name=request.task_name,
                is_completed=is_completed,
                log_file_name=log['log_file_name'],
                log_file_content=log['log_file_content']
            )

        # can not find such a task in this server session
        # but task exists on disk
        working_dir_name = request.task_name
        working_dir = Driver.results_dir.joinpath(working_dir_name)
        log_file = working_dir.joinpath(
            f'logs/{Driver.stdout_stderr_log_file_name}')
        if log_file.is_file():
            with open(log_file) as log_file_fd:
                return task_pb2.RunningStatus(
                    task_id=request.task_id,
                    task_name=request.task_name,
                    # TODO: is a on-disk task regarded as completed or uncompleted?
                    is_completed=True,
                    log_file_name=log_file.name,
                    log_file_content=log_file_fd.read()
                )

        return 'task does not exist'

    def GetResult(self, request, context):
        logger.info(
            f'Request to get result of {request.task_name} from {context.peer()}'
        )
        task = self.tasks[request.task_id]
        for file_path in task.result_dir.iterdir():
            if (file_path.suffix in ['.json', '.log'] or
                    file_path.name == 'synthetic_data.csv'):
                with open(file_path, 'rb') as fd:
                    yield task_pb2.ResultFile(
                        file_name=file_path.name,
                        file_content=fd.read()
                    )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    task_pb2_grpc.add_TaskServicer_to_server(
        TaskServicer(), server
    )

    server.add_insecure_port('[::]:50051')
    logger.info('Starting gRPC server at [::]:50051')
    server.start()
    server.wait_for_termination()


def generate_working_dir_name(task_id, task_kind, created_at):
    return f'{task_id}_{task_kind}_{created_at}'


if __name__ == '__main__':
    serve()
