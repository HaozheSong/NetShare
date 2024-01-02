import time
import datetime
import grpc

from grpc_server import task_pb2, task_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = task_pb2_grpc.TaskStub(channel)

now_utc = datetime.datetime.now(datetime.timezone.utc)
now_utc_str = now_utc.strftime('%Y%m%d_%H%M%S')
start_request = []
# dataset_file_path = '../examples/rahul_dataset/rahul.csv'
# config_file_path = '../examples/rahul_dataset/config.json'
dataset_file_path = '../examples/Alibaba-Cluster-Trace/call-graph/raw.csv'
config_file_path = '../examples/Alibaba-Cluster-Trace/call-graph/config.json'
chunk_size = 1024
with open(dataset_file_path, 'rb') as dataset_fd:
    with open(config_file_path, 'rb') as config_fd:
        while True:
            dataset_chunk = dataset_fd.read(chunk_size)
            config_chunk = config_fd.read(chunk_size)
            if len(dataset_chunk) == 0 and len(config_chunk) == 0:
                break
            request = task_pb2.StartingTask(
                task_id=0,
                task_kind='customized',
                created_at=now_utc_str,
                dataset_file_name='raw.csv',
                dataset_file_content=dataset_chunk,
                config_file_name='config.json',
                config_file_content=config_chunk
            )
            start_request.append(request)
start_response = stub.StartTask(iter(start_request))
print(f'{start_response.task_name} created')

query_request = task_pb2.RunningTask(
    task_id=0,
    task_name=start_response.task_name
)
query_response = stub.QueryStatus(query_request)
while not query_response.is_completed:
    print(query_response.log_file_content)
    query_response = stub.QueryStatus(query_request)
    time.sleep(3)

result_request = task_pb2.CompletedTask(
    task_id=0,
    task_name=start_response.task_name
)
for result_response in stub.GetResult(result_request):
    print(result_response.file_name)
    print(len(result_response.file_content))
