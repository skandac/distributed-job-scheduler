import grpc
from concurrent import futures
from internal.proto import scheduler_pb2_grpc, scheduler_pb2
from internal.scheduler.grpc_server import SchedulerServicer
import time

class serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    scheduler_servicer = SchedulerServicer()
    scheduler_pb2_grpc.add_SchedulerServiceServicer_to_server(scheduler_servicer, server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC server started on port 50051")
    try:
        while True:
            time.sleep(86400)  # Keep the server running
    except KeyboardInterrupt:
        server.stop(0)
        print("gRPC server stopped")