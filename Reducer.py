import grpc
import K_means_pb2 as reducer_pb2
import K_means_pb2_grpc as reducer_pb2_grpc
import os
import subprocess
import time
from concurrent import futures
from pathlib import Path
from multiprocessing.pool import ThreadPool
import sys

class ReducerServiceServicer(reducer_pb2_grpc.ReducerServiceServicer):
    def __init__(self, reducer_name):
        self.reducer_name = reducer_name
    
    def compute_centroids(self,centroids):
        centroid_values = []
        if centroids==[]:
            return [] 
        num_coordinates = len(centroids[0].split(','))  
        num_centroids = len(centroids)
        # Iterate over each coordinate index
        for coord_idx in range(num_coordinates):
            coord_sum = sum(float(centroid.split(',')[coord_idx]) for centroid in centroids)  # Calculate the sum of coordinates
            coord_avg = coord_sum / num_centroids  # Calculate the average of coordinates
            centroid_values.append(coord_avg)  # Append the average coordinate to centroid_values
        centroid_values_str = ','.join(str(coord) for coord in centroid_values)
        return [centroid_values_str]
    def Reduce(self, request, context):
        try:
            cluster_assignments = request.points
            
            
            centroids = self.compute_centroids(cluster_assignments)
            print('centroids: ', centroids)
            return reducer_pb2.ReduceResponse(
                status=reducer_pb2.ReduceResponse.Status.SUCCESS,
                output_file_path="path/to/output",
                centroids=centroids
            )
        except Exception as e:
            print("Error in Reduce method:", e)
            return reducer_pb2.ReduceResponse(
                status=reducer_pb2.ReduceResponse.Status.FAILURE,
                output_file_path=""
            )

class Reducer:
    def __init__(self, port, reducer_name):
        self.port = port
        self.reducer_name = reducer_name

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        reducer_pb2_grpc.add_ReducerServiceServicer_to_server(
            ReducerServiceServicer(self.reducer_name), server
        )
        server.add_insecure_port("[::]:" + str(self.port))
        server.start()
        print("Reducer {} started on port {}".format(self.reducer_name, self.port))
        server.wait_for_termination()

if __name__ == "__main__":
    port = int(sys.argv[1])
    reducer_name = sys.argv[2]
    reducer_server = Reducer(port, reducer_name)
    reducer_server.serve()
