For Master.py

Imports: Import necessary modules including gRPC, subprocess, os, random, shutil, time, Path, and ThreadPool
Class Definition: Master:
Initializes the master node with input data location, output data location, number of mappers, number of reducers, K (number of clusters), and maximum iterations.
Initializes mapper and reducer processes, spawning them as separate subprocesses.
Defines methods for input splitting, spawning mappers and reducers, terminating mappers and reducers, map and reduce functions, KMeans running logic, centroid initialization, shuffle and sort, and centroid updating.
Input Splitting (input_split):Splits the input data into chunks for each mapper.
Mapper and Reducer Processes (spawn_mappers and spawn_reducers):Spawns mapper and reducer processes as subprocesses.
Map and Reduce Functions (map_function and reduce_function):Defines functions to be executed by mappers and reducers.
Communicates with mappers and reducers using gRPC.
K-Means Execution (run_kmeans):Executes the K-Means algorithm iteratively.Initializes centroids.Iterates through mapping, shuffling and sorting, reducing, and centroid updating phases for a specified number of iterations.
Centroid Initialization (initialize_centroids):Initializes centroids, typically by randomly selecting K data points as initial centroids.
Shuffle and Sort (shuffle_and_sort):Sorts data points within each cluster and writes them to intermediate files.
Shuffle and Sort (shuffle_and_sort):Sorts data points within each cluster and writes them to intermediate files.
Centroid Updating (update_centroids):Calculates updated centroids based on the mean of data points in each cluster.
Main Block:Defines input parameters for the K-Means algorithm (input and output locations, number of mappers, number of reducers, K).

For  Mapper.py:
Contents:
MapperServiceServicer: This class implements the gRPC service for the mapper. It contains methods to handle incoming requests from the master node, partition data points, and map them to the nearest centroid.
Mapper: This class sets up and starts the gRPC server for the mapper service. It listens for incoming requests from the master node and dispatches them to the MapperServiceServicer.
Usage: The main block of the script sets up the mapper server by specifying the port number and mapper name from the command line arguments. It initializes an instance of the Mapper class and starts the gRPC server to listen for incoming requests.
This repository contains code for a distributed mapper service used in a K-Means clustering algorithm implementation. The mapper service is designed to work with gRPC for communication between different components of the distributed system.

Reducer.py:
This repository contains code for a distributed reducer service used in a K-Means clustering algorithm implementation. The reducer service is designed to work with gRPC for communication between different components of the distributed system
ReducerServiceServicer: This class implements the gRPC service for the reducer. It contains methods to handle incoming requests from the master node, reduce data points assigned to each centroid, and calculate updated centroids.
Reducer: This class sets up and starts the gRPC server for the reducer service. It listens for incoming requests from the master node and dispatches them to the ReducerServiceServicer.
Usage: The main block of the script sets up the reducer server by specifying the port number and reducer name from the command line arguments. It initializes an instance of the Reducer class and starts the gRPC server to listen for incoming request
This code assumes that the reducer receives clustered data points from the master node, reduces them to calculate updated centroids, and writes the updated centroids to an output file. Adjustments may be required based on specific clustering requirements.

K_means.proto:
This Protocol Buffers (protobuf) file defines the message types and gRPC services used in a distributed K-Means clustering system.
1. MapRequest Message:
This message represents the request sent by the mapper to the master node.
It includes:
input_location: The location of input data.
files: List of file paths containing data points.
centroids: List of centroid coordinates.
n_reducers: Number of reducers in the system.
2. MapResponse Message:
This message represents the response sent by the master node to the mapper.
It includes:
status: Status of the mapping operation (SUCCESS or FAILURE).
intermediate_file_location: List of file locations containing mapped data.
MapperService Service:
This service defines the gRPC endpoint for the mapper.
It includes a single RPC method Map, which takes a MapRequest as input and returns a MapResponse.
4. ReduceRequest Message:
This message represents the request sent by the master node to the reducer.
It includes:
cluster_assignments: String representation of clustered data points.
5. ReduceResponse Message:
This message represents the response sent by the reducer to the master node.
It includes:
status: Status of the reducing operation (SUCCESS or FAILURE).
output_file_path: File path where the updated centroids are written.
6. ReducerService Service:

This service defines the gRPC endpoint for the reducer.
It includes a single RPC method Reduce, which takes a ReduceRequest as input and returns a ReduceResponse.
