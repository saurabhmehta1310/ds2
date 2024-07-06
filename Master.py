from concurrent import futures
import grpc
import K_means_pb2 as mapper_pb2
import K_means_pb2_grpc as mapper_pb2_grpc
import K_means_pb2 as reducer_pb2
import K_means_pb2_grpc as reducer_pb2_grpc
import os
import random
import subprocess
import shutil
import time
from pathlib import Path
from multiprocessing.pool import ThreadPool

class Master:
    def __init__(self, input_location, output_location, n_mappers, n_reducers, K,max_iterations):
        self.input_location = input_location
        self.output_location = output_location
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.K = K
        self.max_iterations=max_iterations
        self.mappers=dict()
        self.reducers=dict()
        self.port_num=50400
        for id in range(self.n_mappers):
            self.mappers[id]=self.port_num
            self.port_num +=1
        #port_num=60322
        for id in range(self.n_reducers):
            self.reducers[id]=self.port_num
            self.port_num+=1
        self.mappers_process = self.spawn_mappers(self.mappers)
        self.reducers_process = self.spawn_reducers(self.reducers)
        #Master.reduce(map_intermediate_location)
        #Master.terminate_reducers(reducers_process)





    def input_split(self):
        # files = os.listdir(self.input_location)
        # n_input_files = len(files)
        mapper_to_files_mapping = {}

        # chunk_size = n_input_files // self.n_mappers
        # file_i = 0
        for mapper_id in self.mappers.keys():   
            mapper_to_files_mapping[mapper_id]=f'Inputs/M{mapper_id + 1}.txt'
        
        for mapperId in range(self.n_mappers):
            os.mkdir(f'Mappers/M{mapperId + 1}')
            with open(f'Inputs/M{mapperId + 1}.txt', 'w') as file:
                for point in self.points[mapperId::self.n_mappers]:
                    file.write(f'{point}\n')
                

           


        return mapper_to_files_mapping

    def spawn_mappers(self, mappers):
        mappers_process = []
        for mapper_name, port in mappers.items():
            mapper = subprocess.Popen(['python', 'Mapper.py', str(port), str(mapper_name)])
            mappers_process.append(mapper)
        time.sleep(2)
        return mappers_process

    def spawn_reducers(self, reducers):
        reducers_process = []
        for reducer_name, port in reducers.items():
            reducer = subprocess.Popen(['python', 'Reducer.py', str(port), str(reducer_name)])
            reducers_process.append(reducer)
        time.sleep(2)
        return reducers_process

    def terminate_mappers(self, mappers_process):
        for mapper in mappers_process:
            mapper.terminate()

    def terminate_reducers(self, reducers_process):
        for reducer in reducers_process:
            reducer.terminate()

    def map_function(self, mapper_request, port):
        while True:
            try:
                with grpc.insecure_channel('localhost:' + str(port)) as channel:
                    stub = mapper_pb2_grpc.MapperServiceStub(channel)
                    response = stub.Map(mapper_request)
                    if response.status == mapper_pb2.MapResponse.Status.SUCCESS:
                        print("Mapper {} status: SUCCESS".format(mapper_request.files))
                        return  
                    else:
                        print("Mapper {} status: FAILURE".format(mapper_request.files))
            except Exception as e:
                print(f"Error communicating with mapper {mapper_request.files}: {e}")
                # Restart the mapper process and update the port number
                self.port_num += 1
                mapper = subprocess.Popen(['python', 'Mapper.py', str(self.port_num), str(mapper_request.files)])
                self.mappers_process[mapper_request.files] = mapper
                port = self.port_num
                time.sleep(2)  


    
    def reduce_function(self, reducer_request, port):
        
        while True:
            try:
                with grpc.insecure_channel('localhost:' + str(port)) as channel:
                    stub = reducer_pb2_grpc.ReducerServiceStub(channel)
                    response = stub.Reduce(reducer_request)
                    if response.status == reducer_pb2.ReduceResponse.Status.SUCCESS:
                        print("Reducer {} status: SUCCESS".format(reducer_request.id))
                        centroids = response.centroids
                        return centroids
                    else:
                        print("Reducer {} status: FAILURE".format(reducer_request.id))
                        
            except Exception as e:
                print(f"Error communicating with reducer {reducer_request.id}: {e}")
                self.port_num+=1
                reducer = subprocess.Popen(['python', 'Reducer.py', str(self.port_num), str(reducer_request.id)])
                self.reducers_process[int(reducer_request.id)]=reducer
                port=self.port_num

                time.sleep(2)  

      
        

    def load_centroids(self):
        centroids = []
        with open('centroid.txt') as file:
            for point in file.readlines():
                centroids.append(point.strip())
        return centroids

    def run_kmeans(self):
        centroids = self.initialize_centroids()
        print("Initial centroids:", centroids)

        for iteration in range(1, self.max_iterations + 1):
            centroids = self.load_centroids()
            if os.path.exists('Mappers'):
                shutil.rmtree('Mappers')
            if not os.path.exists('Mappers'):
                os.mkdir('Mappers/')
            if not os.path.exists('Reducers'):
                os.mkdir('Reducers/')

            mapper_to_files_mapping = self.input_split()
            mapper_requests = []
            for mapper_name, port in self.mappers.items():
                request = mapper_pb2.MapRequest(
                    input_location=self.input_location,
                    files=mapper_to_files_mapping[mapper_name],
                    centroids=centroids,
                    n_reducers=self.n_reducers
                )
                mapper_requests.append((request, port))

            with ThreadPool() as pool:
                pool.starmap(self.map_function, mapper_requests)

            intermediate_file_locations = self.shuffle_and_sort("Mappers", "Reducers")
            
            reducer_requests = []
            for reducer_name, port in self.reducers.items():
                points = intermediate_file_locations[reducer_name] if reducer_name in intermediate_file_locations else []
                request = reducer_pb2.ReduceRequest(
                    points=points
                )
                reducer_requests.append((request, port))

            with ThreadPool() as pool:
                centroids_list = pool.starmap(self.reduce_function, reducer_requests)

            
            all_centroids = []
            for i in centroids_list:
                if i:
                    all_centroids.extend(i)

            
            if(self.compare_centroids(centroids,all_centroids)):
                centroids = self.update_centroids(all_centroids)
                print("centroids converge Success")
                break


        
            centroids = self.update_centroids(all_centroids)

            
    def compare_centroids(self, centroids, all_centroids, precision=1e-6):
        
        if len(centroids) != len(all_centroids):
            return False  
        
        for centroid_str, all_centroid_str in zip(centroids, all_centroids):
            centroid_x, centroid_y = map(float, centroid_str.split(',')[1:])
            all_centroid_x, all_centroid_y = map(float, all_centroid_str.split(',')[1:])
            
            if abs(centroid_x - all_centroid_x) > precision or abs(centroid_y - all_centroid_y) > precision:
                return False 
        
        return True

    def initialize_centroids(self):
        self.points = []
        
        with open(self.input_location, 'r') as f:
                    centroid = f.readlines()
                     
                    for i in centroid:
                        
                        self.points.append(i.strip())
        centroids=random.sample(self.points,self.K)
       
        return centroids



    def shuffle_and_sort(self, input_folder, output_folder):
        reducer_data = {}
        mapper_file_index = {}

        # Iterate over mapper folders
        for mapper_folder in os.listdir(input_folder):
            if mapper_folder.startswith("M") and os.path.isdir(os.path.join(input_folder, mapper_folder)):
                mapper_id = int(mapper_folder[1:])  # Extract mapper ID from the folder name
                mapper_input_folder = os.path.join(input_folder, mapper_folder)

                # Initialize file index for the mapper folder
                if mapper_id not in mapper_file_index:
                    mapper_file_index[mapper_id] = 0

                # Iterate over files in each mapper folder
                for filename in os.listdir(mapper_input_folder):
                    if filename.startswith("partition") and filename.endswith(".txt"):
                        with open(os.path.join(mapper_input_folder, filename), 'r') as file:
                            data_points = file.readlines()

                        # Partition data points into reducer files
                        for data_point in data_points:
                            reducer_index = int(filename.strip('partition').strip('.txt'))
                            
                            # partition_filename = f"P{reducer_index}_{mapper_file_index[mapper_id]}.txt"

                            # Append data points to reducer_data dictionary
                            if reducer_index in reducer_data:
                                reducer_data[reducer_index].append(data_point.strip())
                            else:
                                reducer_data[reducer_index] = [data_point.strip()]

                # Increment file index for the mapper folder
                mapper_file_index[mapper_id] += 1

        if reducer_data:
            for partition_index, data_points in reducer_data.items():
                reducer_file_path = os.path.join(output_folder, f"Reducer{partition_index}.txt")
                with open(reducer_file_path, 'w') as reducer_file:
                    reducer_file.writelines('\n'.join(data_points))
        else:
            print("No intermediate file locations found.")

        return reducer_data


                
    def update_centroids(self, reduced_data):
        with open('centroid.txt', 'w') as file:
            file.write('\n'.join(reduced_data))
        return reduced_data


if __name__ == '__main__':
    '''input_location = input("Enter input data location (folder name): ")
    output_location = input("Enter output data location (folder name): ")
    n_mappers = int(input("Enter the number of mappers: "))
    n_reducers = int(input("Enter the number of reducers: "))
    K = int(input("Enter the number of clusters (K): "))'''
    input_location = "points.txt"
    output_location = "Outputs.txt"
    n_mappers = 4
    n_reducers = 2
    K = 2
    max_iteration=10
    

    master = Master(input_location, output_location, n_mappers, n_reducers, K,10)
    master.run_kmeans()
