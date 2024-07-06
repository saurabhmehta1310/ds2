from concurrent import futures

import grpc
import K_means_pb2 as mapper_pb2
import K_means_pb2_grpc as mapper_pb2_grpc

from pathlib import Path
import datetime
import os
import pandas as pd
import sys
import math


class MapperServiceServicer(mapper_pb2_grpc.MapperServiceServicer):
	def __init__(self, mapper_name):
		self.mapper_name = mapper_name

	def file_read(self, path):
		with open(path, "r") as file:
			file_content = file.read().splitlines()
		return file_content

	def file_write(self, path, content):

		with open(path, "a+") as file:
			file.write(content + "\n")

	def partition(self, key):
		return key % (self.n_reducers)


	def find_nearest_centroid(self,data_point, centroids):
		min_distance = math.inf
		nearest_centroid = None

		for i,centroid_str in enumerate(centroids):
			
			distance = self.calculate_distance(data_point, centroid_str)
			if distance < min_distance:
				min_distance = distance
				nearest_centroid = i

		return nearest_centroid

	def calculate_distance(self,point1, point2):
		
		distance = math.sqrt(sum((x - y) ** 2 for x, y in zip(point1, point2)))
		return distance

	def Map(self, request, context):
		data_points = []
		with open(request.files, 'r') as file:
			# Adjust for your data format
			data_points.extend(file.read().strip().split('\n'))
		for i in range(len(data_points)):
			data_points[i]=tuple(map(float,data_points[i].split(",")))
		centroids=[]
		for i in request.centroids:
			centroids.append(tuple(map(float,i.split(","))))
		
		mapped_data = {}
		self.n_reducers = request.n_reducers
		for data_point in data_points:
			nearest_centroid = self.find_nearest_centroid(
				data_point, centroids)
			reducer_index = self.partition(nearest_centroid)
			if reducer_index in mapped_data:
				mapped_data[reducer_index].append(data_point)
			else:
				mapped_data[reducer_index] = [data_point]
			
		file_name = []
		
		for centroid, data_points in mapped_data.items():
			x = centroid % self.n_reducers
			file_name.append(f"Mappers/M{1 + int(mapper_name)}/partition{x}.txt")
			with open(f"Mappers/M{1 + int(mapper_name)}/partition{x}.txt", 'w') as f:
				for i in data_points:
					f.write(f"{centroid},{i[0]},{i[1]}\n")
				

		response = mapper_pb2.MapResponse(
			status=mapper_pb2.MapResponse.Status.SUCCESS)
		response.intermediate_file_location.extend(file_name)
		
		return response


class Mapper:
	def __init__(self, port, mapper_name):
		self.address = "localhost"
		self.port = port						
		self.mapper_name = mapper_name

	def serve(self, mapper_name):
		server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
		mapper_pb2_grpc.add_MapperServiceServicer_to_server(
			MapperServiceServicer(mapper_name), server)
		server.add_insecure_port(f"[::]:{port}")
		server.start()
		print("mapperstarted")
		server.wait_for_termination()


if __name__ == "__main__":
	#
	port = sys.argv[1]
	mapper_name = sys.argv[2]
	myServer = Mapper(port, mapper_name)
	myServer.serve(mapper_name)
