
#!/usr/bin/python
#Computational tools for Big Data
#Week 8 
#Exercise 2

from mrjob.job import MRJob
from mrjob.step import MRStep


class MREular(MRJob):
	def steps(self):
		#Define multiple steps for the Job 
		return [
			MRStep(mapper=self.mapper_get_nodes,
				reducer=self.reducer_edges),
			MRStep(reducer=self.reducer_find_if_cyclic)
			]
	def mapper_get_nodes(self, key,line):
		#Mapper
		for node in line.split():
			#print node 
			yield (node, 1)

	def reducer_edges(self,node,counts):
		boolean = sum(counts)%2==0
		yield None, boolean	

	#Reduce the list of booleans 
	def reducer_find_if_cyclic(self,_,values):
		if False in values:
			yield ':(', 'The graph does not have an Euler tour'
		else: 
			yield ':)', 'The graph has an Euler tour '



if __name__ == '__main__':
    MREular.run()