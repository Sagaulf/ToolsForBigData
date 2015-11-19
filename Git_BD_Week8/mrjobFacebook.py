#!/usr/bin/python

# Construct common friends from list of edges

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRGraphEdgeCount(MRJob):

    def steps(self):
        # Define multiple steps for the job
        return [
            MRStep(mapper=self.map_edges),
            MRStep(mapper=self.mapper, 
                combiner=self.combiner,  
                reducer=self.reducer),
            MRStep(reducer=self.reduce_residual)
            ]

    # Additional mapper to convert from list of edges to same format as slides
    # Map 0 1, 0 2, 0 3 to 0 -> [1 2 3] 
    def map_edges(self, key, line):
    	# Too much data stored?
    	for key in line.split():
    		if numkeys%2 ==1:
    			print previous_key,key
    			yield (previous_key, key)
    			previous_key = key
    			numkeys = numkeys + 1
    		else:
    			numkeys = numkeys + 1

    # Map

    def combiner(self, key, counts):
        print 'combiner'

    # Check if any nodes have an uneven num of connections
    def reducer(self, node, counts):
        print 'reducer'

    # Return true or false 
    def reduce_residual(self,_,values):
    	print 'reducer2'

if __name__ == '__main__':
    MRGraphEdgeCount.run()