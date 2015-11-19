#!/usr/bin/python

# ---------------------------------------------------------------
# Define and implement a MapReduce job that determines if a graph
# has a Euler tour

# A trail is a walk with no repeated edges
# An Euler tour is a closed trail that visits every edge once

# Graph G is Eulerian if every vertex has an even degree

# Dict or list to hold the eulerGraphs

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRGraphEdgeCount(MRJob):

    def steps(self):
        # Define multiple steps for the job
        return [
            MRStep(mapper=self.mapper_nodes, 
                #combiner=self.combine,  
                reducer=self.reducer),
            MRStep(reducer=self.reduce_residual)
            ]

    # Map nodes 
    def mapper_nodes(self, node, line):
        for node in line.split():
            yield (node, 1)

    # Check if any nodes have an uneven num of connections
    def reducer(self, node, counts):
        # Obs return NONE so output isn't repeated for all nodes
        yield (None, sum(counts)%2)

    # Return true or false 
    def reduce_residual(self,_,values):
        # Return result - is there a Euler path?
        if 1 in values:
            yield ':(','The graph has no Euler tour'
        else:
            yield ':)','The graph has an Euler Tour'


if __name__ == '__main__':
    MRGraphEdgeCount.run()
