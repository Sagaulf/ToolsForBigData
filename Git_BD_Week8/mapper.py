#!/usr/bin/python
import sys

for line in sys.stdin:
	words = line.split()
	for words in words:
		print "%s\t%d" % (words,1)