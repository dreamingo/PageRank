#! /usr/bin/python

import sys

def convert():
	filename = sys.argv[1]
	new_file = sys.argv[2]
	f = open(filename, 'r')
	
	tmp = f.readline()
	rankIni = 1.0 / (int(tmp))	

	content = f.readlines()
	
	sig = False
	i = 1
	n = open(new_file, 'w')
	for line in content:
		list = line.split(',')
		#if (sig):
		#	n.write('\n')	
		#else:	
		#	sig = True

		n.write(str(i)+'\t'+str(rankIni))
		for item in list:
			n.write('\t'+item)
		i += 1

	f.close()
	n.close()

def main():
	if (len(sys.argv) != 3):
		print 'Usage: convert.py [inputfile] [outputfile]'
	else:
		convert()

if __name__=='__main__':
	main()
