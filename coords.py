import csv

coords = {}

with open('HICPS_coordinates.csv', mode='r') as infile:
    reader = csv.reader(infile)
    coords = dict((cols[2],cols[3]) for cols in reader)
