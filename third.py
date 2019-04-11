from mrjob.job import MRJob
from statistics import mean

class MRIris(MRJob):

    def mapper(self, _, line):
        a=line.split(",") #as in a line values are seperated by comma
        yield a[4],float(a[0])
        #4th index stores the species name and 0th the sepal length

    def reducer(self, key, values):
        yield key, mean(values)


if __name__ == '__main__':
    MRIris.run()
