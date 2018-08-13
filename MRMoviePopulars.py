from mrjob.step import MRStep
from mrjob.job import MRJob
import numpy as np

class MRMoviePopulars(MRJob):

    # definning mapper method
    def my_mapper(self, _, line):
        (_, word, rating, _) = line.split()
        yield int(word), int(rating)

    # definning reducer method
    def my_reducer(self, word, rating):
        array = [val for val in rating]
        yield np.array(array).mean(), word #now, the key will be the mean rating, and the values a list of movies

    # definning reducer method to second step
    def reducerSort(self, rating, values):
        movies = [val for val in values]
        yield (rating, movies)

    def steps(self):
        return [MRStep(mapper = self.my_mapper,
                        reducer = self.my_reducer), MRStep(reducer = self.reducerSort)]

if __name__ == '__main__':
    MRMoviePopulars.run()
