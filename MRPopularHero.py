from mrjob.step import MRStep
from mrjob.job import MRJob
import numpy as np

class MRPopularHero(MRJob):

    # line -> hero and friends
    def mapper(self, _, line):

        hero, friends = line.split()[0], line.split()[1:]
        yield hero, int(len(friends))

    def reducer(self, hero, friends):
        yield sum(friends), hero

    def reducerSort(self, amt, hero):
        hero = list(hero)
        yield amt, hero

    def steps(self):
        return [MRStep(mapper = self.mapper, reducer = self.reducer), MRStep(reducer = self.reducerSort)]

if __name__ == "__main__":
    MRPopularHero.run()
