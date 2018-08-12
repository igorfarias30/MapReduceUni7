from mrjob.step import MRStep
from mrjob.job import MRJob
import numpy as np

class MRPopularHero(MRJob):

    # line -> hero_id and friends_id
    def mapper(self, _, line):
        hero, friends = line.split()[0], line.split()[1:]
        yield hero, len(friends)

    def reducer(self, hero, friends):
        yield sum(friends), hero

    # passing the key and values to only line
    def mapperAgg(self, amt_friends, hero):
        yield None, (amt_friends, hero)

    def reducerSort(self, _, hero):
        
        # converting hero to a list
        hero = [hero_ for hero_ in hero]
        hero = sorted(hero, reverse = True)

        # return the top of the list sorted and break loop
        for i in range(len(hero)):
            yield hero[i][0], hero[i][1]
            break

    def steps(self):
        return [MRStep(mapper = self.mapper, reducer = self.reducer),
                MRStep(mapper = self.mapperAgg, reducer = self.reducerSort)]

if __name__ == "__main__":
    MRPopularHero.run() 
