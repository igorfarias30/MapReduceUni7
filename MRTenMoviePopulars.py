from mrjob.step import MRStep
from mrjob.job import MRJob
import numpy as np

class MRTenMoviePopulars(MRJob):

    def mapperLine(self, _, line):
        ( _, movie, rating, _ ) = line.split()
        yield int(movie), (float(rating), 1)

    def reducerLine(self, movie, rating):

        # get the mean rating
        ratins, popula = [], []
        rating = list(rating)

        for i in range(len(rating)):
            ratins.append(rating[i][0])
            popula.append(rating[i][1])

        mean = round(np.array(ratins).mean(), 2)
        soma = sum(popula)

        yield soma, (soma, movie)

    # deffining mapper
    def mapper_2(self, _, line):
        yield None, line

    #line ->[mean, movie]
    def reducer_2(self, _, line):

        line = sorted(line, reverse = True)
        for i in range(10):
            #    "Ordem", ["Soma", "Movie_id"]
            yield i + 1, (line[i][0], line[i][1])

    # defining reducer to sort by mean rating
    def steps(self):
        return [MRStep(mapper = self.mapperLine, reducer = self.reducerLine),
                MRStep(mapper = self.mapper_2, reducer = self.reducer_2)]

if __name__ == "__main__":
    MRTenMoviePopulars.run()
