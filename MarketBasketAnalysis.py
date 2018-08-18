from mrjob.step import MRStep
from mrjob.job import MRJob

class MarketBasketAnalysis(MRJob):

    # function return a list of combinations    
    def generate_combinations(self, items):
        
        result = []
        items.sort()

        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                for k in range(j + 1, len(items)):
                    a = items[i]
                    b = items[j]
                    c = items[k]
                    result.append((a, b, c))
        
        return result

    # creating mapper
    def mapper(self, key, line):
        items = line.split(',')

        combinations = self.generate_combinations(items)

        for combination in combinations:
            yield combination, 1
    
    # combine values
    def combiner(self, key, values):
        yield key, sum(values)
    
    # sum vales in the key
    def reducer(self, key, values):
        yield key, sum(values)
    
    # passing the result in reducer to one line
    def mapper_2(self, key, values):
        yield None, (key, values)

    # values is a list with [3-key, freq]
    def reducer_2(self, _, values):
        
        values = list(values)
        values = sorted(values)
        total = 0.0

        for pair in values:
            total = total + pair[1]

        for pair in values:
            yield pair[0], str(round(pair[1]/total * 100, 2)) + " %"

    # defining steps
    def steps(self):
        return [MRStep(mapper = self.mapper, reducer = self.reducer), 
                MRStep(mapper = self.mapper_2, reducer = self.reducer_2)]

if __name__ == "__main__":
    MarketBasketAnalysis.run()