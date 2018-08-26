from mrjob.step import MRStep
from mrjob.job import MRJob
import numpy as np

class Recommendation(MRJob):

    # defining reducer to sort by mean rating
    def steps(self):
        return [MRStep(mapper=self.mapper_products_by_user, reducer=self.reducer_products_by_user),
                MRStep(mapper=self.mapper_strips, reducer=self.reducer_strips)]

    # mapper to strip line
    def mapper_strips(self, userID, items):
        
        for item in items:
            map = {}
            for j in items:
                
                if j not in map:
                    map[j] = 0
                
                map[j] = int(map[j]) + 1
            
            yield item, map
    
    # 
    def reducer_strips(self, item, stripes):
        
        stripes = list(stripes)
        final = {}

        for map in stripes:
            
            for k, v in map.items():    
                if k not in final:
                    final[k] = 0
                
                final[k] = int(final[k]) + int(v)
        
        #yield item, final
        result = sorted(final.items(), key=lambda v: v[1], reverse=True)
        yield item, result[:min(5, len(result))]
    
    def mapper_products_by_user(self, _, line):
        (userID, itemID) = line.split(',')
        yield userID, itemID

    def reducer_products_by_user(self, userID, values):
        items = list(values)
        yield userID, items


if __name__ == "__main__":
    Recommendation.run()
