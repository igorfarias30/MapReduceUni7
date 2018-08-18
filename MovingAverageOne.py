from mrjob.step import MRStep
from mrjob.job import MRJob

class MovingAverageOne(MRJob):
    window_size = 2

    def mapper(self, key, line):
        (name, timestamp, value) = line.split(',')
        yield name, (timestamp, value)
    
    def reducer(self, key, values):
        time_series = list(values)
        time_series.sort()

        sum = 0.0

        for i in range(len(time_series)):
            if i >= self.window_size:
                sum = sum - float(time_series[i - self.window_size][1])
            
            sum = sum + float(time_series[i][1])

            if i < self.window_size:
                moving_average = sum / (i + 1)
            else:
                moving_average = sum / self.window_size
            
            timestamp = time_series[i][0]
            yield key, (timestamp, moving_average)
    

if __name__ == "__main__":
    MovingAverageOne.run() 