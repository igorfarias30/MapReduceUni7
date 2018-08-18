class SimpleMovingAverage(object):

    def __init__(self, period):
        if period < 1:
            raise NameError("period must be > 0")
        
        self.sum = 0.0
        self.period = period
        self.window = []

    def add_new_number(self, number):
        self.sum = self.sum + number

        self.window.append(number)

        if len(self.window) > self.period:
            self.sum = self.sum - self.window.pop(0)

    def get_moving_average(self):
        if len(self.window) == 0:
            raise NameError("average in undefineded")
        
        return self.sum / len(self.window)

if __name__ == "__main__":
    
    test_data = [10, 18, 20, 30, 24, 33, 27]
    all_window_sizes = [3, 4]

    for size in all_window_sizes:
        sma = SimpleMovingAverage(size)

        print("WindowSize = {}".format(size))

        for number in test_data:
            sma.add_new_number(number)

            print("Next number = {0}, SMA = {1}".format(number, sma.get_moving_average()))
        
        print("---")