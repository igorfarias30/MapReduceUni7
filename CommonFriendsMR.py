from CommonFriends import CommonFriends
from mrjob.step import MRStep
from mrjob.job import MRJob

class CommonFriendsMR(MRJob):
    
    common = CommonFriends()

    def mapper(self, key, line):

        values = line.split(',')
        person = values[0]
        friends = values[1].strip().split(' ')

        result = []

        for friend in friends:
            pair = self.build_sorted_key(person, friend)

            yield pair, friends
    
    def build_sorted_key(self, person1, person2):
        if person1 > person2:
            return (person1, person2)
        else:
            return (person2, person1)

    def reducer(self, key, values):
        result = []

        friends = list(values)
        result = friends.pop(0)

        for friend_list in friends:
            result = self.common.intersection(result, friend_list)
        
        yield key, friends



if __name__ == "__main__":
    CommonFriendsMR.run()