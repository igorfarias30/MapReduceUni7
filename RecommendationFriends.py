from mrjob.step import MRStep
from mrjob.job import MRJob

class RecommendationFriends(MRJob):
    
    def mapper_friends(self, _, line):

        hero, friends = line.split()[0], line.split()[1:]

        for friend in friends:
            direct = (friend, -1)
            yield hero, direct
        
        for i in range(len(friends)):
            for j in range(i + 1, len(friends)):

                possiibleFriend1 = (friends[j], hero)
                yield friends[i], possiibleFriend1

                possiibleFriend2 = (friends[i], hero)
                yield friends[j], possiibleFriend2

    def buildOutput(self, mutualFriends):
        
        output = ""
        for key, values in mutualFriends.items():
            if values != []:
                output += key + " (" + str(len(values)) + ": " + str(values) + "), "
        
        return output

    def reducer(self, key, values):
        
        mutualFriends = {}
        values = list(values)

        for toUser, mutualFriend in (values):
            alreadyFriend = (mutualFriend == -1)
            if toUser in mutualFriends.keys():
                if alreadyFriend:
                    mutualFriends[toUser] = []
                elif mutualFriends[toUser] != []:
                    mutualFriends[toUser].append(mutualFriend)
            else:
                if alreadyFriend:
                    mutualFriends[toUser] = []
                    
                else:
                    mutualFriends[toUser] = [mutualFriend]
        
        reducerOutput = self.buildOutput(mutualFriends)
        yield key, reducerOutput
        #yield key, mutualFriends

    def steps(self):
        return [MRStep(mapper = self.mapper_friends, reducer = self.reducer)]
                #MRStep(mapper=self.mapper_one_line, reducer=self.reducerSortbyKey)]

if __name__ == "__main__":
    RecommendationFriends.run()
