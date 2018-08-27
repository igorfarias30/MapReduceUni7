from mrjob.step import MRStep
from mrjob.job import MRJob

class RecommendationFriends(MRJob):

    def mapper_friends(self, _, line):

        hero, friends = line.split()[0], line.split()[1:]

        # mapping friends that have a friendship
        for friend in friends:
            direct = (friend, -1) # -1 means tha hero is a friend of friend[i], for all i in [1, ..., p]
            yield hero, direct

        # creting tuple of possible friends in hero friends
        for i in range(len(friends)):
            for j in range(i + 1, len(friends)):

                possiibleFriend1 = (friends[j], hero)
                yield friends[i], possiibleFriend1

                possiibleFriend2 = (friends[i], hero)
                yield friends[j], possiibleFriend2

    # creating a list of suggest friends and show common friends
    def buildOutput(self, mutualFriends):

        output = ""
        for key, values in mutualFriends.items():
            #values = sorted(values, key = lambda v: len(v), reverse = True)
            if values != []:
                output += key + " (" + str(len(values)) + ": " + str(values) + "), "

        return output

    # reducer to make mutualfriends
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
                if alreadyFriend: mutualFriends[toUser] = []
                else: mutualFriends[toUser] = [mutualFriend]

        reducerOutput = self.buildOutput(mutualFriends)
        yield key, reducerOutput

    def steps(self):
        return [MRStep(mapper = self.mapper_friends, reducer = self.reducer)]
                #MRStep(mapper=self.mapper_one_line, reducer=self.reducerSortbyKey)]

if __name__ == "__main__":
    RecommendationFriends.run()
