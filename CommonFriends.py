class CommonFriends(object):

    def intersection(self, user1friends, user2friends):
        if not user1friends:
            return None
        
        if not user2friends:
            return None
        
        if len(user1friends) < len(user2friends):
            return self.intersect(user1friends, user1friends)
        else:
            return self.intersect(user2friends, user1friends)
        
    def intersect(self, small, large):

        result = []

        for n in small:
            if n in large:
                result.append(n)
        
        return result