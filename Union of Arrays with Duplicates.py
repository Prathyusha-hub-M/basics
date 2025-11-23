#https://www.geeksforgeeks.org/problems/union-of-two-arrays3538/1
class Solution:    
    def findUnion(self, a, b):
        # code here
        a=set(a)
        b=set(b)
        result = []
        for e in b:
            result.append(e)
        for e in a:
            if e not in result:
                result.append(e)
                
        return result