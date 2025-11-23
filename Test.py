#User function Template for python3
class Solution:
    # Function to check if two arrays are disjoin

    def areDisjoint(self, a, b):
        #code here
        dic1 = {}
        for e in a:
            dic1[e] =1
        
        for e in b:
            if e in dic1:
                return False

        return True   

if __name__ == "__main__" :
   a = [2, 34, 11, 9, 3]
   b = [2, 1, 3, 5]
   obj = Solution()
   print(obj.areDisjoint(a, b))
   
