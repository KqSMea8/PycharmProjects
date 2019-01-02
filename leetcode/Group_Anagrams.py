class Solution(object):
    def groupAnagrams(self, strs):
        """
        :type strs: List[str]
        :rtype: List[List[str]]
        """

        if strs == None or len(strs) == 0:
            return [[]]
        strSet = set()

        for s in strs:
            strSet.add(self.sortString(s))

        strMap = dict()
        for s in strs:
            sortStr = self.sortString(s)
            if strMap.get(sortStr):
                strMap[sortStr].append(s)
            else:
                strMap[sortStr] = [s]


        return strMap.values()

    def sortString(self, s):
        l = sorted(list(s))
        newStr = ''
        for s in l:
            newStr += s
        return newStr

if __name__ == '__main__':
    solution = Solution()
    strs = []
    print(solution.groupAnagrams(strs))