class Solution(object):
    def letterCombinations(self, digits):
        """
        :type digits: str
        :rtype: List[str]
        """

        if digits == None or digits == '':
            return []
        num2 = ('a', 'b', 'c')
        num3 = ('d', 'e', 'f')
        num4 = ('g', 'h', 'i')
        num5 = ('j', 'k', 'l')
        num6 = ('m', 'n', 'o')
        num7 = ('p', 'q', 'r', 's')
        num8 = ('t', 'u', 'v')
        num9 = ('w', 'x', 'y', 'z')

        nums = (num2, num3, num4, num5, num6, num7, num8, num9)
        result = ['']
        for s in digits:
            digit = int(s)
            num = nums[digit-2]
            temp = result[:]
            result = []
            for n in num:
                for t in temp:
                    result.append(t + n)

        return result



if __name__ == '__main__':
    solution = Solution()
    digits = '23'
    print(solution.letterCombinations(digits))


