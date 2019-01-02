import unittest
import url

class UrlTest(unittest.TestCase):

    domain_pairs = (
            ('http://www.99fang.com', '99fang.com'),
            ('http://0799.tv/html/classad/201009/20035131214.htm', '0799.tv'),
            ('http://jiaonan.tv/html/classad/201009/28062609848.htm', 'jiaonan.tv'),
            ('http://WWW.jjhouse.CN/sub_hack.asp?id=38735', 'jjhouse.CN'),
            ('http://WWW.JJHOUSE.CN/sub_hack.asp?id=38735', 'JJHOUSE.CN'),
            ('http://www.fulee.com:8080/fulee4/viewt.jsp?mid=22009', 'fulee.com'),
            ('http://61.153.223.3/life/gqxx.asp?id=286042', '61.153.223.3'),
            ('http://61.153.223.3:8080/life/gqxx.asp?id=286042', '61.153.223.3'),
            ('99fang.com', '99fang.com'),
            ('99fang.com:82', '99fang.com'),
            ('http://99fang.com', '99fang.com'),
            ('http://99fang.com.cn', '99fang.com.cn'),
            ('http://99fang.com.cn/haha', '99fang.com.cn'),
            ('http://99fang.com.cn?haha', '99fang.com.cn'),
            ('http://99fang.com?haha', '99fang.com'),
            ('http://99fang.com:8080/?haha', '99fang.com'),
            ('http://www.99fang.com:8080/?url=http://www.hoho.com/', '99fang.com'),
            ('http://www.99fang.com:8080?url=http://www.hoho.com/', '99fang.com'),
            ('http://99fang.com:8080?url=http://www.hoho.com/', '99fang.com'),
            ('http://www.com:8080?url=http://www.hoho.com/', 'www.com'),
            ('http://www.www.com:8080?url=http://www.hoho.com/', 'www.com'),
            ('http://www.www.com:8080?url=http://www.hoho.com:7878/', 'www.com'),
            )
    def test_get_main_domain(self):
        for u, domain in UrlTest.domain_pairs:
            self.assertEqual(url.get_main_domain(u), domain)

if __name__ == '__main__':
    unittest.main()
