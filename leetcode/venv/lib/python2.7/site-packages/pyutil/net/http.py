import urllib2
import urllib
import cookielib

class Downloader:
  def __init__(self,httpproxy=""):
    cj = cookielib.CookieJar()
    self.opener=None
    if httpproxy!="":
      proxy_support = urllib2.ProxyHandler({"http":httpproxy})
      self.opener = urllib2.build_opener(proxy_support,urllib2.HTTPCookieProcessor(cj))
    else:
      self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
      
  def http_fetch(self,url,dictHeader={},data=None):
    if data==None:
      if isinstance(url , urllib2.Request):
        request=url
      else:
        request = urllib2.Request(url) 
    else:  
      request = urllib2.Request(url,data) 

    if "User-Agent" not in dictHeader:
      dictHeader["User-Agent"]='Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.8.1.6) Gecko/20070725 Firefox/2.0.0.6' 
    for aKey in dictHeader:
      request.add_header(aKey,dictHeader[aKey])   
    fNet = self.opener.open(request)
    return fNet

  def http_download(self,url,dictHeader={},data=None):
    if url.lower().find("http")==-1:
        url="http://"+url;
    fNet=self.http_fetch(url,dictHeader,data)
    fData=fNet.read()
    fNet.close()
    return fData
