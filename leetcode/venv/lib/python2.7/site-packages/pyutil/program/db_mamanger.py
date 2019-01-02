import threading
from pyutil.program.db import DAL

local_dals = None
def get_local_dal(host, name, user, passwd, port=3306):
    global local_dals
    if not local_dals:
        local_dals = threading.local()
    if not hasattr(local_dals, 'dals'):
        local_dals.dals = {}

    key = '%s:%s@%s:%s/%s' % (user, passwd, host, port, name)
    dal = local_dals.dals.get(key)
    if not dal:
        dal = DAL(host=host, port=port, user=user, passwd=passwd, name=name)
        local_dals.dals[key] = dal
    return dal 



if __name__ == '__main__':
    dal = get_local_dal(host='192.168.20.73', 
              port=3306, 
              user='recommend', 
              passwd='recommendC901C2A857297E7', 
              name='recommend')

    sql = """select to_group_id, to_group_publish_time from article_topic_similarity 
             where from_group_id=%s order by similarity desc limit 5""" % (1820351162,)
    dal.execute(sql)
    print local_dals.dals
    result = dal.get_cursor().fetchall()
    print len(result)
    dal.close()
    dal.close()
    print local_dals.dals


    dal.execute(sql)
    print local_dals.dals
    result = dal.get_cursor().fetchall()
    print len(result)
    dal.close()
    print local_dals.dals
