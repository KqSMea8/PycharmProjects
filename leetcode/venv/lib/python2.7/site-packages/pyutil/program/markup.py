#coding=utf8

import re
from pyutil.program.python import unicode_to_str


def get_wrapper_tag(markup):
    '''
    get wrapped xml tag

    >>> get_wrapper_tag(u'<td width="467">fo<img src=""></td>')
    'td'
    >>> get_wrapper_tag('<p>x')
    'p'
    >>> get_wrapper_tag('a<p>x</p>')
    >>> get_wrapper_tag('<p>x</p>b')
    >>> get_wrapper_tag('<p>x</p><b>x</b>')
    '''
    from lxml.html import fragment_fromstring
    markup = unicode_to_str(markup)
    try:
        et = fragment_fromstring(markup, create_parent='div')
    except etree.XMLSyntaxError as e:
        return None
    if len(et) != 1:
        return None
    if et.text or et[0].tail:
        return None
    return et[0].tag

def strip_end_tags(html):
    """ Remove html tags in the begin and end
    >>> html = '<div><p>ab<br />c</p></div>'
    >>> strip_end_tags(html)
    '<p>ab<br />c</p>'
    >>> strip_end_tags(None)
    """

    if not html:
        return html
    text = re.sub(r'^<[^>]+>|<[^>]+>$', '', html)
    return text


if __name__ == '__main__':
    import sys
    sys.path = sys.path[1:]
    import doctest
    doctest.testmod()
