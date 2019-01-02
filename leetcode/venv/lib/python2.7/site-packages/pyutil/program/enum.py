#coding=utf8

class Enum(object):
    ''' http://www.python.org/dev/peps/pep-0435/ '''

    @classmethod
    def display_name(cls, val):
        return cls._get_name_map().get(val, unicode(val))

    @classmethod
    def name(cls, val):
        return cls._get_val2name().get(val, '')

    @classmethod
    def description(cls, val):
        return cls._get_description_map().get(val, cls.display_name(val))

    @classmethod
    def index(cls, val):
        '''
        choices中的索引(不存在返回-1), 一般用于优先级排序
        '''
        return cls._get_val2index().get(val, -1)

    @classmethod
    def max_by_index(cls, *vals):
        vals = [x for x in vals if x is not None]
        return max(vals, key=lambda x: cls.index(x))

    @classmethod
    def min_by_index(cls, *vals):
        vals = [x for x in vals if x is not None]
        return min(vals, key=lambda x: cls.index(x))

    @classmethod
    def merge_value(cls, *vals):
        '''
        优先用vals中第一个已设置的值

        >>> Enum.merge_value(0, None, 1, 0)
        1
        '''
        for v in vals:
            if not cls.is_unset(v):
                break
        return v

    @classmethod
    def is_unset(cls, v):
        '''
        判断给定值是否表示未设置(不一定是0或None). 子类有必要时需重载此函数
        '''
        return not v

    @classmethod
    def names(cls):
        return map(cls.name, cls.values())

    @classmethod
    def values(cls):
        return [x[0] for x in cls.get_choices()]

    @classmethod
    def items(cls):
        return cls._get_name2val().items()

    @classmethod
    def display_names(cls):
        return [x[1] for x in cls.get_choices()]

    @classmethod
    def use_en_choices(cls):
        cls._use_en_choices = True
        for k in ['_name_map', '_val2index', '_description_map', '_val2name', '_name2val']:
            if hasattr(cls, k):
                delattr(cls, k)

    @classmethod
    def get_choices(cls):
        if getattr(cls, '_use_en_choices', False) and hasattr(cls, 'en_choices'):
            return cls.en_choices
        else:
            return cls.choices

    @classmethod
    def _get_name_map(cls):
        if not hasattr(cls, '_name_map'):
            cls._name_map = dict(cls.get_choices())
        return cls._name_map

    @classmethod
    def _get_description_map(cls):
        if not hasattr(cls, '_description_map'):
            cls._description_map = dict(getattr(cls, 'long_choices', ()))
        return cls._description_map

    @classmethod
    def _get_val2index(cls):
        if not hasattr(cls, '_val2index'):
            cls._val2index = {val: i for i, (val, display_name) in enumerate(cls.get_choices())}
        return cls._val2index

    @classmethod
    def _get_val2name(cls):
        if not hasattr(cls, '_val2name'):
            cls._val2name = {v: k for k, v in cls._get_name2val().items()}
        return cls._val2name

    @classmethod
    def _get_name2val(cls):
        name_map = cls._get_name_map()
        if not hasattr(cls, '_name2val'):
            cls._name2val = {k: v for k, v in cls.__dict__.items()
                    if isinstance(v, (int, basestring)) and v in name_map}
        return cls._name2val

class IntEnum(Enum):
    pass
