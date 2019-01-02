#coding=utf-8

__all__ = ['thrift_attrs', 'thrift_methods', 'thrift_type_convert']

def thrift_attrs(obj_or_cls):
    """Obtain Thrift data type attribute names for an instance or class."""
    return [v[2] for v in obj_or_cls.thrift_spec[1:] if v]

def thrift_methods(ClientOrIface):
    '''
    ClientOrIface - Client or Iface
    '''

    bases = [x for x in ClientOrIface.__bases__ if x != object]
    method_names, sub_method_names = [], []
    if bases:
        sub_method_names = thrift_methods(bases[-1])
    if ClientOrIface.__name__ == 'Iface':
        method_names = [k for k in ClientOrIface.__dict__ if not k.startswith('__')]
    return method_names + sub_method_names

def thrift_type_convert(struct, conv_func):
    do_conv = lambda x: thrift_type_convert(x, conv_func)
    if isinstance(struct, dict):
        return {do_conv(k): do_conv(v) for k, v in struct.items()}
    elif isinstance(struct, (set, list, tuple)):
        return type(struct)(do_conv(x) for x in struct)
    elif hasattr(struct, '__dict__'):
        from copy import copy
        nstruct = copy(struct)
        for k in struct.__dict__.keys():
            setattr(nstruct, k, do_conv(getattr(struct, k)))
        return nstruct
    else:
        return conv_func(struct)
