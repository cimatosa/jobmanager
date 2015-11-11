import struct
import numpy as np
import sys
from math import ceil

_spec_types = (bool, type(None))

_SPEC       = 0x00    # True, False, None
_INT_32     = 0x01
_FLOAT      = 0x02
_COMPLEX    = 0x03
_STR        = 0x04
_BYTES      = 0x05
_INT        = 0x06
_TUPLE      = 0x07
_NAMEDTUPLE = 0x08 
_NPARRAY    = 0x09
_LIST       = 0x0a
_GETSTATE   = 0x0b
_DICT       = 0x0c
_INT_NEG    = 0x0d

__max_int32 = +2147483647
__min_int32 = -2147483648


def __int_to_bytes(i):
    m = 0xff
    assert i >= 0
    ba = bytearray()
    while i > 0:
        b = i & m
        ba += bytearray([b])
        i = i >> 8
    return ba[::-1]

def __bytes_to_int(ba):
    i = 0
    for b in ba:
        i = i << 8
        i += b
    return i

BYTES_CLASS = bytearray

if sys.version_info.major > 2: 
    str_to_bytes = lambda s: BYTES_CLASS(s, 'utf8')
    bytes_to_str = lambda b: str(b, 'utf8')
    LONG_TYPE    = int
    int_to_bytes = lambda i: i.to_bytes(ceil(i.bit_length() / 8), 'big')  
    bytes_to_int = lambda ba: int.from_bytes(ba, 'big')
    np_load      = lambda ba: np.loads(ba)
else:

    str_to_bytes = lambda s: s
    bytes_to_str = lambda b: str(b)
    LONG_TYPE    = long
    int_to_bytes = __int_to_bytes
    bytes_to_int = __bytes_to_int
    np_load      = lambda ba: np.loads(str(ba))

class BFLoadError(Exception):
    pass

def _dump_spec(ob):
    if ob == True:
        b = BYTES_CLASS([_SPEC, ord('T')])
    elif ob == False:
        b = BYTES_CLASS([_SPEC, ord('F')])
    elif ob == None:
        b = BYTES_CLASS([_SPEC, ord('N')])
    else:
        raise RuntimeError("object is not of 'special' kind!")        
    return b

def _load_spec(b):
    assert b[0] == _SPEC
    if b[1] == ord('T'):
        return True, 2
    elif b[1] == ord('F'):
        return False, 2
    elif b[1] == ord('N'):
        return None, 2   
    else:
        raise BFLoadError("unknown code for 'special' {}".format(b[1]))
    
def _dump_int_32(ob):
    b = BYTES_CLASS([_INT_32])
    b += struct.pack('>i', ob)
    return b
    
def _load_int_32(b):
    assert b[0] == _INT_32
    i = struct.unpack('>i', b[1:5])[0]
    return i, 5
    
def _dump_int(ob):
    if ob < 0:
        b = BYTES_CLASS([_INT_NEG])
        ob *= -1
    else:
        b = BYTES_CLASS([_INT])
        
    ib = int_to_bytes(ob)
    num_bytes = len(ib)
    b += struct.pack('>I', num_bytes)
    b += ib
    return b

def _load_int(b):
    if b[0] == _INT:
        m = 1
    elif b[0] == _INT_NEG:
        m = -1
    else:
        assert False
    num_bytes = struct.unpack('>I', b[1:5])[0]
    i = m*bytes_to_int(b[5:5+num_bytes])
    return i, num_bytes + 5
    
def _dump_float(ob):
    b = BYTES_CLASS([_FLOAT])
    b += struct.pack('>d', ob)
    return b

def _load_float(b):
    assert b[0] == _FLOAT
    f = struct.unpack('>d', b[1:9])[0]
    return f, 9

def _dump_complex(ob):
    b = BYTES_CLASS([_COMPLEX])
    b += struct.pack('>d', ob.real)
    b += struct.pack('>d', ob.imag)
    return b

def _load_complex(b):
    assert b[0] == _COMPLEX
    re = struct.unpack('>d', b[1:9])[0]
    im = struct.unpack('>d', b[9:17])[0]
    return re + 1j*im, 13
    
def _dump_str(ob):
    b = BYTES_CLASS([_STR])    
    str_bytes = str_to_bytes(ob)
    num_bytes = len(str_bytes)
    b += struct.pack('>I', num_bytes)
    b += str_bytes
    return b

def _load_str(b):
    assert b[0] == _STR
    num_bytes = struct.unpack('>I', b[1:5])[0]
    s = bytes_to_str(b[5:5+num_bytes])
    return s, 5+num_bytes

def _dump_bytes(ob):
    b = BYTES_CLASS([_BYTES])
    num_bytes = len(ob)
    b += struct.pack('>I', num_bytes)
    b += ob
    return b

def _load_bytes(b):
    assert b[0] == _BYTES
    num_bytes = struct.unpack('>I', b[1:5])[0]
    b_ = b[5:5+num_bytes]
    return b_, 5+num_bytes

def _dump_tuple(t):   
    b = BYTES_CLASS([_TUPLE])
    size = len(t)
    b += struct.pack('>I', size)    
    for ti in t:
        b += _dump(ti)
    return b

def _load_tuple(b):    
    assert b[0] == _TUPLE
    size = struct.unpack('>I', b[1:5])[0]
    idx = 5
    t = []
    for i in range(size):
        ob, len_ob = _load(b[idx:])
        t.append(ob)
        idx += len_ob
    return tuple(t), idx

def _dump_namedtuple(t):   
    b = BYTES_CLASS([_NAMEDTUPLE])
    size = len(t)
    b += struct.pack('>I', size)
    b += _dump(t.__class__.__name__)    
    for ti in t:
        b += _dump(ti)
    return b

def _load_namedtuple(b):    
    assert b[0] == _NAMEDTUPLE
    size = struct.unpack('>I', b[1:5])[0]
    class_name, len_ob = _load(b[5:])
    idx = 5 + len_ob
    t = []
    for i in range(size):
        ob, len_ob = _load(b[idx:])
        t.append(ob)
        idx += len_ob
    return (class_name, tuple(t)), idx

def _dump_list(t):
    b = BYTES_CLASS([_LIST])
    size = len(t)
    b += struct.pack('>I', size)    
    for ti in t:
        b += _dump(ti)
    return b

def _load_list(b):    
    assert b[0] == _LIST    
    size = struct.unpack('>I', b[1:5])[0]
    idx = 5
    t = []
    for i in range(size):
        ob, len_ob = _load(b[idx:])
        t.append(ob)
        idx += len_ob
    return t, idx

def _dump_np_array(np_array):
    b = BYTES_CLASS([_NPARRAY])
    nparray_bytes = np.ndarray.dumps(np_array)
    size  = len(nparray_bytes)
    b += struct.pack('>I', size)
    b += nparray_bytes
    return b
    
def _load_np_array(b):
    assert b[0] == _NPARRAY
    size = struct.unpack('>I', b[1:5])[0]
    npa = np_load(b[5: size+5])
    return npa, size+5

def _dump_getstate(ob):
    b = BYTES_CLASS([_GETSTATE]) 
    state = ob.__getstate__()
    obj_type = ob.__class__.__name__
    b += _dump(str(obj_type))
    b += _dump(state)
    
    return b

def _load_getstate(b):
    assert b[0] == _GETSTATE
    obj_type, l_obj_type = _load(b[1:])
    state, l_state = _load(b[l_obj_type+1:])
    return (obj_type, state), l_obj_type+l_state+1

def _dump_dict(ob):
    b = BYTES_CLASS([_DICT]) 
    keys = ob.keys()
    bin_keys = []
    for k in keys:
        bin_keys.append( (dump(k), dump(ob[k])) )
    b += _dump_list(sorted(bin_keys))
    return b

def _load_dict(b):
    assert b[0] == _DICT
    sorted_keys_value, l = _load_list(b[1:])
    res_dict = {}
    for i in range(len(sorted_keys_value)):
        key   = load(sorted_keys_value[i][0])
        value = load(sorted_keys_value[i][1])
        res_dict[key] = value

    return res_dict, l+1

def _dump(ob):
    if isinstance(ob, _spec_types):
        return _dump_spec(ob)
    elif isinstance(ob, (int, LONG_TYPE)):
        if (__min_int32 <= ob) and (ob <= __max_int32):
            return _dump_int_32(ob)
        else:
            return _dump_int(ob)
    elif isinstance(ob, float):
        return _dump_float(ob)
    elif isinstance(ob, complex):
        return _dump_complex(ob)
    elif isinstance(ob, str):
        return _dump_str(ob)
    elif isinstance(ob, (bytearray, bytes)):
        return _dump_bytes(ob)    
    elif isinstance(ob, tuple):
        if hasattr(ob, '_fields'):
            return _dump_namedtuple(ob)
        else:
            return _dump_tuple(ob)    
    elif isinstance(ob, list):
        return _dump_list(ob)
    elif isinstance(ob, np.ndarray):
        return _dump_np_array(ob)
    elif isinstance(ob, dict):
        return _dump_dict(ob)    
    elif hasattr(ob, '__getstate__'):
        return _dump_getstate(ob)    
    else:
        raise RuntimeError("unsupported type for dump '{}'".format(type(ob)))
    
def _load(b):
    identifier = b[0]
    if identifier == _SPEC:
        return _load_spec(b)
    elif identifier == _INT_32:
        return _load_int_32(b)
    elif (identifier == _INT) or (identifier == _INT_NEG):
        return _load_int(b)    
    elif identifier == _FLOAT:
        return _load_float(b)
    elif identifier == _COMPLEX:
        return _load_complex(b)
    elif identifier == _STR:
        return _load_str(b)
    elif identifier == _BYTES:
        return _load_bytes(b)    
    elif identifier == _TUPLE:
        return _load_tuple(b)
    elif identifier == _NAMEDTUPLE:
        return _load_namedtuple(b)    
    elif identifier == _LIST:
        return _load_list(b)    
    elif identifier == _NPARRAY:
        return _load_np_array(b)
    elif identifier == _DICT:
        return _load_dict(b)    
    elif identifier == _GETSTATE:
        return _load_getstate(b)    
    else:
        if isinstance(identifier, str):
            identifier = ord(identifier)
        raise BFLoadError("unknown identifier '{}'".format(hex(identifier)))
    
def dump(ob):
    """
        returns the binary footprint of the object 'ob' as BYTES_CLASS
    """
    return _dump(ob)

def load(b):
    """
        reconstruct the object from the binary footprint given an BYTES_CLASS 'ba'
    """
    return _load(b)[0]
        