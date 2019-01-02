#encoding=utf8
from bitvector import BitVector
import md5
import struct
import math

class BloomFilter2(object):
    def __init__(self, bit_num=None, hash_num=None, item_num=None, bits=None):
        self.bit_num = bit_num
        self.hash_num = hash_num
        self.item_num = item_num if item_num else 0
        self._bits = BitVector(size=bit_num, bits=bits)

    def gen_offsets(self, key):
        h = md5.new()
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        else:
            key = str(key)
        h.update(key)
        a, b = struct.unpack('QQ', h.digest())
        for i in range(self.hash_num):
            yield (a + i * b) % self.bit_num

    def add(self, key):
        ''' Adds a key to this bloom filter. If the key already exists in this
            filter it will return True. Otherwise False. '''
        dup = True
        for i in self.gen_offsets(key): 
            if dup and not self._bits.has_bit(i):
                dup = False
            self._bits.set_bit(i)
        if not dup:
            self.item_num += 1
        return dup

    def __contains__(self, key):
        for i in self.gen_offsets(key):
            if not self._bits.has_bit(i):
                return False
        return True

    def __len__(self):
        return self.item_num

    def bits(self):
        return self._bits.to_binary()

    def bits_size(self):
        return len(self._bits.to_binary())

def create_bloomfilter(capacity=1000, error_rate=0.01,
        bit_num=None, hash_num=None, item_num=None, bits=None):
    if bit_num and hash_num:
        return BloomFilter2(bit_num=bit_num, hash_num=hash_num, item_num=item_num, bits=bits)
    if bit_num or hash_num:
        raise ValueError('should specify both bit_num and hash_num')

    # given M = num_bits, k = num_slices, p = error_rate, n = capacity
    #       k = log2(1/P)
    # solving for m = bits_per_slice
    # n ~= M * ((ln(2) ** 2) / abs(ln(P)))
    # n ~= (k * m) * ((ln(2) ** 2) / abs(ln(P)))
    # m ~= n * abs(ln(P)) / (k * (ln(2) ** 2))
    num_slices = int(math.ceil(math.log(1 / error_rate, 2)))
    # the error_rate constraint assumes a fill rate of 1/2
    # so we double the capacity to simplify the API
    bits_per_slice = int(math.ceil(
        (2 * capacity * abs(math.log(error_rate))) /
        (num_slices * (math.log(2) ** 2))))
    num_bits = num_slices * bits_per_slice
    return BloomFilter2(bit_num=num_bits, hash_num=num_slices)

if __name__ == '__main__':
    e = BloomFilter2(bit_num=100, hash_num=4)
    e.add('one')
    e.add('two')
    e.add('three')
    e.add('four')
    e.add('five')

    assert 'one' in e
    assert 'ten' not in e

    f = create_bloomfilter(capacity=20, error_rate=0.01)
    f.add('three')
    f.add('four')
    f.add('five')
    f.add('six')
    f.add('seven')
    f.add('eight')
    f.add('nine')
    f.add("ten")

    # test check for dupe on add
    assert not f.add('eleven')
    assert f.add('eleven')

    # test membership operations
    assert 'ten' in f
    assert 'one' not in f

    # rebuild it
    bit_num = f.bit_num
    hash_num = f.hash_num
    bits = f.bits()
    f2 = create_bloomfilter(bit_num=bit_num, hash_num=hash_num, bits=bits)
    assert 'ten' in f2
    assert 'one' not in f2
    #assert 'foo' in f2

    print 'done'

