from bitvector import BitVector
import numpy
import msgpack

class BloomFilter(object):
    def __init__(self, n=None, m=None, k=None, bits=None):
        self.n = 0
        self.m = m
        self.k = k
        if bits:
            self.bits = bits
        else:
            self.bits = BitVector( size=m )

    def __contains__(self, key):
        for i in self._gen_hash(key):
            if not self.bits.has_bit(i):
                return False
        return True

    def add(self, key):
        dupe = True
        for i in self._gen_hash(key): 
            if dupe and not self.bits.has_bit(i):
                dupe = False
            self.bits.set_bit(i)
        self.n += 1
        return dupe

    def _gen_hash(self,key):
        numpy.random.seed(abs(hash(key)))
        for i in range(self.k):
            yield numpy.random.randint(0,self.m-1)

    def __len__(self):
        return self.n

    def size(self):
        return len(self.to_binary())

    def to_binary(self):
        d = {'n': self.n,
             'm': self.m,
             'k': self.k,
             'bits': self.bits.to_binary()}
        return msgpack.dumps(d)

    @staticmethod
    def from_binary(binary):
        try:
            d = msgpack.loads(binary)
            return BloomFilter(d.n, d.m, d.k, d.bits)
        except:
            return None

if __name__ == '__main__':
    e = BloomFilter(m=100, k=4)
    e.add('one')
    e.add('two')
    e.add('three')
    e.add('four')
    e.add('five')

    f = BloomFilter(m=100, k=4)
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
    assert 'one' in e
    assert 'ten' not in e
    assert 'one' not in f

