#! /bin/env python

import array

class BitVector(object):
    def __init__(self, size, bits=None):
        self.size = size
        self.item_size = 64
        if bits is None:
            self.bits = array.array('L', [0] * (size / self.item_size + 1))
        else:
            self.bits = array.array('L')
            self.bits.fromstring(bits)

    def set_bit(self, i):
        if i < 0 or i >= self.size:
            return False
        self.bits[i / self.item_size] |= (1 << (i % self.item_size))
        return True

    def has_bit(self, i):
        if i < 0 or i >= self.size:
            return False
        return (self.bits[i / self.item_size] & (1 << (i % self.item_size))) != 0

    def size(self):
        return len(self.bits.tostring())

    def to_binary(self):
        return self.bits.tostring()

