class VectorClock:
    def __init__(self, size):
        self.size = size
        self._clock = tuple([0 for _ in range(size)])

    def __eq__(self, other):
        return self._clock == other.value()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __le__(self, other):
        return all([i <= j for i, j in zip(self._clock, other.value())])

    def __gt__(self, other):
        return not self.__le__(other)

    def __lt__(self, other):
        return self.__le__(other) and self.__ne__(other)

    def __ge__(self, other):
        return not self.__lt__(other)

    def increment(self, index):
        new = list(self._clock)
        new[index] += 1
        self._clock = tuple(self._clock)

    def merge(self, other):
        if self.size == other.size:
            self._clock = tuple(map(max, zip(self._clock, other.value())))
        else:
            raise IndexError('Vector clocks are of different lengths')

    def value(self):
        return self._clock

    @staticmethod
    def concurrent(a, b):
        return not a < b and not b < a

    @classmethod
    def fromiterable(cls, arr):
        new = cls(len(arr))
        new._clock = tuple(arr)
        return new

    @classmethod
    def fromvectorclock(cls, vc):
        new = cls(vc.size)
        new.merge(vc)
        return new
