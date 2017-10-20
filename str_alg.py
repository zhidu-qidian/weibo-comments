# coding:utf-8

"""
字符串匹配相关的算法
"""
from __future__ import division
import warnings
from types import StringTypes


class KMP(object):
    """
    KMP算法
    """
    pattern = ""
    string = ""
    ptbl = []

    def init_ptbl(self):
        """初始化前缀表"""
        pattern = self.pattern
        p_length = len(pattern)
        for i in range(p_length):
            k = 1
            prefixes = []
            suffixes = []
            while k <= i:
                prefixes.append(''.join(pattern[0:k]))
                suffixes.append(''.join(pattern[k:i + 1]))
                k += 1
            same = [len(v) for v in prefixes if v in suffixes]
            if len(same) > 0:
                lsp = max(same)
            else:
                lsp = 0
            self.ptbl.append(lsp)

        return self.ptbl

    def do_search(self):
        """搜索匹配"""
        pos = []
        ptn = self.pattern
        strng = self.string
        self.init_ptbl()
        print "Looking for pattern %s in string %s: " % (ptn, strng)
        s_length = len(strng)
        l = len(ptn)
        i = 0
        j = 0
        while i < s_length:
            print ("Checking... string[%i](%s) and pattern[%i](%s)") % \
                  (i, strng[i:i + l + 1], j, ptn[j])
            if strng[i:i + l] == ptn[0:l + 1]:
                pos.append(i)
                print "Found match at position %i in string" % i
            if ptn[j] == strng[i]:
                i += 1
                j += 1
                print "Same... next pair"
            elif ptn[j] != strng[i]:
                shift = self.ptbl[j]
                if self.ptbl[j] == 0:
                    shift = 1
                print "Not same... shifting from string[%i] to string[%i]" % (i, i + shift)
                i += shift
                j = 0
            if j == l:
                j = 0
        if len(pos):
            print "Match", pos
        else:
            print "Not Match"


class MaxSub(object):
    """
    最大子串 & 最大子序列
    """

    @staticmethod
    def max_sub_str(str_item_a, str_item_b):
        matrix = [[0 for i in range(len(str_item_b) + 1)] for j in range(len(str_item_a) + 1)]
        mmax = 0
        pos = 0
        for i in range(len(str_item_a)):
            for j in range(len(str_item_b)):
                if str_item_a[i] == str_item_b[j]:
                    matrix[i + 1][j + 1] = matrix[i][j] + 1
                    if matrix[i + 1][j + 1] > mmax:
                        mmax = matrix[i + 1][j + 1]
                        pos = i + 1
        return str_item_a[pos - mmax:pos], mmax

    @staticmethod
    def max_sub_seque(str_item_a, str_item_b):
        matrix = [[0 for x in range(len(str_item_b) + 1)] for y in range(len(str_item_a) + 1)]
        matrix_d = [[None for x in range(len(str_item_b) + 1)] for y in range(len(str_item_a) + 1)]

        for pos_a in range(len(str_item_a)):
            for pos_b in range(len(str_item_b)):
                if str_item_a[pos_a] == str_item_b[pos_b]:
                    matrix[pos_a + 1][pos_b + 1] = matrix[pos_a][pos_b] + 1
                    matrix_d[pos_a + 1][pos_b + 1] = 'ok'
                elif matrix[pos_a + 1][pos_b] > matrix[pos_a][pos_b + 1]:
                    matrix[pos_a + 1][pos_b + 1] = matrix[pos_a + 1][pos_b]
                    matrix_d[pos_a + 1][pos_b + 1] = 'left'
                else:
                    matrix[pos_a + 1][pos_b + 1] = matrix[pos_a][pos_b + 1]
                    matrix_d[pos_a + 1][pos_b + 1] = 'up'
        (pos_a, pos_b) = (len(str_item_a), len(str_item_b))
        s = []
        while matrix[pos_a][pos_b]:
            c = matrix_d[pos_a][pos_b]
            if c == 'ok':
                s.append(str_item_a[pos_a - 1])
                pos_a -= 1
                pos_b -= 1
            if c == 'left':
                pos_b -= 1
            if c == 'up':
                pos_a -= 1
        s.reverse()
        result = ''.join(s)
        return result, len(result)

    @staticmethod
    def max_sub_seque_dis(str_item_a, str_item_b):
        same_seq, length = MaxSub.max_sub_seque(str_item_a, str_item_b)
        dis_a = []
        dis_b = []

        for index, char in enumerate(same_seq):
            if index > length - 2:
                break
            num = str_item_a.index(char)
            str_item_a = str_item_a[num:]
            num_after = str_item_a.index(same_seq[index + 1])
            dis_a.append(num_after)
            num = str_item_b.index(char)
            str_item_b = str_item_b[num:]
            num_after = str_item_b.index(same_seq[index + 1])
            dis_b.append(num_after)
        return same_seq, length, dis_a, dis_b


class NGram(set):
    def __init__(self, items=None, threshold=0.0, warp=1.0, key=None,
                 N=3, pad_len=None, pad_char='$', **kwargs):
        super(NGram, self).__init__()
        if not (0 <= threshold <= 1):
            raise ValueError("threshold out of range 0.0 to 1.0: "
                             + repr(threshold))
        if not (1.0 <= warp <= 3.0):
            raise ValueError(
                "warp out of range 1.0 to 3.0: " + repr(warp))
        if not N >= 1:
            raise ValueError("N out of range (should be N >= 1): " + repr(N))
        if pad_len is None:
            pad_len = N - 1
        if not (0 <= pad_len < N):
            raise ValueError("pad_len out of range: " + repr(pad_len))
        if not len(pad_char) == 1:
            raise ValueError(
                "pad_char is not single character: " + repr(pad_char))
        if key is not None and not callable(key):
            raise ValueError("key is not a function: " + repr(key))
        self.threshold = threshold
        self.warp = warp
        self.N = N
        self._pad_len = pad_len
        self._pad_char = pad_char
        self._padding = pad_char * pad_len  # derive a padding string
        # compatibility shim for 3.1 iconv parameter
        if 'iconv' in kwargs:
            self._key = kwargs.pop('iconv')
            warnings.warn('"iconv" parameter deprecated, use "key" instead.', DeprecationWarning)
        # no longer support 3.1 qconv parameter
        if 'qconv' in kwargs:
            raise ValueError('qconv query conversion parameter unsupported. '
                             'Please process query to a string before calling .search')
        self._key = key
        self._grams = {}
        self.length = {}
        if items:
            self.update(items)

    def __reduce__(self):
        return NGram, (list(self), self.threshold, self.warp, self._key,
                       self.N, self._pad_len, self._pad_char)

    def copy(self, items=None):
        return NGram(items if items is not None else self,
                     self.threshold, self.warp, self._key,
                     self.N, self._pad_len, self._pad_char)

    def key(self, item):
        return self._key(item) if self._key else item

    def pad(self, string):
        return self._padding + string + self._padding

    def _split(self, string):
        for i in range(len(string) - self.N + 1):
            yield string[i:i + self.N]

    def split(self, string):
        return self._split(self.pad(string))

    def ngrams(self, string):
        """Alias for 3.1 compatibility, please set pad_len=0 and use split."""
        warnings.warn('Method ngram deprecated, use method split with pad_len=0 instead.', DeprecationWarning)
        return self._split(string)

    def ngrams_pad(self, string):
        warnings.warn('Method ngrams_pad deprecated, use method split instead.', DeprecationWarning)
        return self.split(string)

    def splititem(self, item):
        return self.split(self.key(item))

    def add(self, item):
        if item not in self:
            super(NGram, self).add(item)
            padded_item = self.pad(self.key(item))
            self.length[item] = len(padded_item)
            for ngram in self._split(padded_item):
                self._grams.setdefault(ngram, {}).setdefault(item, 0)
                self._grams[ngram][item] += 1

    def remove(self, item):
        if item in self:
            super(NGram, self).remove(item)
            del self.length[item]
            for ngram in set(self.splititem(item)):
                del self._grams[ngram][item]

    def pop(self):
        item = super(NGram, self).pop()
        del self.length[item]
        for ngram in set(self.splititem(item)):
            del self._grams[ngram][item]
        return item

    def items_sharing_ngrams(self, query):
        shared = {}
        remaining = {}
        for ngram in self.split(query):
            try:
                for match, count in self._grams[ngram].items():
                    remaining.setdefault(ngram, {}).setdefault(match, count)
                    if remaining[ngram][match] > 0:
                        remaining[ngram][match] -= 1
                        shared.setdefault(match, 0)
                        shared[match] += 1
            except KeyError:
                pass
        return shared

    def searchitem(self, item, threshold=None):
        return self.search(self.key(item), threshold)

    def search(self, query, threshold=None):
        threshold = threshold if threshold is not None else self.threshold
        results = []
        for match, samegrams in self.items_sharing_ngrams(query).items():
            allgrams = (len(self.pad(query))
                        + self.length[match] - (2 * self.N) - samegrams + 2)
            similarity = self.ngram_similarity(samegrams, allgrams, self.warp)
            if similarity >= threshold:
                results.append((match, similarity))
        results.sort(key=lambda x: x[1], reverse=True)
        return results

    def finditem(self, item, threshold=None):
        results = self.searchitem(item, threshold)
        if results:
            return results[0][0]
        else:
            return None

    def find(self, query, threshold=None):
        results = self.search(query, threshold)
        if results:
            return results[0][0]
        else:
            return None

    @staticmethod
    def ngram_similarity(samegrams, allgrams, warp=1.0):
        if abs(warp - 1.0) < 1e-9:
            similarity = float(samegrams) / allgrams
        else:
            diffgrams = float(allgrams - samegrams)
            similarity = ((allgrams ** warp - diffgrams ** warp)
                          / (allgrams ** warp))
        return similarity

    @staticmethod
    def compare(s1, s2, **kwargs):
        if s1 is None or s2 is None:
            if s1 == s2:
                return 1.0
            return 0.0
        try:
            return NGram([s1], **kwargs).search(s2)[0][1]
        except IndexError:
            return 0.0

    def update(self, items):
        for item in items:
            self.add(item)

    def discard(self, item):

        if item in self:
            self.remove(item)

    def clear(self):
        super(NGram, self).clear()
        self._grams = {}
        self.length = {}

    def union(self, *others):
        return self.copy(super(NGram, self).union(*others))

    def difference(self, *others):
        return self.copy(super(NGram, self).difference(*others))

    def difference_update(self, other):
        for item in other:
            self.discard(item)

    def intersection(self, *others):
        return self.copy(super(NGram, self).intersection(*others))

    def intersection_update(self, *others):
        self.difference_update(super(NGram, self).difference(*others))

    def symmetric_difference(self, other):
        return self.copy(super(NGram, self).symmetric_difference(other))

    def symmetric_difference_update(self, other):
        intersection = super(NGram, self).intersection(other)
        self.update(other)  # add items present in other
        self.difference_update(intersection)  # remove items present in both


class Distance(object):
    @staticmethod
    def avg_dis(list_a, list_b):
        avg_a = sum(list_a) / len(list_a)
        avg_b = sum(list_b) / len(list_b)
        return abs(avg_a - avg_b)

    @staticmethod
    def cos_dis(list_a, list_b):
        if not list_a: return 1
        fenzi = sum(map(lambda x: x[0] * x[1], zip(list_a, list_b)))
        fenmu_a = sum(map(lambda x: x * x, list_a))
        fenmu_b = sum(map(lambda x: x * x, list_b))
        return float(fenzi) / (fenmu_a * fenmu_b)


if __name__ == "__main__":
    title = "王新勇"
    content = "王新"
    print NGram.compare(title, content)
