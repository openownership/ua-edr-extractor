import logging
from enum import IntEnum

logger = logging.getLogger("parser")


class AbstractParser(object):
    pass


class FingerprintClass(IntEnum):
    IDEAL = 1  # Full name (3 name chunks)
    ALMOST_IDEAL = 2  # Full name (3 name chunks) + junk
    COMPLICATED = 3   # Full name (more than 3 name chunks)
    COMPLICATED_AND_STRANGE = 4  # Full name (more than 3 name chunks) + junk
    EMPTY = 5  # No name chunks
    ALMOST_EMPTY = 6  # No name chunks longer than 1
    INCOMPLETE = 7  # Longest name chunk has length 2 (like a partially recognized name)
    JUNK = 666  # Not classified

    @classmethod
    def classify_fingerprint(cls, fingerprint):
        if len(fingerprint) == 0:
            return cls.EMPTY

        if max(fingerprint) == 3 and len(fingerprint) == 1:
            return cls.IDEAL

        if max(fingerprint) == 3:
            return cls.ALMOST_IDEAL

        if max(fingerprint) > 3 and len(fingerprint) == 1:
            return cls.COMPLICATED

        if max(fingerprint) > 3:
            return cls.COMPLICATED_AND_STRANGE

        if max(fingerprint) == 1:
            return cls.ALMOST_EMPTY

        if max(fingerprint) == 2:
            return cls.INCOMPLETE

        return cls.JUNK


class HeuristicBasedParser(AbstractParser):
    def __init__(self):
        self.names_set = self.load_dicts(
            include=["datasets/fuge_name_dataset.txt", "datasets/extra_names.txt"],
            exclude=["datasets/names_junk.txt"]
        )

        self.countries_set = self.load_dicts(
            include=["datasets/countries.txt"],
            exclude=[]
        )

    def load_dicts(self, include, exclude):
        imported_set = set()
        junk = set()

        for fname in include:
            with open(fname, "r") as fp:
                imported_set.update(filter(lambda s: len(s) > 1, map(str.strip, fp)))

        logger.info("{} chunks added to the chunks dict".format(len(imported_set)))

        for fname in exclude:
            with open(fname, "r") as fp:
                junk.update(map(str.strip, fp))

        logger.info("{} chunks added to the junk dict".format(len(junk)))
        imported_set -= junk
        logger.info("{} chunks left after filtering junk".format(len(imported_set)))

        return imported_set

    def preclassify_chunk_as_name(self, chunk):
        return chunk in self.names_set

    def preclassify_chunk_as_country(self, chunk):
        return chunk in self.countries_set

    def parse_founders_record(self, founder):
        name_rec = {
            "record": founder,
            "preclassified": list(map(self.preclassify_chunk_as_name, founder))
        }

        name_fingerprint = self.get_fingerprint(name_rec)
        fingerprint_cls = FingerprintClass.classify_fingerprint(name_fingerprint)

        name = None
        country = None

        if fingerprint_cls in [
                FingerprintClass.IDEAL, FingerprintClass.COMPLICATED,
                FingerprintClass.ALMOST_IDEAL, FingerprintClass.COMPLICATED_AND_STRANGE]:
            rng = self.get_longest_range(name_rec)
            name = " ".join(name_rec["record"][rng[0]:rng[1]])

        country_rec = {
            "record": founder,
            "preclassified": list(map(self.preclassify_chunk_as_country, founder))
        }

        country_fingerprint = self.get_fingerprint(country_rec)
        if country_fingerprint in [(1,), (2, ), (3, )]:
            rng = self.get_longest_range(country_rec)
            country = " ".join(name_rec["record"][rng[0]:rng[1]])

        return {
            "Name": name,
            "Country of residence": country
        }

    def get_extracted(self, record):
        res = []
        state = 0
        current_chunk = []

        for i, word_cls in enumerate(record["preclassified"]):
            if word_cls == 1:
                current_chunk.append(record["record"][i])
            elif word_cls != state:
                res.append(current_chunk)
                current_chunk = []

            state = word_cls

        if current_chunk:
            res.append(current_chunk)

        return res

    def get_longest_range(self, record):
        res = []
        state = 0
        current_chunk = []

        for i, word_cls in enumerate(record["preclassified"]):
            if word_cls == 1:
                current_chunk.append(i)
            elif word_cls != state:
                res.append(current_chunk)
                current_chunk = []

            state = word_cls

        if current_chunk:
            res.append(current_chunk)

        if res:
            res = sorted(res, key=lambda x: len(x), reverse=True)
            return res[0][0], res[0][-1] + 1

    def get_fingerprint(self, record):
        return tuple(map(len, self.get_extracted(record)))


if __name__ == '__main__':
    parser = HeuristicBasedParser()

    assert(parser.get_longest_range({
        "preclassified": [1, 1, 0, 1, 1, 1, 0],
        "record": ["a", "b", "c", "d", "e", "f", "g"]
    }) == (3, 6))

    assert(parser.get_longest_range({
        "preclassified": [0, 1, 1, 0, 1],
        "record": ["a", "b", "c", "d", "e"]}) == (1, 3))

    assert(parser.get_fingerprint({
        "preclassified": [0, 1, 1, 0, 1],
        "record": ["a", "b", "c", "d", "e"]}) == (2, 1))
    assert(parser.get_fingerprint({
        "preclassified": [0, 0, 0, 0, 0],
        "record": ["a", "b", "c", "d", "e"]}) == ())
    assert(parser.get_fingerprint({
        "preclassified": [1, 1, 1, 1, 1],
        "record": ["a", "b", "c", "d", "e"]}) == (5,))
    assert(parser.get_fingerprint({
        "preclassified": [1, 1, 0, 1, 1, 1, 0],
        "record": ["a", "b", "c", "d", "e", "f", "g"]}) == (2, 3))
