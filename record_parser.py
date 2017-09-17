import os
import logging
from enum import IntEnum
from mitie import named_entity_extractor

logger = logging.getLogger("parser")


class AbstractParser(object):
    def parse_founders_record(self, founder):
        raise NotImplementedError()


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
        basedir = os.path.join(os.path.dirname(__file__), "datasets")
        self.names_set = self.load_dicts(
            include=[os.path.join(basedir, "fuge_name_dataset.txt"),
                     os.path.join(basedir, "extra_names.txt")],
            exclude=[os.path.join(basedir, "names_junk.txt")]
        )

        self.countries_set = self.load_dicts(
            include=[os.path.join(basedir, "countries.txt")],
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

    def parse_founders_record(self, founder, include_range=False):
        name_rec = {
            "record": founder,
            "preclassified": list(map(self.preclassify_chunk_as_name, founder))
        }

        name_fingerprint = self.get_fingerprint(name_rec)
        fingerprint_cls = FingerprintClass.classify_fingerprint(name_fingerprint)

        name = None
        name_rng = None
        country = None
        country_rng = None
        address = None
        address_rng = None

        if fingerprint_cls in [
                FingerprintClass.IDEAL, FingerprintClass.COMPLICATED,
                FingerprintClass.ALMOST_IDEAL, FingerprintClass.COMPLICATED_AND_STRANGE]:
            name_rng = self.get_longest_range(name_rec)
            name = " ".join(name_rec["record"][name_rng[0]:name_rng[1]])

        country_rec = {
            "record": founder,
            "preclassified": list(map(self.preclassify_chunk_as_country, founder))
        }

        country_fingerprint = self.get_fingerprint(country_rec)
        if country_fingerprint in [(1,), (2, ), (3, )]:
            country_rng = self.get_longest_range(country_rec)
            country = " ".join(country_rec["record"][country_rng[0]:country_rng[1]])

            if country_rng[1] < len(country_rec["record"]):
                if "розмір" in country_rec["record"]:
                    address_rng = [country_rng[1], country_rec["record"].index("розмір")]
                else:
                    address_rng = [country_rng[1], len(country_rec["record"])]

                # try:
                if country_rec["record"][address_rng[0]] in ",.;- ":
                    address_rng[0] += 1

                if country_rec["record"][address_rng[1] - 1] in ",.;- ":
                    address_rng[1] -= 1
                # except IndexError:
                #     pass

                if address_rng[0] < address_rng[1]:
                    address = " ".join(country_rec["record"][address_rng[0]:address_rng[1]])
                else:
                    address_rng = None

        result = {
            "Name": name,
            "Country of residence": country,
            "Address of residence": address
        }

        if include_range:
            result.update({
                "name_rng": name_rng,
                "country_rng": country_rng,
                "address_rng": address_rng
            })

        return result

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


class MITIEBasedParser(AbstractParser):
    def __init__(self, model="test_data/edr_ner_model_gigaword_embeddings.dat"):
        self.ner = named_entity_extractor(model)

    def parse_founders_record(self, founder):
        entities = self.ner.extract_entities(founder)
        name = None
        if entities:
            names = []
            for rng, _, _ in entities:
                names.append(" ".join(founder[i] for i in rng))

            name = "; ".join(names)

        return {
            "Name": name,
            "Country of residence": None
        }


if __name__ == '__main__':
    ner_parser = MITIEBasedParser()
    ner_advanced_parser = MITIEBasedParser("test_data/edr_ner_model_edr_embeddings.dat")
    ner_advanced_full_name_parser = MITIEBasedParser("expirements/edr_ner_model_combined_embeddings_name_class_full.dat")
    heur_parser = HeuristicBasedParser()
    import csv

    with open("test_data/difference_heur_vs_simple_ner_vs_advanced_ner.csv", "w") as fp_out:
        w = csv.writer(fp_out, dialect="excel")

        with open("test_data/output_founders_alt_tokenization.txt", "r") as fp:
            for i, l in enumerate(fp):
                rec = l.strip().split(" ")
                ner_results = ner_parser.parse_founders_record(rec)
                ner_advanced_results = ner_advanced_parser.parse_founders_record(rec)
                ner_advanced_full_name_results = ner_advanced_full_name_parser.parse_founders_record(rec)
                heur_results = heur_parser.parse_founders_record(rec)

                if len(set([heur_results["Name"], ner_results["Name"], ner_advanced_results["Name"], ner_advanced_full_name_results["Name"]])) > 1:
                    w.writerow([
                        heur_results["Name"],
                        ner_results["Name"],
                        ner_advanced_results["Name"],
                        ner_advanced_full_name_results["Name"], l.strip()
                    ])
