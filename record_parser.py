"""
This module does all the heavylifting: parsing of the founder records to
extract 3 types of entities: name, country and address

This module contains different classes to do the same job: simple heuristic based
class that uses dictionary, class that relies on MITIE ML library (https://github.com/mit-nlp/MITIE)
and uses pre-trained models and a ensemble class that can be used to combine results obtained from
different models (for example, heuristic based and ML-based)
"""


import os
import logging
from enum import IntEnum
from collections import Counter
from mitie import named_entity_extractor

logger = logging.getLogger("parser")


class AbstractParser(object):
    """
    Abstract class to parse founder records
    """

    def parse_founders_record(self, founder, include_range=False):
        """
        Parse given tokenized founder record and return found entities
        """
        raise NotImplementedError()


class FingerprintClass(IntEnum):
    """
    Utility enumeration class to for heuristic based parser to help
    with name extraction. Each founder record is being analyzed using
    huge dictionary of names and then resulting fingerprint is being classified
    into one of bins below

    Fingerprint is tuple of numbers where each element represents a number of consecutive
    words that has non-Other class. For example "John and Mary Smith" will have fingerprint
    of (1, 2)

    If fingerprint has one 3 consequitive words identified as
    names it's being given an IDEAL class, etc (see comment to each class for details)
    """

    IDEAL = 1  # Full name (longest and only chunk is 3 name chunks)
    ALMOST_IDEAL = 2  # Full name (3 name chunks) + junk
    COMPLICATED = 3   # Full name (more than 3 name chunks)
    COMPLICATED_AND_STRANGE = 4  # Full name (more than 3 name chunks) + junk
    EMPTY = 5  # No name chunks
    ALMOST_EMPTY = 6  # No name chunks longer than 1
    INCOMPLETE = 7  # Longest name chunk has length 2 (like a partially recognized name)
    JUNK = 666  # Not classified

    @classmethod
    def classify_fingerprint(cls, fingerprint):
        """
        Class method to analyze binary vector made of founder record and return
        a class above using simple rules
        """

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
    """
    Heuristic based class to extract names, country and address

    For names huge dataset of names/surnames is used to classify each
    word in the record. Then heuristics are applied to make chains out
    of those words (ukrainian names usually contains 3 words, firstname,
    patronymic, lastname)

    Dictionary is also used to find country

    Simple heuristic is used to parse the addess (which usually follows country)
    """

    def __init__(self):
        """
        Initializes class, loads all required datasets from disk
        """
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
        """
        Helper method. Loads dicts/gazetteers from disk. Can load
        any number of files, to parse and union their content,
        then can load files to exclude from the final result

        :param include: pathnames of dicts that should be included
        :type include: list of str
        :param exclude: pathnames to dicts that should be excluded
        :type exclude: list of str
        :returns: merged set of strings with loaded data
        :rtype: set of str
        """
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
        """
        Checks if the given word is present in the names dict

        :param chunk: word from founder record
        :type chunk: str
        :returns: Result of the check
        :rtype: bool
        """
        return chunk in self.names_set

    def preclassify_chunk_as_country(self, chunk):
        """
        Checks if the given word is present in the countries dict

        :param chunk: word from founder record
        :type chunk: str
        :returns: Result of the check
        :rtype: bool
        """
        return chunk in self.countries_set

    def parse_founders_record(self, founder, include_range=False):
        """
        Parses founder record, tries to extract name, country and address from it
        using smart (actually not) heuristics

        :param founder: tokenized founder record
        :type founder: list of str
        :param include_range: also return range for each entity found
        :type include_range: bool
        :returns: Results of the parsing. All found entities are returned in the
        "Name", "Country of residence", "Address of residence" fields, for example:
        {
            "Name": ["John Smith", "Alex Smith"],
            "Country of residence": ["United States of America"]
            "Address of residence": []
        }
        if include_range is on, then the response will also carry 3 extra keys,
        for example:
        {
            "name_rng": [(0, 2), (4, 6)],
            "country_rng": [(10, 14)],
            "address_rng": []
        }
        :rtype: dict
        """

        name_rec = {
            "record": founder,
            "preclassified": list(map(self.preclassify_chunk_as_name, founder))
        }

        name_fingerprint = self.get_fingerprint(name_rec)
        fingerprint_cls = FingerprintClass.classify_fingerprint(name_fingerprint)

        names = []
        name_rng = []
        countries = []
        country_rng = []
        addresses = []
        address_rng = []

        if fingerprint_cls in [
                FingerprintClass.IDEAL, FingerprintClass.COMPLICATED,
                FingerprintClass.ALMOST_IDEAL, FingerprintClass.COMPLICATED_AND_STRANGE]:
            name_rng = [self.get_longest_range(name_rec)]
            names = [" ".join(name_rec["record"][n_rng[0]:n_rng[1]]) for n_rng in name_rng]

        country_rec = {
            "record": founder,
            "preclassified": list(map(self.preclassify_chunk_as_country, founder))
        }

        country_fingerprint = self.get_fingerprint(country_rec)
        # Cheap-n-dirty heuristic for country and address
        if country_fingerprint in [(1,), (2, ), (3, )]:
            country = self.get_longest_range(country_rec)
            country_rng = [country]

            countries = [" ".join(country_rec["record"][c_rng[0]:c_rng[1]]) for c_rng in country_rng]

            # If found country is not at the end of the string
            if country[1] < len(country_rec["record"]):
                if "розмір" in country_rec["record"]:
                    # if size of the share is specified at the end of the record, we
                    # pick chunks between country and share as an address
                    address_rng = [country[1], country_rec["record"].index("розмір")]
                else:
                    # Otherwise we threat chunk between country and end of the file as
                    # address
                    address_rng = [country[1], len(country_rec["record"])]

                # Dropping some punctuation if any
                if country_rec["record"][address_rng[0]] in ",.;- ":
                    address_rng[0] += 1

                if country_rec["record"][address_rng[1] - 1] in ",.;- ":
                    address_rng[1] -= 1

                # Performing sanity check (ie address range is valid)
                if address_rng[0] < address_rng[1]:
                    addresses = [" ".join(country_rec["record"][address_rng[0]:address_rng[1]])]
                    address_rng = [tuple(address_rng)]
                else:
                    address_rng = []

        result = {
            "Name": names,
            "Country of residence": countries,
            "Address of residence": addresses
        }

        if include_range:
            result.update({
                "name_rng": name_rng,
                "country_rng": country_rng,
                "address_rng": address_rng
            })

        return result

    def get_extracted(self, record):
        """
        Returns the list of consequitive chunks from the record which has
        been assigned non-other class

        :param record: internal representation of pre-parsed record like this
        {
            "record": ["fabulous", "john", "smith"],  # original tokenized record
            "preclassified": [False, True, True], # boolean vector of classes assigned to words above
        }
        :type record: dict
        :returns: list of consequitive chunks with non-zero class. For the example above it'll be
        [["john", "smith"]]
        :rtype: list of lists of str
        """

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
        """
        Returns longest chain of consequitive chunk with non-zero class

        :param record: internal representation of pre-parsed record like this
        {
            "record": ["fabulous", "john", "smith", "and", "george"],
            "preclassified": [False, True, True, False, True],
        }
        :type record: dict
        :returns: longest chain of consequitive chunks with non-zero class. For the example above it'll be
        ["john", "smith"]
        :rtype: list of str
        """

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
        """
        Turns preclassified record into the fingerprint.
        :param record: internal representation of pre-parsed record like this
        {
            "record": ["fabulous", "john", "smith", "and", "george"],
            "preclassified": [False, True, True, False, True],
        }
        :type record: dict
        :returns: lengths of chains found in the original record
        [2, 1]
        :rtype: list of int
        """

        return tuple(map(len, self.get_extracted(record)))


class MITIEBasedParser(AbstractParser):
    """
    ML-based class to parse records using named entity extraction facilities of
    MITIE lib. Requires pre-trained models!
    """

    def __init__(self, model="test_data/edr_ner_model_gigaword_embeddings.dat"):
        """
        Initializes class and MITIE model
        :param model: filepath of MITIE model
        :type model: str
        """

        self.ner = named_entity_extractor(model)

    def parse_founders_record(self, founder, include_range=False):
        """
        Parses founder record, tries to extract name, country and address from it
        using smart ML-based models of MITIE

        :param founder: tokenized founder record
        :type founder: list of str
        :param include_range: also return range for each entity found
        :type include_range: bool
        :returns: Results of the parsing. All found entities are returned in the
        "Name", "Country of residence", "Address of residence" fields, for example:
        {
            "Name": ["John Smith", "Alex Smith"],
            "Country of residence": ["United States of America"]
            "Address of residence": []
        }
        if include_range is on, then the response will also carry 3 extra keys,
        for example:
        {
            "name_rng": [(0, 2), (4, 6)],
            "country_rng": [(10, 14)],
            "address_rng": []
        }
        :rtype: dict
        """

        entities = self.ner.extract_entities(founder)
        names = []
        countries = []
        addresses = []
        name_rng = []
        country_rng = []
        address_rng = []

        if entities:
            names = []
            for rng, tag, _ in entities:
                if tag == "name":
                    name_rng.append((rng.start, rng.stop))
                    names.append(" ".join(founder[i] for i in rng))
                elif tag == "country":
                    country_rng.append((rng.start, rng.stop))
                    countries.append(" ".join(founder[i] for i in rng))
                elif tag == "address":
                    address_rng.append((rng.start, rng.stop))
                    addresses.append(" ".join(founder[i] for i in rng))

        result = {
            "Name": names,
            "Country of residence": countries,
            "Address of residence": addresses,
        }

        if include_range:
            result.update({
                "name_rng": name_rng,
                "country_rng": country_rng,
                "address_rng": address_rng,
            })

        return result


class EnsembleBasedParser(AbstractParser):
    """
    Class that combines results of work of different parsers
    (for example, heuristic based and mitie based) using different voting rules
    # Still WIP
    """

    def __init__(self, voters, cutoff=1):
        """
        Initializes the class with list of voters

        :param voters: ensemble of voters (descendants of AbstractParser)
        :type voters: list of AbstractParser

        :param cutoff: max number of votes to reject the entity
        :type cutoff: int
        """

        self.voters = voters

    def does_intersect(self, rng1, rng2):
        """
        Helper method whic shows if two ranges overlaps

        :param rng1: First range
        :type rng1: tuple/list of ints
        :param rng2: Second range
        :type rng2: tuple/list of ints

        :returns: overlapping or not
        :rtype: bool
        """

        return rng1[0] <= rng2[1] and rng2[0] <= rng1[1]

    def calculate_individual_votes(self, votes):
        """
        Simple heuristic that combines entities found by different
        parsers using voting with cutoff (i.e each entity gets
        into final result if it gets more than cutoff votes)

        :param votes: entities found by different voters
        :type votes: list of dicts
        :returns: combined results
        :rtype: dict
        """

        cntr = Counter(votes)

        res_good = []
        res_bad = []
        for k, v in cntr.most_common():
            if v > self.cutoff:
                res_good.append(k)
            else:
                res_bad.append(k)

        return res_good, res_bad

    def parse_founders_record(self, founder, include_range=False):
        """
        Parses founder record, tries to extract name, country and address from it
        using ensemble of parsers specified during class initialisation.

        Still WIP and very raw, API is a subject to change

        :param founder: tokenized founder record
        :type founder: list of str
        :param include_range: also return range for each entity found
        :type include_range: bool
        :returns: Results of the parsing. All found entities are returned in the
        "Name", "Country of residence", "Address of residence" fields, for example:
        {
            "Name": ["John Smith", "Alex Smith"],
            "Country of residence": ["United States of America"]
            "Address of residence": []
        }
        if include_range is on, then the response will also carry 3 extra keys,
        for example:
        {
            "name_rng": [(0, 2), (4, 6)],
            "country_rng": [(10, 14)],
            "address_rng": []
        }
        :rtype: dict
        """

        combined = {
            "Name": [],
            "Country of residence": [],
            "Address of residence": [],
            "name_rng": [],
            "country_rng": [],
            "address_rng": []
        }

        for voter in self.voters:
            res = voter.parse_founders_record(rec, include_range=True)
            for k in res:
                combined[k] += res[k]

        result = {
            "Name": [],
            "Country of residence": [],
            "Address of residence": [],
            "name_rng": [],
            "country_rng": [],
            "address_rng": [],
            "Name_outliers": [],
            "Country of residence_outliers": [],
            "Address of residence_outliers": [],
            "name_rng_outliers": [],
            "country_rng_outliers": [],
            "address_rng_outliers": []
        }

        for k1, k2 in [
                ("name_rng", "Name"),
                ("country_rng", "Country of residence"),
                ("address_rng", "Address of residence")]:

            good_rngs, bad_rngs = self.calculate_individual_votes(combined[k1])

            result[k1] = good_rngs
            result[k2] = [" ".join(rec[r[0]:r[-1]]) for r in good_rngs]

            result[k1 + "_outliers"] = bad_rngs
            result[k2 + "_outliers"] = [" ".join(rec[r[0]:r[-1]]) for r in bad_rngs]

        if not include_range:
            del result["name_rng"]
            del result["country_rng"]
            del result["address_rng"]

        return result


if __name__ == '__main__':
    import csv

    ner_parser = MITIEBasedParser()
    ner_advanced_parser = MITIEBasedParser(
        "test_data/edr_ner_model_edr_embeddings.dat")
    ner_advanced_full_name_parser = MITIEBasedParser(
        "expirements/edr_ner_model_combined_embeddings_name_class_full.dat")
    heur_parser = HeuristicBasedParser()

    parser = EnsembleBasedParser([
        ner_parser,
        ner_advanced_parser,
        ner_advanced_full_name_parser,
        heur_parser,
        MITIEBasedParser(
            "expirements/edr_ner_model_combined_embeddings_address_class_full.dat"),
        MITIEBasedParser(
            "expirements/edr_ner_model_combined_embeddings_name_class_syntetic.dat"),
        # MITIEBasedParser(
        #     "expirements/edr_ner_model_combined_embeddings_3classes_200k.dat"),
    ])

    with open("test_data/combined.csv", "w") as fp_out:
        w = csv.writer(fp_out, dialect="excel")

        with open("test_data/output_founders_alt_tokenization.txt", "r") as fp:
            for i, l in enumerate(fp):
                rec = l.strip().split(" ")
                combined_res = parser.parse_founders_record(rec)

                if combined_res["Name_outliers"]:
                    w.writerow([
                        "; ".join(combined_res["Name"]),
                        "; ".join(combined_res["Country of residence"]),
                        "; ".join(combined_res["Address of residence"]),
                        "; ".join(combined_res["Name_outliers"]),
                        "; ".join(combined_res["Country of residence_outliers"]),
                        "; ".join(combined_res["Address of residence_outliers"]),
                        l.strip()
                    ])

                if i > 10000:
                    break

    # import csv

    # with open("test_data/difference_heur_vs_simple_ner_vs_advanced_ner.csv", "w") as fp_out:
    #     w = csv.writer(fp_out, dialect="excel")

    #     with open("test_data/output_founders_alt_tokenization.txt", "r") as fp:
    #         for i, l in enumerate(fp):
    #             rec = l.strip().split(" ")
    #             ner_results = ner_parser.parse_founders_record(rec)
    #             ner_advanced_results = ner_advanced_parser.parse_founders_record(rec)
    #             ner_advanced_full_name_results = ner_advanced_full_name_parser.parse_founders_record(rec)
    #             heur_results = heur_parser.parse_founders_record(rec)

    #             if len(set([
    #                     tuple(heur_results["Name"]),
    #                     tuple(ner_results["Name"]),
    #                     tuple(ner_advanced_results["Name"]),
    #                     tuple(ner_advanced_full_name_results["Name"])])) > 1:
    #                 w.writerow([
    #                     heur_results["Name"],
    #                     ner_results["Name"],
    #                     ner_advanced_results["Name"],
    #                     ner_advanced_full_name_results["Name"], l.strip()
    #                 ])
