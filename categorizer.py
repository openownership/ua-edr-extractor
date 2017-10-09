"""
Module which covers categorization of founder records to find
only those that has information on beneficiary ownership
"""

import logging

logger = logging.getLogger("categorizer")


class HasBeneficiaryOwnershipRecord(object):
    """
    Simple class for categorization using list of text chunks
    """

    def __init__(self):
        """
        Initializes HasBeneficiaryOwnershipRecord class,
        reads/posprocesses the list of markers from text file
        """
        with open("datasets/beneficiary_ownership_markers.txt") as fp:
            self.markers = set(map(str.strip, fp))

    def classify(self, tokenized_record):
        """
        Takes one tokenized founder record and searches for presense of
        any of markers in it

        :param tokenized_record: Single tokenized founder record
        :type tokenized_record: list of str
        :returns: True if record seems to be about beneficial ownership
        :rtype: bool
        """

        return bool(set(tokenized_record).intersection(self.markers))


if __name__ == '__main__':
    benef_classifier = HasBeneficiaryOwnershipRecord()

    assert not benef_classifier.classify(
        "єрмак петро костянтинович , розмір внеску до статутного фонду - 495 . 60 грн .".split(" "))
    assert benef_classifier.classify(
        "кінцевий бенефіціарний власник ( контролер ) - шевчук анатолій миколайович , київська обл ., білоцерківський район , село шкарівка , вулиця незалежності , будинок 33 .".split(" "))
