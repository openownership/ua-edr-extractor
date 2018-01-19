"""
Module which covers categorization of founder records to find
only those that has information on beneficiary ownership
"""

import logging
import os.path

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

        basedir = os.path.dirname(__file__)
        with open(os.path.join(basedir, "datasets/beneficiary_ownership_markers.txt")) as fp:
            self.bo_markers = set(map(str.strip, fp))

        with open(os.path.join(basedir, "datasets/beneficiary_ownership_absent_markers.txt")) as fp:
            self.absent_markers = set(map(str.strip, fp))

        with open(os.path.join(basedir, "datasets/beneficiary_owner_is_founder_markers.txt")) as fp:
            self.ref_markers = set(map(str.strip, fp))

    def classify(self, tokenized_record):
        """
        Takes one tokenized founder record and searches for presense of
        BO record markers in it

        :param tokenized_record: Single tokenized founder record
        :type tokenized_record: list of str
        :returns: True if record seems to be about beneficial ownership
        :rtype: bool
        """

        return bool(set(tokenized_record).intersection(self.bo_markers))

    def is_absent(self, tokenized_record):
        """
        Takes one tokenized founder record and searches for presense of
        markers that say that there are no BO ownership for that company

        :param tokenized_record: Single tokenized founder record
        :type tokenized_record: list of str
        :returns: True if record says that there are no information about BO
        :rtype: bool
        """

        return bool(set(tokenized_record).intersection(self.absent_markers))

    def is_reference(self, tokenized_record):
        """
        Takes one tokenized founder record and searches for presense of
        markers that say the founder(s) is the BO of that company

        :param tokenized_record: Single tokenized founder record
        :type tokenized_record: list of str
        :returns: True if record says that the founder is a BO
        :rtype: bool
        """

        return bool(set(tokenized_record).intersection(self.ref_markers))


if __name__ == '__main__':
    benef_classifier = HasBeneficiaryOwnershipRecord()

    assert not benef_classifier.classify(
        "єрмак петро костянтинович , розмір внеску до статутного фонду - 495 . 60 грн .".split(" "))
    assert benef_classifier.classify(
        "кінцевий бенефіціарний власник ( контролер ) - шевчук анатолій миколайович , київська обл ., білоцерківський район , село шкарівка , вулиця незалежності , будинок 33 .".split(" "))

    assert benef_classifier.is_absent(
        '" кінцевий бенефіціарний власник  бюро сертифікейшен холдінг  встановити неможливо " , розмір внеску до статутного фонду  0.00 грн .'.split(" "))

    assert benef_classifier.is_reference(
        "засновник є кінцевим бенефіціарним власником ( контролером ) юридичної особи".split(" "))
