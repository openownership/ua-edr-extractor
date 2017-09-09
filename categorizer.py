import logging

logger = logging.getLogger("categorizer")


class HasBeneficiaryOwnershipRecord(object):
    def __init__(self):
        with open("datasets/beneficiary_ownership_markers.txt") as fp:
            self.markers = set(map(str.strip, fp))

    def classify(self, tokenized_record):
        return bool(set(tokenized_record).intersection(self.markers))


if __name__ == '__main__':
    benef_classifier = HasBeneficiaryOwnershipRecord()

    assert not benef_classifier.classify(
        "єрмак петро костянтинович , розмір внеску до статутного фонду - 495 . 60 грн .".split(" "))
    assert benef_classifier.classify(
        "кінцевий бенефіціарний власник ( контролер ) - шевчук анатолій миколайович , київська обл ., білоцерківський район , село шкарівка , вулиця незалежності , будинок 33 .".split(" "))
