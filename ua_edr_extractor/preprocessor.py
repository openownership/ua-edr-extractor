"""
Module which handles simple pre-processing of founder records

For now it only covers customizable tokenization
(default is wordpunct tokenization from nltk package), lowercasing
and simple cleanup of hanging dashes
"""

import logging
from nltk.tokenize import wordpunct_tokenize


logger = logging.getLogger("preprocessor")


class PreProcessor(object):
    """
    Pre-processor class that does tokenization of founder records in
    the company record
    """

    def __init__(self, tokenizer=wordpunct_tokenize):
        """
        Initializes PreProcessor class and sets the tokenizer

        :param tokenizer: tokenization function (default is NLTK's wordpunct)
        :type tokenizer: callable which accepts str and returns list of str
        """
        self.tokenizer = tokenizer

    def process_founders(self, company):
        """
        Processes single company record. Tokenizes all the foundership records,
        strips hanging dashes, applies lowercasing

        :param company: Company record from EDR registry
        :type company: dict
        :returns: list of tokenized founder records
        :rtype: list of lists of strings
        """

        return [
            list(
                map(
                    lambda x: x.strip("-"),
                    self.tokenizer(l.strip().lower())
                )
            )
            for l in company.get("founders", []) or []
        ]
