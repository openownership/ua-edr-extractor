import logging

from nltk.tokenize import wordpunct_tokenize


logger = logging.getLogger("preprocessor")


class PreProcessor(object):
    def __init__(self, tokenizer=wordpunct_tokenize):
        self.tokenizer = tokenizer

    def process_founders(self, company):
        return [
            list(
                map(
                    lambda x: x.strip("-"),
                    self.tokenizer(l.strip().lower())
                )
            )
            for l in company.get("founders", []) or []
        ]
