import argparse
import logging
import json

from reader import EDRReader
from preprocessor import PreProcessor
from categorizer import HasBeneficiaryOwnershipRecord
from tokenize_uk import tokenize_words
from record_parser import HeuristicBasedParser

logger = logging.getLogger("transformer")


class Transformer(object):
    def __init__(self, input_file):
        self.reader = EDRReader(input_file)
        self.preprocessor = PreProcessor(tokenize_words)
        self.beneficiary_categorizer = HasBeneficiaryOwnershipRecord()
        self.parser = HeuristicBasedParser()

    def parse_beneficial_owners(self, founders):
        return list(map(
            self.parser.parse_founders_record,
            founders
        ))

    def transform_company(self, company):
        founders = []

        for founder in self.preprocessor.process_founders(company):
            if self.beneficiary_categorizer.classify(founder):
                founders.append(founder)

        return {
            "Name": company["name"],
            "Company number": company["edrpou"],
            "Beneficial owners": self.parse_beneficial_owners(founders)
        }

    def pump_it(self):
        for company in self.reader.iter_docs():
            yield self.transform_company(company)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_xml', help='UO XML, exported from NAIS, to process')
    parser.add_argument('output_jsonl', help='Path to output document (JSONL)')
    parser.add_argument(
        '--limit', help='Process only first N records', dest="limit", default=0,
        type=int)

    args = parser.parse_args()

    pump = Transformer(args.input_xml)

    result = 0

    with open(args.output_jsonl, "w") as f_out:
        for res in pump.pump_it():
            f_out.write(json.dumps(res, ensure_ascii=False, sort_keys=True) + "\n")

            result += 1
            if args.limit and result >= args.limit:
                break

    logger.info("Successfully pumped {} records".format(res))
