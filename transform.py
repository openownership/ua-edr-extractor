"""
Whole idea of that project is to parse pulically available XML feed of ukrainian
registry of companies and extract the information about beneficiary ownership into
machine readable format (JSONL by default).

Unfortunatelly, the task is not as simple as XML to JSON conversion, and here is why:
The general data format of each record from input feed is following:
<RECORD>
    <NAME></NAME>
    <SHORT_NAME></SHORT_NAME>
    <EDRPOU></EDRPOU>
    <ADDRESS></ADDRESS>
    <BOSS></BOSS>
    <KVED></KVED>
    <STAN></STAN>
    <FOUNDERS>
        <FOUNDER></FOUNDER>
        <FOUNDER></FOUNDER>
        <FOUNDER></FOUNDER>
    </FOUNDERS>
</RECORD>

The information that we need is stored in founders array, where each founder record is a raw string
That record might describe the founder of the company OR beneficial owner (or both!). More over,
beneficial ownership record usually carries not only the name of the owner, but also country of registration
and address (which we also need). In some cases, such record might mention more than one owner or refer to
legal entity as an owner. Finally, some records just saying that there are no beneficial owners or that the founder
is also a beneficial owner (without naming him/her directly)

This is a module which conducts the work of all elements of the pipeline:
    Loading (opens XML feed downloaded from NAIS and does CP1251 to UTF-8 conversion)
    Pre-processing (Tokenizes founder records using tokenizer, suitable for ukrainian language)
    Categorization (Picks founder records which has information about beneficial ownership)
    Parsing (Extracts the structured information about name/country/address of beneficial owner)
    Saving (Saves the result in JSONL format (more formats coming soon))
"""

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
    """
    Umbrella class to run the sequence of steps to convert input xml
    file into generator of parsed dicts
    """

    def __init__(self, input_file):
        """
        Initializes Transformer class and instantiates all the pipeline
        step classes

        :param input_file: input XML file, downloaded from NAIS
        :type input_file: str
        """

        self.reader = EDRReader(input_file)
        self.preprocessor = PreProcessor(tokenize_words)
        self.beneficiary_categorizer = HasBeneficiaryOwnershipRecord()
        self.parser = HeuristicBasedParser()

    def parse_beneficial_owners(self, founders):
        """
        Applies categorizer to list of ownership records found in
        a record of a company

        :param founders: array of tokenized records about beneficial ownership
        :type founders: list of lists of tokens
        :returns: Results of parsing: name/country/address of owner (if any)
        :rtype: list of dicts
        """
        return list(map(
            self.parser.parse_founders_record,
            founders
        ))

    def transform_company(self, company):
        """
        Applies pre-process, categorization and parsing steps to record from the
        company registry

        :param company: One record from the input file
        :type company: dict
        :returns: Results of processing
        :rtype: Dict
        """

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
        """
        Iterates over the input file and processes all the records found

        :returns: processed companies
        :rtype: iterator
        """

        for company in self.reader.iter_docs():
            yield self.transform_company(company)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_xml', help='UO XML, exported from NAIS, to process')
    parser.add_argument('output_jsonl', help='Path to output document (JSONL)')
    parser.add_argument(
        '--limit', help='Process only first N records', dest="limit", default=0,
        type=int)

    parser.add_argument(
        '--log', help='Logging level', dest="loglevel", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

    args = parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)
    logging.basicConfig(level=numeric_level)

    pump = Transformer(args.input_xml)

    result = 0

    with open(args.output_jsonl, "w") as f_out:
        for res in pump.pump_it():
            f_out.write(json.dumps(res, ensure_ascii=False, sort_keys=True) + "\n")

            result += 1
            if args.limit and result >= args.limit:
                break

    logger.info("Successfully pumped {} records".format(res))
