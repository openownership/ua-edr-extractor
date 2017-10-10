"""
Module which covers reading of XML file, retrieved from NAIS
"""

import re
import logging

import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError

logger = logging.getLogger("reader")


class EDRReader(object):
    """
    Simple reader class which allows to iterate over XML file
    """

    def __init__(self, file_path):
        """
        Initializes EDRReader class

        :param file_path: full path to the input file
        :type file_path: str
        """

        self.file_path = file_path

    def iter_docs(self):
        """
        Reads input file record by record. Regex magic is required to
        cover records that was incorrectly exported and incomplete, thus
        make whole XML file invalid
        (happens sometime)

        :returns: iterator over company records from registry
        :rtype: collections.Iterable[dict]
        """

        with open(self.file_path, 'r', encoding='cp1251') as fp:
            mapping = {
                'NAME': 'name',
                'SHORT_NAME': 'short_name',
                'EDRPOU': 'edrpou',
                'ADDRESS': 'location',
                'BOSS': 'head',
                'KVED': 'company_profile',
                'STAN': 'status',
                'FOUNDERS': 'founders'
            }

            for i, chunk in enumerate(re.finditer('<RECORD>.*?</RECORD>', fp.read())):
                company = {}
                founders_list = []
                try:
                    etree = ET.fromstring(chunk.group(0))
                except ParseError:
                    self.stderr.write('Cannot parse record #{}, {}'.format(i, chunk))
                    continue

                for el in etree.getchildren():
                    if el.tag == 'EDRPOU' and el.text and el.text.lstrip('0'):
                        company[mapping[el.tag]] = str(int(el.text)).rjust(8, "0")
                    elif el.tag == 'FOUNDERS':
                        for founder in el.getchildren():
                            founders_list.append(founder.text)
                    else:
                        company[mapping[el.tag]] = el.text

                company[mapping['FOUNDERS']] = founders_list

                if i and i % 50000 == 0:
                    logger.info('Read {} companies from XML feed'.format(i))

                yield company


if __name__ == '__main__':
    # Quick test of reader
    r = EDRReader("test_data/15.1-EX_XML_EDR_UO.xml")

    for company in r.iter_docs():
        pass
