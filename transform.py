from reader import EDRReader
from preprocessor import PreProcessor
from categorizer import HasBeneficiaryOwnershipRecord


class Transformer(object):
    def __init__(self, input_file):
        self.reader = EDRReader(input_file)
        self.preprocessor = PreProcessor()
        self.beneficiary_categorizer = HasBeneficiaryOwnershipRecord()

    def transform_company(self, company):
        for founder in self.preprocessor.process_founders(company):
            if self.beneficiary_categorizer.classify(founder):
                yield {
                    "company_name": company["name"],
                    "founder_record": founder
                }

    def pump_it(self):
        for company in self.reader.iter_docs():
            for transformed in self.transform_company(company):
                yield transformed


if __name__ == '__main__':
    pump = Transformer("test_data/15.1-EX_XML_EDR_UO.xml")

    result = 0

    import json
    with open("test_data/output.json", "w") as f_out:
        for res in pump.pump_it():
            f_out.write(json.dumps(res, ensure_ascii=False, sort_keys=True) + "\n")

            result += 1

    print(result)
