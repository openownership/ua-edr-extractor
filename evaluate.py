"""
Simple test bed script that can read config of pipeline from supplied
YAML file, run the tests and save results/show statistic
"""
import yaml
import json
import argparse
import logging
import sys
import csv
from collections import defaultdict

from werkzeug.utils import import_string

logger = logging.getLogger("evaluate")


# TODO: merge with transform.Transformer?
class Pipeline(object):
    def resolve_param(self, v):
        if isinstance(v, str) and v.startswith("!"):
            return import_string(v[1:])

        if isinstance(v, (list, tuple)):
            if v and all(map(lambda x: isinstance(x, (list, tuple)), v)):
                return [self.resolve_param(x) for x in v]

            if v[0].startswith("!"):
                return self.load_class(v)

        return v

    def load_class(self, call_signature):
        class_name = call_signature[0]

        if class_name.startswith("!"):
            class_name = class_name[1:]

        if len(call_signature) == 1:
            return import_string(class_name)()
        else:
            args = {
                k: self.resolve_param(v)
                for k, v in call_signature[1].items()
            }

            return import_string(class_name)(**args)

    def __init__(self, config):
        self.reader = self.load_class(config["reader"])
        self.preprocessor = self.load_class(config["preprocessor"])
        self.beneficiary_categorizer = self.load_class(config["beneficiary_categorizer"])
        self.parser = self.load_class(config["parser"])

    def transform_company(self, company):
        """
        Applies pre-process, categorization and parsing steps to record from the
        company registry

        :param company: One record from the input file
        :type company: dict
        :returns: Results of processing
        :rtype: Dict
        """

        base_rec = {
            "Company name": company["name"],
            "Company number": company["edrpou"],
            "Is beneficial owner": False
        }

        founders = self.preprocessor.process_founders(company)

        if not founders:
            yield base_rec

        for founder in founders:
            rec = base_rec.copy()

            rec["Raw founder record"] = founder

            if self.beneficiary_categorizer.classify(founder):
                rec["Is beneficial owner"] = True
                owner = self.parser.parse_founders_record(founder, include_stats=True)
                rec.update(owner)

            yield rec

    def pump_it(self):
        """
        Iterates over the input file and processes all the records found

        :returns: processed companies
        :rtype: iterator
        """

        for company in self.reader.iter_docs():
            for res in self.transform_company(company):
                for k, v in res.items():
                    if isinstance(v, (list, tuple)):
                        joiner = " " if k == "Raw founder record" else ", "

                        res[k] = joiner.join(map(str, v))
                yield res


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("profile_yaml", help='YAML file with configuration of the pipeline, input and output files')
    parser.add_argument("--source_xml", help='The source data in XML format - takes precedence over the file path set in the profile')
    parser.add_argument("--output_csv", help='The path for the CSV output file - takes precedence over the file path set in the profile')
    parser.add_argument("--show_stats", help='Show also global stats', default=False, action="store_true")

    parser.add_argument(
        '--log', help='Logging level', dest="loglevel", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])

    args = parser.parse_args()

    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)
    logging.basicConfig(level=numeric_level)

    with open(args.profile_yaml, "r") as fp:
        profile = yaml.load(fp.read())

        # TODO: validate the object
        try:
            # Change the reader config if a source file was specified via args
            if args.source_xml:
                reader_file_path_entry = next((x for x in profile["pipeline"]["reader"] if isinstance(x, dict) and x.get("file_path")), None)
                if reader_file_path_entry:
                    reader_file_path_entry["file_path"] = args.source_xml

            pipeline = Pipeline(profile["pipeline"])
            output_csv = args.output_csv or profile["output_csv"]

            result = 0
            bo_result = 0

            accum = []
            stats = defaultdict(int)
            counts = defaultdict(int)
            keys = set()

            for res in pipeline.pump_it():
                if res["Is beneficial owner"]:
                    bo_result += 1

                # Ugly and memory hungry :(
                if not profile.get("export_only_beneficial_owners"):
                    accum.append(res)
                else:
                    if res["Is beneficial owner"]:
                        accum.append(res)

                for k in res:
                    if k.startswith("total_"):
                        stats[k] += res[k]

                        if res[k]:
                            counts[k] += 1

                keys |= set(res.keys())

                result += 1
                if profile.get("limit") and result >= profile["limit"]:
                    break

            with open(output_csv, "w") as f_out:
                w = csv.DictWriter(f_out, fieldnames=sorted(keys), dialect="excel")
                w.writeheader()
                w.writerows(accum)

            logger.info("Successfully pumped {} records".format(result))

            if args.show_stats:
                from prettytable import PrettyTable
                x = PrettyTable()
                stat_keys = sorted([k for k in keys if k.startswith("total_")])
                x.field_names = ["metric"] + stat_keys + ["Total records with BO"] + ["Total records processed"]

                x.add_row(["Found entities"] + [stats[k] for k in stat_keys] + [bo_result, result])
                x.add_row(["Records with at least one entity"] + [counts[k] for k in stat_keys] + [bo_result, result])

                if bo_result > 0:
                    x.add_row(
                        ["Found entities avg"] +
                        ["{:2.3f}%".format(stats[k] / bo_result * 100) for k in stat_keys] +
                        ["100%", "100%"]
                    )
                    x.add_row(
                        ["Records with at least one entity, avg"] +
                        ["{:2.3f}%".format(counts[k] / bo_result * 100) for k in stat_keys] +
                        ["100%", "100%"]
                    )

                print(x)

        except KeyError as e:
            logger.error("Cannot parse profile file: %s" % e)
            exit(1)
