import argparse

from csv import DictWriter, DictReader

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_csv', help='CSV to sort columns in', type=argparse.FileType('r'))
    parser.add_argument('output_csv', help='CSV to save results', type=argparse.FileType('w'))

    args = parser.parse_args()

    r = DictReader(args.input_csv, dialect="excel")
    w = DictWriter(args.output_csv, fieldnames=sorted(r.fieldnames), dialect="excel")

    w.writeheader()
    for l in r:
        w.writerow(l)

    args.input_csv.close()
    args.output_csv.close()
