import argparse
from csv import DictWriter, DictReader

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input1_csv', help='CSV1 to compare', type=argparse.FileType('r'))
    parser.add_argument('input2_csv', help='CSV2 to compare', type=argparse.FileType('r'))
    parser.add_argument('output_csv', help='CSV to output diff', type=argparse.FileType('w'))

    args = parser.parse_args()

    r1 = DictReader(args.input1_csv, dialect="excel")
    r2 = DictReader(args.input2_csv, dialect="excel")

    w = DictWriter(args.output_csv, fieldnames=["filename"] + sorted(r1.fieldnames), dialect="excel")
    w.writeheader()

    for l1, l2 in zip(r1, r2):
        if l1 != l2:
            l1.update({"filename": args.input1_csv.name})
            l2.update({"filename": args.input2_csv.name})

            w.writerow(l1)
            w.writerow(l2)
            w.writerow({k: "" for k in l1})

    args.input1_csv.close()
    args.input2_csv.close()
    args.output_csv.close()
