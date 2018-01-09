from __future__ import print_function

import os
import csv
import ophion
import argparse

VERVE = 'toprankedret.tsv'
BMEG = 'http://bmeg.io'


def generate_analysis(bmeg, path):
    file = open(path)
    lines = csv.DictReader(file, quotechar='"', delimiter='\t')
    samples = map(
        lambda l: l['gdc_cases.samples.portions.submitter_id'][:16], lines
    )

    Op = ophion.Ophion(bmeg)

    def analyze(gene):
        errors = []
        results = []

        for sample in samples:
            print(sample)
            result = Op.query().has("gid", ['biosample:' + sample]).\
                incoming("tumorSample").incoming("effectOf").\
                outgoing("inGene").has("symbol", [gene]).count().execute()
            if 'error' in result:
                errors.append(sample)
            else:
                results.append(sample + "\t" + str(result['result'][0]))

        return results, errors

    return analyze


def reverse_analysis(bmeg, path):
    file = open(path)
    lines = csv.DictReader(file, quotechar='"', delimiter='\t')
    samples = set(map(
        lambda l: l['gdc_cases.samples.portions.submitter_id'][:16], lines
    ))

    Op = ophion.Ophion(bmeg)

    def analyze(gene):
        results = Op.query().has("gid", ["gene:" + gene]).incoming(
            "inGene").outgoing("effectOf").outgoing("tumorSample").execute()
        result_set = set(map(
            lambda res: res['properties']['barcode'][:16], results['result']
        ))
        intersection = set.intersection(result_set, samples)

        print(len(intersection))

        return intersection, []

    return analyze


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--bmeg", type=str, default=BMEG)
    parser.add_argument("--path", type=str, default=VERVE)
    parser.add_argument("--gene", type=str, default='BRAF')
    parser.add_argument("--out", type=str, default='mutations.tsv')
    args = parser.parse_args()

    analyze = reverse_analysis(args.bmeg, args.path)
    results, errors = analyze(args.gene)
    parts = args.out.split('/')
    parts[-1] = args.gene.lower() + '-' + parts[-1]
    out = '/'.join(parts)
    with open(out, 'w') as handle:
        for line in results:
            handle.write(line + os.linesep)

    print(str(errors))
