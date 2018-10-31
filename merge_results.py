#! /usr/bin/env python3

## Copyright 2018 Eugenio Gianniti
##
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##     http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

import csv
import re
import sys

from argparse import ArgumentParser
from collections import defaultdict


def parse_arguments (argv = None):
    parser = ArgumentParser (description = "summarize results by case")
    msg = "a file containing the comparison of simulations and real measures"
    parser.add_argument ("comparisons", help = msg)
    msg = "a file containing cases (Make syntax)"
    parser.add_argument ("cases", help = msg)
    return parser.parse_args (argv)


def parse_case (lineno, line):
    if not hasattr (parse_case, "name_re"):
        parse_case.name_re = re.compile ("^\s*(\w+)\s*:")

    match = parse_case.name_re.match (line)

    if match:
        name = match.group (1)
    else:
        raise SyntaxError ("missing name in case file, line {}:\n{}".
                           format (lineno, line))

    _, _, values_str = line.partition (":")
    # By enforcing the sorting we are sure that the minimum distance
    # configuration is always the smallest in case of ties
    values = sorted (int (cores) for cores in values_str.split ())
    return name, values


def cases_from_file (filename):
    with open (filename) as infile:
        lines = infile.readlines ()

    cases = dict (parse_case (idx, line)
                  for idx, line in enumerate (lines, start = 1))
    return cases


def arrange_cases (filename):
    cases = cases_from_file (filename)
    all_cores = cases["all"]
    del cases["all"]
    pairings = dict ()

    for name, values in cases.items ():
        pairs = dict ()

        for core in all_cores:
            closest = min (values, key = lambda c: abs (c - core))
            pairs[core] = closest

        pairings[name] = pairs

    return pairings


def parse_comparisons (filename):
    data = defaultdict (dict)

    with open (filename) as infile:
        reader = csv.DictReader (infile)
        experiment = re.compile (
            "(?P<executors>\d+)_(?P<cpus>\d+)_\d+[mMgG]_(?P<datasize>\d+)")

        for row in reader:
            model = int (row["ModelCores"])
            query = row["Query"]
            match = experiment.fullmatch (row["Experiment"])
            cores = int (match["executors"]) * int (match["cpus"])
            data[query][(cores, model)] = 100 * float (row["Error[1]"])

    return data


def avg (numbers):
    result = 0.
    count = 0

    for n in numbers:
        result += n
        count += 1

    return result / count


def arrange_results (errors, pairings):
    results = list ()

    for case, pairs in pairings.items ():
        for query, partials in errors.items ():
            training = avg (abs (partials[(cores, model)])
                            for cores, model in pairs.items ()
                            if cores == model)
            test = avg (abs (partials[(cores, model)])
                        for cores, model in pairs.items ()
                        if cores != model)
            results.append ({
                "Case": case,
                "Query": query,
                "Training MAPE": training,
                "Test MAPE": test
            })

    return results


def write_table (results):
    fields = ["Case", "Query", "Training MAPE", "Test MAPE"]
    writer = csv.DictWriter (sys.stdout, fields)
    writer.writeheader ()
    writer.writerows (results)


if __name__ == "__main__":
    args = parse_arguments ()
    pairings = arrange_cases (args.cases)
    data = parse_comparisons (args.comparisons)
    results = arrange_results (data, pairings)
    write_table (results)
