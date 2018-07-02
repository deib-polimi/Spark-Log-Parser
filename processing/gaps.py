#! /usr/bin/env python3

## Copyright 2018 Eugenio Gianniti <eugenio.gianniti@polimi.it>
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
import sys

from itertools import tee
from operator import itemgetter


# From the recipe in https://docs.python.org/3.7/library/itertools.html#recipes
def pairwise (iterable):
    a, b = tee (iterable)
    next (b, None)
    return zip (a, b)


def parseInput (filename):
    with open (filename, "r") as infile:
        reader = csv.DictReader (infile)
        fields = reader.fieldnames
        label = next (f for f in fields if "ID" in f)
        data = [
            {"ID": row[label],
             "Submission Time": row["Submission Time"],
             "Completion Time": row["Completion Time"]}
            for row in reader
        ]

    return data


def processData (data):
    sortedData = sorted (data, key = itemgetter ("Submission Time"))
    possibleGaps = (
        {"Previous ID": first["ID"],
         "Next ID": second["ID"],
         "Span": int (second["Submission Time"]) - int (first["Completion Time"])}
        for first, second in pairwise (sortedData)
    )
    gaps = [row for row in possibleGaps if row["Span"] > 0]
    headers = ["Previous ID", "Next ID", "Span"]
    return headers, gaps


def produceCSV (headers, gaps):
    writer = csv.DictWriter (sys.stdout, headers)
    writer.writeheader ()
    writer.writerows (gaps)


def main ():
    if len (sys.argv) == 2:
        filename = str (sys.argv[1])
        data = parseInput (filename)
        headers, gaps = processData (data)
        produceCSV (headers, gaps)
    else:
        print ("Required args: [CSV_FILE]", file = sys.stderr)
        sys.exit (2)


if __name__ == "__main__":
    main ()
