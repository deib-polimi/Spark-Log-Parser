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

import re
import subprocess
import sys

from argparse import ArgumentParser
from pathlib import Path


def parse_arguments (argv = None):
    parser = ArgumentParser (description = "run a set of dagSim simulations")
    parser.add_argument ("query", help = "query of interest")
    parser.add_argument ("dataset", help = "dataset size", type = int)
    parser.add_argument ("cases",
                         help = "name of a file containing cases (Make syntax)")
    parser.add_argument ("--results", "-o",
                         help = "results directory passed down to simulate.py")
    parser.add_argument ("--models", "-m",
                         help = "models directory passed down to simulate.py")
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


def run_simulator (args, pairings):
    base_command = ["simulate.py"]

    if args.results:
        base_command.extend (["--results", args.results])

    if args.models:
        base_command.extend (["--models", args.models])

    tuples = sorted (set (p for pairs in pairings.values ()
                          for p in pairs.items ()))
    results = dict ()

    for core, closest in tuples:
        command = base_command[:]

        if core != closest:
            command.extend (["-c", str (closest)])

        command.extend ([args.query, str (core), str (args.dataset)])

        try:
            print (" ".join (command))
            subprocess.run (command, stdin = subprocess.DEVNULL,
                            check = True)
        except subprocess.CalledProcessError as e:
            sys.exit (e.returncode)


if __name__ == "__main__":
    args = parse_arguments ()
    pairings = arrange_cases (args.cases)
    run_simulator (args, pairings)
