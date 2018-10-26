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

from pathlib import Path
from argparse import ArgumentParser

import locale
import re
import shlex
import shutil
import subprocess
import sys


def parse_args (argv = None):
    parser = ArgumentParser (description =
                             "simulate dagSim model with arbitrary number of cores")
    parser.add_argument ("-c", "--model-cores",
                         help = "number of cores to build the model",
                         type = int)
    parser.add_argument ("-m", "--models", help = "base directory with models",
                         default = ".")
    parser.add_argument ("-o", "--results", help = "base directory for results",
                         default = "results")
    parser.add_argument ("query", help = "the query to simulate")
    parser.add_argument ("cores", help = "number of simulated cores",
                         type = int)
    parser.add_argument ("dataset", help = "dataset size", type = int)
    args = parser.parse_args (argv)
    args.model_cores = args.model_cores or args.cores
    return args


def parse_configuration ():
    conf = {}
    script = Path (__file__).resolve ()
    config_file = script.with_name ("config.sh")

    with config_file.open () as infile:
        for line in infile:
            tokens = shlex.split (line)

            if tokens and "#" not in tokens[0]:
                name, _, value = tokens[0].partition ("=")
                conf[name] = value

    return conf


def prepare_model_files (args, options):
    base_dir = Path (args.models)
    experiment = re.compile (
        "(?P<executors>\d+)_(?P<cpus>\d+)_\d+[mMgG]_(?P<datasize>\d+)")

    def is_needed (name):
        result = experiment.match (name)
        cores = int (result["executors"]) * int (result["cpus"])
        datasize = int (result["datasize"])
        return cores == args.model_cores and datasize == args.dataset

    relevant = [model for model in base_dir.iterdir ()
                if is_needed (model.name)]

    if len (relevant) < 1:
        print ("error: no models available", file = sys.stderr)
        sys.exit (1)
    elif len (relevant) > 1:
        print ("error: too many models available", file = sys.stderr)
        sys.exit (1)

    rel_model_dir = relevant[0] / args.query / "empirical"
    abs_model_dir = rel_model_dir.resolve ()
    base_result_dir = Path (args.results).resolve ()
    result_name = "{query}_C{cores}_M{model}_D{dataset}".format (
        query = args.query, cores = args.cores,
        model = args.model_cores, dataset = args.dataset)
    result_dir = base_result_dir / result_name

    try:
        result_dir.mkdir (parents = True)
    except FileExistsError:
        print ("error: directory '{}' already exists".format (result_dir),
               file = sys.stderr)
        sys.exit (1)

    for src in abs_model_dir.glob ("*.txt"):
        dest = result_dir / src.name
        shutil.copy (src, dest)

    lua_file_name = "{query}.lua".format (query = args.query)
    src_lua_file = (abs_model_dir / lua_file_name).with_suffix (".lua.template")
    dest_lua_file = result_dir / lua_file_name
    timing_re = re.compile (
        'solver.fileToArray\s*\("[^"]*/(S\d+)\.txt"\)')
    nodes_re = re.compile ("Nodes\s*=\s*\d+;")

    with src_lua_file.open () as infile, dest_lua_file.open ("w") as outfile:
        for line in infile:
            replacement = 'solver.fileToArray("{path}/\\1.txt")'\
                .format (path = str (result_dir))
            path_line = timing_re.sub (replacement, line)
            node_line = "Nodes = {cores};".format (cores = args.cores)
            subbed_line = nodes_re.sub (node_line, path_line)
            final_line = (subbed_line
                          .replace ("@@MAXJOBS@@", options["DAGSIM_MAXJOBS"])
                          .replace ("@@COEFF@@", options["DAGSIM_CONFINTCOEFF"])
                          .replace ("@@NUMPERC@@", options["DAGSIM_NUMPERC"])
                          .replace ("@@PERCSAMPLES@@", options["DAGSIM_PERCSAMPLES"]))
            outfile.write (final_line)

    return dest_lua_file


def run_simulator (model_file, options):
    results_file = model_file.with_suffix (".dagsim.txt")

    with results_file.open ("w") as outfile:
        try:
            subprocess.run ([options["DAGSIM"], str (model_file)],
                            stdin = subprocess.DEVNULL,
                            encoding = locale.getpreferredencoding (False),
                            stderr = subprocess.PIPE,
                            stdout = outfile, check = True)
        except subprocess.CalledProcessError as e:
            print (e.stderr, file = sys.stderr)
            sys.exit (e.returncode)


def main ():
    args = parse_args ()
    options = parse_configuration ()
    dagsim_model_file = prepare_model_files (args, options)
    run_simulator (dagsim_model_file, options)


if __name__ == "__main__":
    main ()
