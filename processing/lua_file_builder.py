## Copyright 2016 Giorgio Pea <giorgio.pea@mail.polimi.it>
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

import os
import sys


def buildLuaFile(targetDirectory, name, containers):
    scriptdir = os.path.dirname(os.path.realpath(__file__))

    with open(os.path.join(scriptdir, 'template.lua'), 'r') as infile:
        content = infile.read()

    content = content \
              .replace('@@STAGES@@', os.environ['DAGSIM_STAGES']) \
              .replace('@@CONTAINERS@@', containers) \
              .replace('@@USERS@@', os.environ['DAGSIM_USERS']) \
              .replace('@@TYPE@@',
                       os.environ['DAGSIM_UTHINKTIMEDISTR_TYPE']) \
              .replace('@@PARAMS@@',
                       os.environ['DAGSIM_UTHINKTIMEDISTR_PARAMS'])

    outfilename = os.path.join(targetDirectory,
                               '{}.lua.template'.format(name))
    with open(outfilename, 'w') as outfile:
        outfile.write(content)


def main():
    args = sys.argv
    if len(args) != 4:
        print("Required args: [TARGET_DIRECTORY] [NAME]")
        sys.exit(2)
    else:
        if os.path.exists(str(args[1])):
            buildLuaFile(str(args[1]), str(args[2]), str(args[3]))
        else:
            print("Inserted directory does not exist")
            sys.exit(1)


if __name__ == '__main__':
    main()
