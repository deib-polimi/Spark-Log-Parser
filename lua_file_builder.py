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
    file = open('./template.lua', 'r')
    targetFile = open(targetDirectory + '/' + name + '.lua', 'w')
    data = file.read()
    data = data \
        .replace('[STAGES]', os.environ['DAGSIM_STAGES']) \
        .replace('[CONTAINERS]', containers) \
        .replace('[USERS]', os.environ['DAGSIM_USERS']) \
        .replace('[TYPE]', os.environ['DAGSIM_UTHINKTIMEDISTR_TYPE']) \
        .replace('[PARAMS]', os.environ['DAGSIM_UTHINKTIMEDISTR_PARAMS']) \
        .replace('[MAXJOBS]', os.environ['DAGSIM_MAXJOBS']) \
        .replace('[COEFF]', os.environ['DAGSIM_CONFINTCOEFF'])
    targetFile.write(data);
    targetFile.close()
    file.close()


def main():
    args = sys.argv
    if len(args) != 4:
        print("Required args: [TARGET_DIRECTORY] [NAME]")
        exit(-1)
    else:
        if os.path.exists(str(args[1])):
            buildLuaFile(str(args[1]), str(args[2]), str(args[3]))
        else:
            print("Inserted directory does not exist")
            exit(-1)


if __name__ == '__main__':
    main()
