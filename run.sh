#!/bin/sh

## Copyright 2017 Eugenio Gianniti <eugenio.gianniti@polimi.it>
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

SOURCE="$0"
while [ -L "$SOURCE" ]; do
    DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [ "${SOURCE:0:1}" != / ] && SOURCE="$DIR/$SOURCE"
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

set -a
. "$DIR"/config.sh
set +a

NONZIP_REGEX='app(lication)?-[0-9]+-[0-9]+'

error_aux ()
{
    echo $0: $1: ${@:2} >&2
    exit 1
}
alias error='error_aux $LINENO '

parse_configuration ()
{
    EXPERIMENT=$(echo $1 | tr / '\n' | grep -E '[0-9]+_[0-9]+_[0-9]+G_[0-9]+')
    EXECUTORS=$(echo $EXPERIMENT | awk -F _ '{ print $1 }')
    CORES=$(echo $EXPERIMENT | awk -F _ '{ print $2 }')
    MEMORY=$(echo $EXPERIMENT | awk -F _ '{ print $3 }')
    DATASIZE=$(echo $EXPERIMENT | awk -F _ '{ print $4 }')
    TOTAL_CORES=$(expr $EXECUTORS \* $CORES)
}

build_lua_file ()
{
    reldir="$1"
    app_id="$2"
    cores="$3"
    absdir="$(pwd)/$reldir"

    STAGES=$(python "$DIR"/automate.py "$absdir/jobs_1.csv" \
                    "$absdir/tasks_1.csv" "$absdir/stages_1.csv" "$absdir")
    DAGSIM_STAGES="$STAGES" python "$DIR"/lua_file_builder.py "$reldir" "$app_id" "$cores"
}

process_data ()
{
    root="$1"

    find -E "$root" -regex '.*'/"$NONZIP_REGEX".zip -execdir unzip '{}' \;

    find -E "$root" -regex '.*'/"$NONZIP_REGEX" | while read -r filename; do
        app_id=$(echo $filename | grep -o -E "$NONZIP_REGEX")

        parse_configuration "$filename"
        echo $app_id, $EXECUTORS, $TOTAL_CORES, $MEMORY, $DATASIZE \
             >> "$root/ubertable.csv"

        dir="$(dirname "$filename")"
        newdir="$dir/${app_id}_csv"
        mkdir -p "$newdir"

        python "$DIR"/parser.py "$dir/$app_id" 1 "$newdir"
        build_lua_file "$newdir" "$app_id" "$TOTAL_CORES"
    done
}

simulate_all ()
{
    root="$1"

    results_file="$root/simulations.csv"
    echo Experiment, Run, Sim Result > "$results_file"

    dir_regex='.*'/"$NONZIP_REGEX"_csv
    find -E "$1" -regex "$dir_regex" | while read -r dir; do
        path="$(echo $dir | sed s/_csv//)"
        filename="$(basename "$path")"

        absdir="$(cd -P "$dir" && pwd)"
        outfile="$absdir/tmp.txt"
        trap "rm -f \'$outfile\'; exit -1" INT TERM

        echo Simulating $filename
        cd "$DAGSIM_DIR"
        ./dagSim "$absdir/$filename.lua" > "$outfile"
        cd -

        result=$(cat "$outfile" | grep '^0' | cut -f 2- | grep '^0' | cut -f 2)
        rm "$outfile"

        parse_configuration "$dir"
        echo $EXPERIMENT, $filename, $result >> "$results_file"
        echo Finished
    done
}

usage ()
{
    echo $0: usage is [ROOT_DIRECTORY] [MODE], where MODE must be in \
         '(0|1|2)', 0 '->' extract only, 1 '->' simulate only, 2 both >&2
    exit 2
}

isnumber ()
{
    test "$1" && printf '%d' "$1" > /dev/null 2>&1
}

if [ $# -ne 2 ]; then
    usage
fi

if [ ! -d "$1" ]; then
    error the inserted directory does not exist
fi

if ! isnumber "$2"; then
    usage
fi

if [ "$2" -gt -1 ] && [ "$2" -lt 3 ]; then
    echo Run, Executors, Total Cores, Memory, Datasize > "$1/ubertable.csv"
    case "$2" in
        0)
            process_data "$1"
            ;;
        1)
            simulate_all "$1"
            ;;
        2)
            process_data "$1"
            simulate_all "$1"
            ;;
    esac
else
    usage
fi
