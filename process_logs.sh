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


error_aux ()
{
    echo $0: $1: ${@:2} >&2
    exit 1
}
alias error='error_aux $LINENO '

usage ()
{
    echo $(basename "$0") '[-p|-s]' directory >&2
    echo '    process the data in directory' >&2
    echo '    -p to only profile the logs, -s to only simulate' >&2
    exit 2
}

while getopts :psh opt; do
    case "$opt" in
        p)
            PROCESS=yes
            ;;
        s)
            SIMULATE=yes
            ;;
        h)
            usage
            ;;
        \?)
            error unrecognized option -$OPTARG
            ;;
        :)
            error -$OPTARG option requires an argument
            ;;
    esac
done
shift $(expr $OPTIND - 1)

if [ "x$PROCESS" != xyes ] && [ "x$SIMULATE" != xyes ]; then
    PROCESS=yes
    SIMULATE=yes
fi

if [ $# -ne 1 ]; then
    usage
fi

if [ ! -d "$1" ]; then
    error the inserted directory does not exist
fi


parse_configuration ()
{
    EXPERIMENT=$(echo $1 | tr / '\n' | grep -E '[0-9]+_[0-9]+_[0-9]+G_[.0-9]+')
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
    absdir="$(cd -P -- "$reldir" && pwd)"

    STAGES=$(python "$DIR"/processing/automate.py "$absdir/jobs_1.csv" \
                    "$absdir/tasks_1.csv" "$absdir/stages_1.csv" "$absdir")
    DAGSIM_STAGES="$STAGES" python "$DIR"/processing/lua_file_builder.py \
                 "$reldir" "$app_id" "$cores"
}

process_data ()
{
    root="$1"

    find -E "$root" -regex '.*'/"$APP_REGEX".zip -execdir unzip -u -o '{}' \;

    results_file="$1/ubertable.csv"
    echo Run, Executors, Total Cores, Memory, Datasize > "$results_file"

    find -E "$root" -regex '.*'/"$APP_REGEX" | while read -r filename; do
        if echo "$filename" | grep -q /logs/; then
            app_id=$(echo $filename | grep -o -E "$APP_REGEX")

            parse_configuration "$filename"
            echo $app_id, $EXECUTORS, $TOTAL_CORES, $MEMORY, $DATASIZE \
                 >> "$results_file"

            dir="$(dirname "$filename")"
            newdir="$dir/${app_id}_csv"
            mkdir -p "$newdir"

            python "$DIR"/processing/parser.py "$dir/$app_id" 1 "$newdir"
            build_lua_file "$newdir" "$app_id" "$TOTAL_CORES"
        fi
    done
}

simulate_all ()
{
    root="$1"

    results_file="$root/simulations.csv"
    echo Experiment, Run, Sim Avg, Sim Dev, Sim Lower, \
         Sim Upper, Sim Accuracy > "$results_file"

    find -E "$1" -regex '.*'/"$APP_REGEX"_csv | while read -r dir; do
        path="$(echo $dir | sed s/_csv//)"
        filename="$(basename "$path")"

        absdir="$(cd -P "$dir" && pwd)"
        outfile="$absdir/tmp.txt"
        trap "rm -f \'$outfile\'; exit -1" INT TERM

        echo Simulating $filename
        luafile="$absdir/$filename.lua"
        cat "$luafile".template | \
            sed -e "s#@@MAXJOBS@@#$DAGSIM_MAXJOBS#g" \
                -e "s#@@COEFF@@#$DAGSIM_CONFINTCOEFF#g" \
                > "$luafile"
        cd "$DAGSIM_DIR"
        ./dagSim "$luafile" > "$outfile"
        cd - > /dev/null 2>&1

        results_line="$(cat "$outfile" | grep ^0 | cut -f 2- | \
                            grep ^0 | cut -f 2-)"
        rm "$outfile"

        avg="$(echo $results_line | awk '{ print $1 }')"
        dev="$(echo $results_line | awk '{ print $2 }')"
        lower="$(echo $results_line | awk '{ print $3 }')"
        upper="$(echo $results_line | awk '{ print $4 }')"
        accuracy="$(echo $results_line | awk '{ print $NF }')"

        parse_configuration "$dir"
        echo $EXPERIMENT, $filename, ${avg:-error}, ${dev:-error}, \
             ${lower:-error}, ${upper:-error}, \
             ${accuracy:-error} >> "$results_file"
        echo Finished
    done
}


## If you reach this point unscathed, you are sure you can do something.
test "x$PROCESS" = xyes && process_data "$1"
test "x$SIMULATE" = xyes && simulate_all "$1"
