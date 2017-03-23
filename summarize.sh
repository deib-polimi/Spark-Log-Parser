#!/bin/sh

## Copyright 2017 Eugenio Gianniti <eugenio.gianniti@polimi.it>
## Copyright 2017 Giorgio Pea <giorgio.pea@mail.polimi.it>
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
    [ "${SOURCE%${SOURCE#?}}" != / ] && SOURCE="$DIR/$SOURCE"
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

. "$DIR/config.sh"


error_aux ()
{
    line="$1"
    shift
    echo $(basename "$0"): $line: $@ >&2
    exit 1
}
alias error='error_aux $LINENO '

usage ()
{
    echo $(basename "$0") '[-u number]' directory >&2
    echo '    summarize the data in directory' >&2
    echo '    you can provide the number of users with -u, the default is 1' >&2
    exit 2
}

while getopts :u:h opt; do
    case "$opt" in
        u)
            users="$OPTARG"
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


parse_configuration ()
{
    EXPERIMENT=$(echo $1 | tr / '\n' | grep -E '[0-9]+_[0-9]+_[0-9]+G_[0-9]+')
    EXECUTORS=$(echo $EXPERIMENT | awk -F _ '{ print $1 }')
    CORES=$(echo $EXPERIMENT | awk -F _ '{ print $2 }')
    MEMORY=$(echo $EXPERIMENT | awk -F _ '{ print $3 }')
    DATASIZE=$(echo $EXPERIMENT | awk -F _ '{ print $4 }')
    TOTAL_CORES=$(( $EXECUTORS * $CORES ))
}

process_data ()
{
    root="$1"
    find "$root" -type d -name logs -exec dirname {} \; \
        | sort | uniq | while IFS= read -r dir; do

        parse_configuration "$dir"
        python "$DIR/summary/extractor.py" "$APP_REGEX" "$dir" "$USERS" \
               "$DATASIZE" "$TOTAL_CORES"

        application="$(basename "$dir")"
        {
            echo Application class: $application
            cat "$dir/summary.csv"
        } > "$dir/aux.csv"
        mv "$dir/aux.csv" "$dir/summary.csv"
    done
}

isnumber ()
{
    test "$1" && printf '%d' "$1" > /dev/null 2>&1
}


if [ $# -ne 1 ]; then
    usage
fi

if [ ! -d "$1" ]; then
    error the inserted directory does not exist
fi

USERS=${users:-1}
if ! isnumber "$USERS"; then
    error the -u option requires a number as argument
fi

process_data "$1"
