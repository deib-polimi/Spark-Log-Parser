#!/bin/bash

function extractDataSize {
    DATASIZE=$(echo $1 | awk '{split($0,a,"_"); print a[4]}')
    echo ${DATASIZE}
}

function extractCores {
    EXECUTORS=$(echo $1 | awk '{split($0,a,"_"); print a[1]}')
    CORES_EXEC=$(echo $1 | awk '{split($0,a,"_"); print a[2]}')
    echo $((${EXECUTORS}*${CORES_EXEC}))
}
function extractUsers {
    USERS=$(echo $1 | awk '{split($0,a,"_"); print a[1]}')
    echo ${USERS}
}
function firstLevelDirTraversal {
    # $1 ->Upper directory $2 -> #Depth $3 -> mode
    if [ $2 -eq 2 ]
    then
        secondLevelDirTraversal $1/logs $3
    else
    for item in $1/*
    do
        if [[ -d ${item} ]]
        then
            if [ $2 -eq 0 ]
            then
                firstLevelDirTraversal ${item} $(( $2 + 1)) $(basename ${item})
            else
                firstLevelDirTraversal ${item} $(( $2 + 1)) $3
            fi
        fi
    done
    fi
}
function secondLevelDirTraversal {

    for item in $1/*
    do
        if [[ -d ${item} ]]
        then
            thirdLevelDirTraversal ${item} $2
        fi
    done
}
function thirdLevelDirTraversal {
    #REGEX="application_[0-9]\+_[0-9]\+_dir" Azure version
    REGEX="app-[0-9]\+-[0-9]\+_csv" #Cineca version
    if [[ -f $1/summary.csv ]]
    then
        rm $1/summary.csv
    fi
    touch $1/summary.csv
    echo "Run,stageId,CompletionTime,nTask,maxTask,avgTask,SHmax,SHavg,Bmax,Bavg,users,dataSize,nContainers" >> $1/summary.csv
    for item in $(echo $1/* | grep -o ${REGEX})
    do
        python extractor.py $1/${item} $1 $(extractUsers $2) $(extractDataSize $2) $(extractCores $2)
    done

}
function InputCheckAndRun {
    if ! [[ -d $1 ]]
    then
        echo "Error: the directory inserted does not exist"
        exit -1;
    fi
    firstLevelDirTraversal $1 0 _

}

if [ $# -ne 2 ]
then
  echo "Error: usage is [ROOT_DIRECTORY] [N_USERS]"
  exit -1;
fi
InputCheckAndRun $1

