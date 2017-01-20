#!/bin/bash

source ./config.sh

function extractAll {
    #NONZIP_REGEX="application_[0-9]\+_[0-9]\+" Azure version
    NONZIP_REGEX="app-[0-9]\+-[0-9]\+" #Cineca version
    FILENAME=""
    for item in $1/*.zip
    do
        if [[ -f ${item} ]]
        then
            FILENAME=$(echo ${item} | grep -o ${NONZIP_REGEX})
            CONTAINERS=$(getCores $2)
            echo ${FILENAME}","$(extractConfiguaration $2) >> $3/ubertable.csv
            #DIRECTORY=$1/${FILENAME}_dir Azure version
            DIRECTORY=$1/${FILENAME}_csv #Cineca version
            unzip ${item} -d $1
            mkdir ${DIRECTORY}
            python parser.py $1/${FILENAME} 1 ${DIRECTORY}
            buildLuaFile ${DIRECTORY} ${FILENAME} ${CONTAINERS}
        fi
    done
}
function getCores {
    EXECUTORS=$(echo $1 | awk '{split($0,a,"_"); print a[1]}')
    CORES_EXEC=$(echo $1 | awk '{split($0,a,"_"); print a[2]}')
    echo $((${EXECUTORS}*${CORES_EXEC}))
}
function extractConfiguaration {
    EXECUTORS=$(echo $1 | awk '{split($0,a,"_"); print a[1]}')
    CORES_EXEC=$(echo $1 | awk '{split($0,a,"_"); print a[2]}')
    MEM=$(echo $1 | awk '{split($0,a,"_"); print a[3]}')
    DATASIZE=$(echo $1 | awk '{split($0,a,"_"); print a[4]}')
    echo ${EXECUTORS}","$((${EXECUTORS}*${CORES_EXEC}))","${MEM}","${DATASIZE}
}
function dagsimAll {
    #DIR_REGEX="application_[0-9]\+_[0-9]\+_dir" Azure version
    DIR_REGEX="app-[0-9]\+-[0-9]\+_csv" #Cineca version
    VALID_DIRS=$(echo $1/* | grep -o ${DIR_REGEX})
    TMP=""
    if [[ -f $1/simulation.csv ]]
    then
        rm $1/simulation.csv
    fi
    touch $1/simulation.csv
    echo "RUN,SIMRESULT" >> $1/simulation.csv
    PWD_FILTER=$(pwd | grep -o "DagSim")
    if [ ${#PWD_FILTER} -eq 0 ]
    then
        cd ./DagSim/ || exit -1
    fi
    for dir in ${VALID_DIRS}
    do
        FILENAME=$(echo ${dir} | sed s/_csv// ) #Cineca version
        #FILENAME=$(echo ${dir} | sed s/_dir// ) Azure version
        DIR=$1/${dir}
        if [[ -f ${DIR}/out.txt ]]
        then
            rm ${DIR}/out.txt
        fi
        echo "Simulating ${FILENAME}"
        ./dagSim ${DIR}/${FILENAME}".lua" > ${DIR}/out.txt
        TMP=$(awk '{print $3}' ${DIR}/out.txt | sed -n '1 p' )
        echo ${FILENAME}", "${TMP} >> $1/simulation.csv
        echo "Finished"
    done

}
function buildLuaFile {
  export DAGSIM_STAGES=$(python automate.py $1'/jobs_1.csv' $1'/tasks_1.csv' $1'/stages_1.csv' $1)
  python lua_file_builder.py $1 $2 $3
}
function firstLevelDirTraversal {
    # $1 ->Upper directory $2 -> #Depth $3 -> mode
    if [ $2 -eq 2 ]
    then
        secondLevelDirTraversal $1/logs $3 $4 $5
    else
    for item in $1/*
    do
        if [[ -d ${item} ]]
        then
            if [ $2 -eq 0 ]
            then
                firstLevelDirTraversal ${item} $(( $2 + 1)) $3 $(basename ${item}) $5
            else
                firstLevelDirTraversal ${item} $(( $2 + 1)) $3 $4 $5
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
            thirdLevelDirTraversal ${item} $2 $3 $4
        fi
    done
}
function thirdLevelDirTraversal {
    if [ $2 -eq 0 ]
    then
        extractAll $1 $3 $4
    elif [ $2 -eq 1 ]
    then
        dagsimAll $1
    else
        extractAll $1 $3 $4
        dagsimAll $1
        cd ../ || exit -1
    fi

}
function InputCheckAndRun {
    if ! [[ -d $1 ]]
    then
        echo "Error: the directory inserted does not exist"
        exit -1;
    fi
    if [ $2 -gt -1 ] && [ $2 -lt 3 ]
    then
        if [[ -f $1/ubertable.csv ]]
        then
            rm $1/ubertable.csv
        fi
        touch $1/ubertable.csv
        echo "RUN,EXECUTORS,TOTCORES,MEM,DATASIZE" >> $1/ubertable.csv
        firstLevelDirTraversal $1 0 $2 "_" $1
    else
    echo "Error: usage is [ROOT_DIRECTORY] [MODE], where MODE must be in (0|1|2), 0 -> sym only, 1 -> extract only, 2 both"
      exit -1;
    fi
}

if [ $# -ne 2 ]
then
  echo "Error: usage is [ROOT_DIRECTORY] [MODE], where MODE must be in (0|1|2), 0 -> sym only, 1 -> extract only, 2 both"
  exit -1;
fi
InputCheckAndRun $1 $2

