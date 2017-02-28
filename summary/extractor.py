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

from __future__ import division
from collections import OrderedDict
import os
import csv
import sys
import re


class Extractor:
    def __init__(self, directory, filesDirectory, users, memory, containers, headerFlag):
        self.containers = containers
        self.appStartTime = None
        self.appEndTime = None
        self.minTaskLaunchTime = 0
        self.users = users
        self.memory = memory
        self.jobsCardinality = 0;
        self.maxJobsCardinality = 0;
        self.applicationCsvHeaders = ['run','applicationCompletionTime','ApplicationDeltaBeforeComputing']
        self.jobCsvHeaders = ['jobId', 'JobCompletionTime']
        #This headers must be repetead as many times as the maximum number of stages
        #among all the jobs
        self.stagesCsvHeaders = ['stageId','nTask', 'maxTask', 'avgTask', 'SHmax', 'SHavg', 'Bmax', 'Bavg']
        ##
        self.terminalCsvHeaders = ['users', 'dataSize', 'nContainers']
        self.directory = filesDirectory
        self.directoryName = os.path.basename(filesDirectory)
        self.directoryName = self.directoryName[:len(self.directoryName) - 4]
        self.summaryDirectory = directory
        self.stagesRows = None
        self.stagesTasksList = []
        self.jobsList = []
        self.stagesLen = 0
        self.headerFlag = headerFlag

    """
    Writes the header of the csv file this python script produces
    """
    def writeHeader(self):
        with open(self.summaryDirectory+"/summary.csv", 'w') as f:
            writer = csv.writer(f, delimiter=',', lineterminator='\n')
            targetHeaders = []
            targetHeaders += self.applicationCsvHeaders
            for job in self.jobsList:
                targetHeaders += self.jobCsvHeaders
                for stage in job[2]:
                    targetHeaders += self.stagesCsvHeaders
            targetHeaders += self.terminalCsvHeaders
            writer.writerow(targetHeaders)

    def produceFile(self, finalList):
        with open(self.summaryDirectory+"/summary.csv", 'a') as f:
            writer = csv.writer(f, delimiter=',', lineterminator='\n')
            writer.writerow(finalList)

    def retrieveApplicationTime(self):
        with open(self.directory+"/app_1.csv","r") as f:
            app_rows = csv.DictReader(f)
            for index,row in enumerate(app_rows):
                if index==0:
                    self.appStartTime = int(row["Timestamp"])
                elif index==1:
                    self.appEndTime = int(row["Timestamp"])

    def retrieve_jobs(self, jobs_file):
        with open(jobs_file, "r") as f:
            targetList = []
            jobs_rows = sorted(csv.DictReader(f), key=lambda x: x["Job ID"])
            i = 0
            max_ = 0
            while i < len(jobs_rows):
                completionTime = int(jobs_rows[i + 1]["Completion Time"]) - int(jobs_rows[i]["Submission Time"])
                stages = jobs_rows[i]["Stage IDs"][1:(len(jobs_rows[i]["Stage IDs"]) - 1)].split(", ")
                if(self.stagesLen == 0):
                    self.stagesLen = len(stages)
                targetList.append([jobs_rows[i]["Job ID"], completionTime, stages])
                i += 2
        return targetList

    """
    Compares the number of jobs/stages of this application with
    the ones of all the applications processed before
    """

    def produce_final_list(self):
        finalList = [];
        batch = []
        normalizedMaxStageCardinality = len(self.jobCsvHeaders)+self.stagesLen*len(self.stagesCsvHeaders)
        finalList.append(self.directoryName)
        finalList.append(self.appEndTime - self.appStartTime)
        finalList.append(self.minTaskLaunchTime-self.appStartTime)
        for job in self.jobsList:
            batch.append(job[0])
            batch.append(job[1])
            for stageItem in self.stagesTasksList:
                if stageItem["stageId"] in job[2]:
                    batch = batch + [stageItem["stageId"]] + stageItem.values()[1:]
            finalList += batch
            batch = []
        finalList.append(self.users)
        finalList.append(self.memory)
        finalList.append(self.containers)

        return finalList

    def run(self):
        tasks_file = self.directory + "/tasks_1.csv"
        jobs_file = self.directory + "/jobs_1.csv"

        self.retrieveApplicationTime()
        with open(tasks_file, "r") as f:
            self.stagesRows = self.orderStages(csv.DictReader(f))
            self.minTaskLaunchTime = min(map(lambda x: int(x["Launch Time"]) , self.stagesRows))
        self.jobsList = self.retrieve_jobs(jobs_file)
        self.jobsCardinality = len(self.jobsList)
        if self.headerFlag == True:
            self.writeHeader()
        self.buildstagesTasksList()
        self.produceFile(self.produce_final_list())

    """Checks the existence of the given file path"""

    def fileValidation(self, filename):
        if not (os.path.exists(filename)):
            print("The file " + filename + " does not exists")
            exit(-1)

    """Orders the stages dict by 'Stage Id'"""

    def orderStages(self, stages):
        return sorted(stages, key=lambda x: x["Stage ID"])

    def computeStagesTasksDetails(self, stageId, batch):
        shuffleBatch = []
        normalBatch = []
        bytesBatch = []
        for item in batch:
            normalBatch.append(item[0])
            shuffleBatch.append(item[1])
            bytesBatch.append(item[2])
        maxTask = max(normalBatch)
        maxShuffle = max(shuffleBatch)
        avgTask = reduce(lambda x, y: x + y, normalBatch) / len(normalBatch)
        avgShuffle = reduce(lambda x, y: x + y, shuffleBatch) / len(shuffleBatch)
        maxBytes = max(bytesBatch)
        avgBytes = reduce(lambda x, y: x + y, bytesBatch) / len(bytesBatch)
        targetDict = OrderedDict({})
        targetDict["stageId"] = stageId
        targetDict["nTask"] = len(batch)
        targetDict["maxTask"] = maxTask
        targetDict["avgTask"] = avgTask
        targetDict["SHmax"] = maxShuffle
        targetDict["SHavg"] = avgShuffle
        targetDict["Bmax"] = maxBytes
        targetDict["Bavg"] = avgBytes
        return targetDict

    def buildstagesTasksList(self):
        batch = []
        lastRow = None
        for row in self.stagesRows:
            if lastRow != None and lastRow["Stage ID"] != row["Stage ID"]:
                self.stagesTasksList.append(self.computeStagesTasksDetails(lastRow["Stage ID"], batch))
                batch = []
            if row["Shuffle Write Time"] == "NOVAL":
                batch.append([int(row["Executor Run Time"]), -1, -1])
            else:
                batch.append(
                    [int(row["Executor Run Time"]), int(row["Shuffle Write Time"]), int(row["Shuffle Bytes Written"])])
            lastRow = row
        self.stagesTasksList.append(self.computeStagesTasksDetails(lastRow["Stage ID"], batch))

def directoryScan(regex, directory, users, datasize, totCores):
    headerCond = True
    rx = re.compile(regex)
    for index, fileName in enumerate(os.listdir(directory+"/logs")):
        path = directory+"/logs/"+fileName
        if(os.path.isdir(path) and rx.match(fileName)):
            Extractor(directory,path,users,datasize,totCores,headerCond).run()
            if(headerCond == True):
                headerCond = False;




def main():
    args = sys.argv
    if len(args) != 6:
        print("Required args: [REGEX] [QUERY DIRECTORY] [USERS] [DATASIZE] [TOT CORES]")
        exit(-1)
    else:
        directoryScan(str(args[1]), str(args[2]), str(args[3]), str(args[4]), str(args[5]))


if __name__ == '__main__':
    main()
