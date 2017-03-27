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

from __future__ import division
from collections import OrderedDict
from operator import itemgetter

import csv
import os
import re
import sys


class Extractor:
    def __init__(self, directory, filesDirectory, users, memory, containers, headerFlag):
        self.containers = containers
        self.appStartTime = None
        self.appEndTime = None
        self.availableIDs = None
        self.minTaskLaunchTime = 0
        self.users = users
        self.memory = memory
        self.jobsCardinality = 0;
        self.maxJobsCardinality = 0;
        self.directory = filesDirectory
        self.directoryName = os.path.basename(filesDirectory)
        self.directoryName = self.directoryName[:-4]
        self.summaryDirectory = directory
        self.stagesRows = None
        self.stagesTasksDict = {}
        self.jobsList = []
        self.stagesLen = 0
        self.headerFlag = headerFlag


    def writeHeader(self):
        """Write the header of the CSV file this python script produces."""
        applicationCsvHeaders = ['run', 'applicationCompletionTime',
                                 'applicationDeltaBeforeComputing']
        jobCsvHeaders = ['jobCompletionTime_J{job}']
        stagesCsvHeaders = ['nTask_J{job}_S{stage}', 'maxTask_J{job}_S{stage}',
                            'avgTask_J{job}_S{stage}']
        shuffleCsvHeaders = ['SHmax_J{job}_S{stage}', 'SHavg_J{job}_S{stage}',
                             'Bmax_J{job}_S{stage}', 'Bavg_J{job}_S{stage}']
        terminalCsvHeaders = ['users', 'dataSize', 'nContainers']

        targetHeaders = []
        targetHeaders += applicationCsvHeaders

        for job in self.jobsList:
            jobID = job[0]
            targetHeaders += [h.format(job = jobID) for h in jobCsvHeaders]

            for stage in job[2]:
                targetHeaders += [h.format(job = jobID, stage = stage)
                                  for h in stagesCsvHeaders]

                if "SHmax" in self.stagesTasksDict[stage]:
                    targetHeaders += [h.format(job = jobID, stage = stage)
                                      for h in shuffleCsvHeaders]

        targetHeaders += terminalCsvHeaders

        with open(self.summaryDirectory+"/summary.csv", 'w') as f:
            writer = csv.writer(f, delimiter=',', lineterminator='\n')
            writer.writerow(targetHeaders)


    def produceFile(self, finalList):
        with open(self.summaryDirectory+"/summary.csv", 'a') as f:
            writer = csv.writer(f, delimiter=',', lineterminator='\n')
            writer.writerow(finalList)


    def retrieveApplicationTime(self):
        with open(self.directory+"/app_1.csv","r") as f:
            appRows = csv.DictReader(f)

            for index, row in enumerate(appRows):
                if index == 0:
                    self.appStartTime = int(row["Timestamp"])
                elif index == 1:
                    self.appEndTime = int(row["Timestamp"])


    def retrieveJobs(self, jobsFile):
        with open(jobsFile, "r") as f:
            targetList = []
            jobsRows = sorted(csv.DictReader(f), key=lambda x: x["Job ID"])
            i = 0
            max_ = 0

            while i < len(jobsRows):
                completionTime = int(jobsRows[i + 1]["Completion Time"]) - int(jobsRows[i]["Submission Time"])
                dirtyStages = jobsRows[i]["Stage IDs"][1:-1].split(", ")
                stages = sorted (s for s in dirtyStages if s in self.availableIDs)

                if self.stagesLen == 0:
                    self.stagesLen = len(stages)

                targetList.append([jobsRows[i]["Job ID"], completionTime, stages])
                i += 2

        return targetList


    def produceFinalList(self):
        """Compare the number of jobs/stages of this application with
           the ones of all the applications processed before.
        """
        finalList = []

        finalList.append(self.directoryName)
        finalList.append(self.appEndTime - self.appStartTime)
        finalList.append(self.minTaskLaunchTime - self.appStartTime)

        for job in self.jobsList:
            batch = [job[1]]

            for stageId, stageItem in self.stagesTasksDict.iteritems ():
                if stageId in job[2]:
                    batch += stageItem.values()[1:]

            finalList += batch

        finalList.append(self.users)
        finalList.append(self.memory)
        finalList.append(self.containers)

        return finalList


    def run(self):
        tasksFile = self.directory + "/tasks_1.csv"
        jobsFile = self.directory + "/jobs_1.csv"
        stagesFile = self.directory + "/stages_1.csv"

        self.retrieveApplicationTime()

        with open(stagesFile, "r") as infile:
            rows = self.orderStages(csv.DictReader(infile))
            self.availableIDs = [r["Stage ID"] for r in rows]

        with open(tasksFile, "r") as f:
            stagesRows = self.orderStages(csv.DictReader(f))
            self.stagesRows = [r for r in stagesRows if r["Stage ID"] in self.availableIDs]
            self.minTaskLaunchTime = min(map(lambda x: int(x["Launch Time"]) , self.stagesRows))

        self.jobsList = self.retrieveJobs(jobsFile)
        self.jobsCardinality = len(self.jobsList)

        self.buildStagesTasksDict()

        if self.headerFlag:
            self.writeHeader()

        self.produceFile(self.produceFinalList())


    def fileValidation(self, filename):
        """Check the existence of the given file path."""
        if not os.path.exists(filename):
            print("The file " + filename + " does not exists")
            sys.exit(1)


    def orderStages(self, stages):
        """Order the stages dict by 'Stage Id'."""
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
        avgTask = sum(normalBatch) / len(normalBatch)
        avgShuffle = sum(shuffleBatch) / len(shuffleBatch)
        maxBytes = max(bytesBatch)
        avgBytes = sum(bytesBatch) / len(bytesBatch)

        targetDict = OrderedDict({})
        targetDict["stageId"] = stageId
        targetDict["nTask"] = len(batch)
        targetDict["maxTask"] = maxTask
        targetDict["avgTask"] = avgTask

        # If one is negative (because it was missing in the logs),
        # all are negative.
        if maxShuffle >= 0:
            targetDict["SHmax"] = maxShuffle
            targetDict["SHavg"] = avgShuffle
            targetDict["Bmax"] = maxBytes
            targetDict["Bavg"] = avgBytes

        return targetDict


    def buildStagesTasksDict(self):
        batch = []
        lastRow = None

        for row in self.stagesRows:
            if lastRow != None and lastRow["Stage ID"] != row["Stage ID"]:
                stageId = lastRow["Stage ID"]
                self.stagesTasksDict[stageId] = self.computeStagesTasksDetails(stageId, batch)
                batch = []

            if row["Shuffle Write Time"] == "NOVAL":
                batch.append([int(row["Executor Run Time"]), -1, -1])
            else:
                batch.append(
                    [int(row["Executor Run Time"]), int(row["Shuffle Write Time"]), int(row["Shuffle Bytes Written"])])

            lastRow = row

        stageId = lastRow["Stage ID"]
        self.stagesTasksDict[stageId] = self.computeStagesTasksDetails(stageId, batch)


def directoryScan(regex, directory, users, datasize, totCores):
    headerCond = True
    rx = re.compile(regex)

    for fileName in os.listdir(directory+"/logs"):
        path = directory+"/logs/"+fileName

        if os.path.isdir(path) and rx.match(fileName):
            Extractor(directory,path,users,datasize,totCores,headerCond).run()

            if headerCond:
                headerCond = False


def main():
    args = sys.argv

    if len(args) != 6:
        print("Required args: [REGEX] [QUERY DIRECTORY] [USERS] [DATASIZE] [TOT CORES]")
        sys.exit(2)
    else:
        directoryScan(str(args[1]), str(args[2]), str(args[3]), str(args[4]), str(args[5]))


if __name__ == '__main__':
    main()
