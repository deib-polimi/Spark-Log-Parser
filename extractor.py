from __future__ import division
from collections import OrderedDict
import os
import csv
import sys


class Extractor:
    def __init__(self, directory, target_directory, users, memory, containers):
        self.containers = containers
        self.users = users
        self.memory = memory
        self.directory = directory
        self.directoryName = os.path.basename(directory)
        self.directoryName = self.directoryName[:len(self.directoryName) - 4]
        self.target_directory = target_directory
        self.stagesRows = None
        self.stagesTasksList = []
        self.stagesCompletionList = []
        self.jobsList = []
        self.maxStagesLenght = 0

    def writeHeader(self):
        with open(self.target_directory + '/summary.csv', 'a') as f:
            writer = csv.writer(f, delimiter=',', lineterminator='\n')
            targetHeaders = []
            jobHeaders = ['run', 'jobId', 'CompletionTime']
            stageHeaders = ['stageId', 'nTask', 'maxTask', 'avgTask', 'SHmax', 'SHavg', 'Bmax', 'Bavg']
            terminalHeaders = ['users', 'dataSize', 'nContainers']
            targetHeaders.append(jobHeaders)
            for i in range(0, self.maxStagesLenght):
                targetHeaders.append(stageHeaders)
            targetHeaders.append(terminalHeaders)
            writer.writerow(targetHeaders)

    def produceFile(self, finalList):
        with open(self.target_directory + '/summary.csv', 'a') as f:
            writer = csv.writer(f, delimiter=',', lineterminator='\n')
            for item in finalList:
                writer.writerow(item)

    def retrieve_jobs(self, jobs_file):
        with open(jobs_file, "r") as f:
            targetList = []
            jobs_rows = sorted(csv.DictReader(f), key=lambda x: x["Job ID"])
            i = 0
            max = 0
            while i < len(jobs_rows):
                completionTime = int(jobs_rows[i + 1]["Completion Time"]) - int(jobs_rows[i]["Submission Time"])
                stages = jobs_rows[i]["Stage IDs"][1:(len(jobs_rows[i]["Stage IDs"]) - 1)].split(", ")
                if max < len(stages):
                    max = len(stages)
                targetList.append([jobs_rows["Job ID"], completionTime, stages])
            self.maxStagesLenght = max
        return targetList

    def produce_final_list(self):
        final_list = []
        tmp_list = []
        for job in self.jobsList:
            tmp_list.append(job[0])
            tmp_list.append(job[1])
            for stageItem in self.stagesTasksList:
                if stageItem["Stage ID"] in job[2]:
                    tmp_list = tmp_list + stageItem
            tmp
        return final_list

    def run(self):
        tasks_file = self.directory + "/tasks_1.csv"
        jobs_file = self.directory + "/jobs_1.csv"
        with open(tasks_file, "r") as f:
            self.stagesRows = self.orderStages(csv.DictReader(f))
        self.jobsList = self.retrieve_jobs(jobs_file)
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


def main():
    args = sys.argv
    if len(args) != 6:
        print("Required args: [TOP_DIRECTORY] [TARGET_DIRECTORY]")
        exit(-1)
    else:
        extractor = Extractor(str(args[1]), str(args[2]), str(args[3]), str(args[4]), str(args[5]))
        extractor.run()


if __name__ == '__main__':
    main()
