from __future__ import division
from collections import OrderedDict
import os
import csv
import sys

class Extractor:
    def __init__(self, jobsFile, stagesFile, resultDirectory):
        self.resultDirectory = resultDirectory
        self.jobsFile = jobsFile
        self.stagesFile = stagesFile
        self.stagesRows = None
        self.stagesTasksList = []
        self.stagesCompletionList = []

    def buildstagesCompletionList(self):
        f = open(self.jobsFile, "r")
        stages = csv.DictReader(f)
        for item in stages:
            self.stagesCompletionList.append({
                "stageId": item["Stage ID"],
                "completionTime" : item["Completion Time"]
            })

    def mergeList(self):
        targetList = []
        z = OrderedDict({})
        for item in self.stagesCompletionList:
            for sub_item in self.stagesTasksList:
                if(item["stageId"] == sub_item["stageId"]):
                    z = sub_item.copy()
                    z["completionTime"] = item["completionTime"]
                    targetList.append(z)
        return targetList

    def produceFile(self,finalList):
        f = open(self.resultDirectory+'/summary.csv','w')
        headers = finalList[0].keys()
        writer = csv.writer(f, delimiter=',', lineterminator='\n')
        writer.writerow(headers)
        for item in finalList:
            writer.writerow(item.values())

    def run(self):
        self.fileValidation(self.stagesFile)
        f = open(self.stagesFile, "r")
        self.stagesRows = self.orderStages(csv.DictReader(f))
        f.close()
        self.buildstagesTasksList()
        self.buildstagesCompletionList()
        self.produceFile(self.mergeList())

    """Checks the existence of the given file path"""
    def fileValidation(self,filename):
        if not(os.path.exists(filename)):
            print("The file "+filename+" does not exists")
            exit(-1)

    """Orders the stages dict by 'Stage Id'"""
    def orderStages(self,stages):
        return sorted(stages, key = lambda x: x["Stage ID"])


    def computeStagesTasksDetails(self,stageId,batch):
        shuffleBatch = []
        normalBatch = []
        for item in batch:
            if item[1] == "ResultTask":
                normalBatch.append(int(item[0]))
            else:
                shuffleBatch.append(int(item[0]))

        if(len(shuffleBatch)==0):
            shuffleBatch.append(-1)
        if(len(normalBatch)==0):
            normalBatch.append(-1)
        maxTask = max(normalBatch)
        maxShuffle = max(shuffleBatch)
        avgTask = reduce(lambda x, y: x + y, normalBatch) / len(normalBatch)
        avgShuffle = reduce(lambda x, y: x + y, shuffleBatch) / len(shuffleBatch)
        targetDict = OrderedDict({})
        targetDict["stageId"] = stageId
        targetDict["numTask"] = len(batch)
        targetDict["maxTask"] = maxTask
        targetDict["avgTask"] = avgTask
        targetDict["maxShuffle"] = maxShuffle
        targetDict["avgShuffle"] = avgShuffle
        return targetDict

    def buildstagesTasksList(self):
        batch = []
        lastRow = None
        for row in self.stagesRows:
            if lastRow != None and lastRow["Stage ID"] != row["Stage ID"]:
                self.stagesTasksList.append(self.computeStagesTasksDetails(lastRow["Stage ID"],batch))
                batch = []
            batch.append([row["Executor Run Time"],row["Task Type"]])
            lastRow = row
        self.stagesTasksList.append(self.computeStagesTasksDetails(lastRow["Stage ID"],batch))

def main():
    args = sys.argv
    if len(args) != 4:
        print("Required args: [STAGES_PER_JOB_CSV_FILE] [TASKS_PER_STAGE_CSV_FILE] [RESULT_DIRECTORY]")
        exit(-1)
    else:
        extractor = Extractor(str(args[1]),str(args[2]),str(args[3]))
        extractor.run()


if __name__ == '__main__':
    main()
