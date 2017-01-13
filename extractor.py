from __future__ import division
import os
import csv
import sys

class Extractor:
    def __init__(self, jobsFile, stagesFile):
        self.targetDirectory= './'
        self.jobsFile = jobsFile
        self.stagesFile = stagesFile
        self.stagesRows = None
        self.targetList = []
        self.completionStage = []

    def stageCompletion(self):
        f = open(self.jobsFile, "r")
        stages = csv.DictReader(f)
        for item in stages:
            self.completionStage.append({
                "stageId": item["Stage ID"],
                "completionTime" : item["Completion Time"]
            })

    def mergeList(self):
        targetList = []
        z = {}
        for item in self.completionStage:
            for sub_item in self.targetList:
                if(item["stageId"] == sub_item["stageId"]):
                    z = sub_item.copy()
                    z["completionTime"] = item["completionTime"]
                    targetList.append(z)
        return targetList

    def produceFile(self,list):
        f = open(self.targetDirectory+'summary.csv','w')
        headers = list[0].keys()
        writer = csv.writer(f, delimiter=',', lineterminator='\n')
        writer.writerow(headers)
        for item in list:
            writer.writerow(item.values())

    def run(self):
        self.fileValidation(self.stagesFile)
        f = open(self.stagesFile, "r")
        self.stagesRows = self.orderStages(csv.DictReader(f))
        f.close()
        self.buildTimeFiles()
        self.stageCompletion()
        self.produceFile(self.mergeList())

    """Checks the existence of the given file path"""
    def fileValidation(self,filename):
        if not(os.path.exists(filename)):
            print("The file "+filename+" does not exists")
            exit(-1)

    """Orders the stages dict by 'Stage Id'"""
    def orderStages(self,stages):
        return sorted(stages, key = lambda x: x["Stage ID"])


    def computeIndexes(self,stageId,batch):
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
        return ({
            "stageId" : stageId,
            "numTask" : len(batch),
            "maxTask" : maxTask,
            "avgTask" : avgTask,
            "maxShuffle" : maxShuffle,
            "avgShuffle" : avgShuffle
        })

    def buildTimeFiles(self):
        batch = []
        lastRow = None
        for row in self.stagesRows:
            if((lastRow != None and lastRow["Stage ID"] != row["Stage ID"])):
                self.targetList.append(self.computeIndexes(lastRow["Stage ID"],batch))
                batch = []
            batch.append([row["Executor Run Time"],row["Task Type"]])
            lastRow = row
        self.targetList.append(self.computeIndexes(lastRow["Stage ID"],batch))
        batch = []

def main():
    args = sys.argv
    if len(args) != 3:
        print("Required args: [JOBS_FILE_CSV] [STAGE_FILE_CSV]")
        exit(-1)
    else:
        extractor = Extractor(str(args[1]),str(args[2]))
        extractor.run()


if __name__ == '__main__':
    main()
