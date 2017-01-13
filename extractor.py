from __future__ import division
import os
import csv

class Extractor:
    def __init__(self, stagesFile,targetDirectory):
        self.targetDirectory= targetDirectory
        self.stagesFile = stagesFile


    def run():
        self.fileValidation(stagesFile)
        f = open(stagesFile, "r")
        self.stagesRows = self.orderStages(csv.DictReader(f))
        f.close()
        self.buildTimeFiles()

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
        targetDictList = []
        for item in batch:
            if item[1] == "Result Task":
                normalBatch.append(int(item[0]))
            else:
                shuffleBatch.append(int(item[0]))

        maxTask = max(normalBatch)
        maxShuffle = max(shuffleBatch)
        avgTask = reduce(lambda x, y: x + y, normalBatch) / len(normalBatch)
        avgShuffle = reduce(lambda x, y: x + y, shuffleBatch) / len(shuffleBatch)
        targetDictList.append({
            stageId : {
                "maxTask" : maxTask,
                "avgTask" : avgTask,
                "maxShuffle" : maxShuffle,
                "avgShuffle" : avgShuffle
            }
        })
        return targetDictList

    def buildTimeFiles(self):
        batch = []
        lastRow = None
        for row in self.stagesRows:
            if((lastRow != None and lastRow["Stage ID"] != row["Stage ID"])):
                print(computeIndexes(lastRow["Stage ID"],batch))
                batch = []
            batch.append([row["Executor Run Time"],row["Task Type"]])
            lastRow = row

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
