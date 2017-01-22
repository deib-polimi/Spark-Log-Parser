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

import csv
import sys
import os

class Parser:
    def __init__(self, jobsFile,stagesFile,stagesRelfile,targetDirectory):
        self.targetDirectory= targetDirectory
        self.stageJobMap = {}
        self.stagesRows = []
        self.jobsMap = {}
        self.jobsFile = jobsFile
        self.stagesRelFile = stagesRelfile
        self.stagesFile = stagesFile
        map(lambda x: self.fileValidation(x),[jobsFile,stagesFile, stagesRelfile])
        self.parseJobs()
        self.buildJobHierarchy()
        f = open(stagesFile, "r")
        self.stagesRows = self.orderStages(csv.DictReader(f))
        f.close()
        self.buildTimeFiles()
        self.buildOutputString()


    """Checks the existence of the given file path"""
    def fileValidation(self,filename):
        if not(os.path.exists(filename)):
            print("The file "+filename+" does not exists")
            exit(-1)

    """Reads jobs records from a csv file and builds a dict based upon them"""
    def parseJobs(self):
        jobs = {}
        f = open(self.jobsFile,"r")
        jobsReader = csv.DictReader(f)
        for row in jobsReader:
            stageIds = row["Stage IDs"]
            jobId = row["Job ID"]
            completionTime = row["Completion Time"]
            submissionTime = row["Submission Time"]

            if(stageIds != "NOVAL"):
                stagesList = self.parseStagesList(stageIds)
                for stage in stagesList:
                    self.stageJobMap[stage]=jobId
                self.jobsMap[jobId] = {
                    "stages":self.parseStagesList(stageIds),
                    "submissionTime": int(submissionTime),
                    "completionTime": 0,
                    "followers" : [],
                    "parents" : [],
                    "firstStages":[],
                    "lastStages" : []
                }
            if(completionTime != "NOVAL"):
                self.jobsMap[jobId]["completionTime"] = int(completionTime)
        f.close()

    """Orders the stages dict by 'Stage Id'"""
    def orderStages(self,stages):
        return sorted(stages, key = lambda x: x["Stage ID"])

    """Splits correctly a list of stages"""
    def parseStagesList(self,stagesList):
        return stagesList[1:len(stagesList)-1].split(", ")

    """Builds a simple hierarchy among job based on the fact that
    a job is considered a parent of another one if it finishes before the start of the other one"""
    def buildJobHierarchy(self):
        for key,value in self.jobsMap.iteritems():
            for key_1, value_1 in self.jobsMap.iteritems():
                if(value["completionTime"] < value_1["submissionTime"] and key != key_1):
                    self.jobsMap[key_1]["parents"].append(key)

        self.buidlComplexJobHierarchy()
        self.decorateWithFollowers(self.jobsMap)

    """Builds a complex job hierarchy from a simple one"""
    def buidlComplexJobHierarchy(self):
        counter = 0
        tmp = []
        #Order the parents of a job per temporal distance from the job
        for key_,value in self.jobsMap.iteritems():
            value["parents"] = sorted(value["parents"], key=lambda x: self.jobsMap[key_]["submissionTime"] - self.jobsMap[x]["completionTime"])

        """Exclude for each job, those parents which are also parents of other parents of the job
        e.g job0 -> parents = [job3,job4,job5]
        job4 is not the parent of job3, but job5 is the parent of job3, so job5 must be excluded.
        """
        for key,value in self.jobsMap.iteritems():
            parents = value["parents"]
            if(len(parents) != 0):
                tmp.append(parents[0])
            for index, parent in enumerate(parents):
                if(index != 0):
                    for index_1, parent_1 in enumerate(parents[:index]):
                        if(parent not in self.jobsMap[parents[index_1]]["parents"]):
                            counter=counter+1
                    if(counter == len(parents[:index])):
                        tmp.append(parent)
                    counter = 0
            value["parents"]=tmp
            tmp = []


    """From a map in which each node contains just a 'parents' field,
    decoretes such nodes with a proper 'followers' field"""
    def decorateWithFollowers(self,jobsMap):
        for key,value in jobsMap.iteritems():
            for key_1, value_1 in jobsMap.iteritems():
                if(key != key_1 and key in value_1["parents"]):
                    value["followers"].append(key_1)

    """Builds .txt files containing the execution time of each task in a stage"""
    def buildTimeFiles(self):
        batch = []
        lastRow = None
        for row in self.stagesRows:
            if((lastRow != None and lastRow["Stage ID"] != row["Stage ID"])):
                f = open(self.targetDirectory+"/J"+self.stageJobMap[lastRow["Stage ID"]]+"S"+lastRow["Stage ID"]+".txt","w")
                f.write("\n".join(batch))
                f.close()
                batch = []
            batch.append(row["Executor Run Time"])
            lastRow = row
        f = open(self.targetDirectory+"/J"+self.stageJobMap[lastRow["Stage ID"]]+"S"+lastRow["Stage ID"]+".txt","w")
        f.write("\n".join(batch))
        f.close()

    """Builds parent-child dependencies among stages in the context
    of a single job"""
    def stagesRel(self):
        f = open(self.stagesRelFile,"r")
        rows = self.orderStages(csv.DictReader(f))
        stagesMap = {}
        for row in rows:
            parentIds = row["Parent IDs"]
            stageId = row["Stage ID"]
            parents = self.parseStagesList(parentIds)
            if(len(parents)== 1 and parents[0] == ''):
                parents = []
            stagesMap[stageId]= {
                "parents": parents,
                "children": [],
                "tasks": row["Number of Tasks"],
                "name": "S"+stageId
            }
            for parent in parents:
                stagesMap[parent]["children"].append(stageId)

        return stagesMap

    """Builds parent-child dependencies among stages considering the
    parent-child dependencies among jobs"""
    def perJobStagesRel(self):
        stagesMap = self.stagesRel()
        tmpFirst = []
        tmpLast = []
        newMap = []
        """For each job retrieve the first stages and the last stages"""
        for key,job in self.jobsMap.iteritems():
            for stage in job["stages"]:
                stagesMap[stage]["name"] = "J"+key+stagesMap[stage]["name"]
                if(len(stagesMap[stage]["children"])==0):
                    tmpLast.append(stage)
                if(len(stagesMap[stage]["parents"])==0):
                    tmpFirst.append(stage)
            job["last"] = tmpLast
            job["first"] = tmpFirst
            tmpLast = []
            tmpFirst = []

        """For each job look at the last stages of that job, and for each
        of them consider the jobs that follows the current one, and for each
        of these jobs, consider their first stages, then express that the last stages
        of the current job are the parent of the first stages of the current child job
        and the contrary"""
        for key, job in self.jobsMap.iteritems():
            for stage in job["last"]:
                for next_job in job["followers"]:
                    for stage_1 in self.jobsMap[next_job]["first"]:
                        stagesMap[stage_1]["parents"].append(stage)
                        stagesMap[stage]["children"].append(stage_1)
        return stagesMap

    """Builds a string to be passed to the DAGSimulator that represents
    the hierarchies among stages created with the other methods"""
    def buildOutputString(self):
        stagesDict = self.perJobStagesRel()
        targetString = ''
        for key,value in stagesDict.iteritems():
            namedParents = map(lambda x: stagesDict[x]["name"], value["parents"])
            namedChildren = map(lambda x: stagesDict[x]["name"], value["children"])
            namedParents = reduce(lambda accumul, current: accumul+'"'+current+'",',namedParents, '' )
            namedChildren = reduce(lambda accumul, current: accumul+'"'+current+'",',namedChildren, '' )
            if(namedParents!=''):
                namedParents = namedParents[:len(namedParents)-1]
            if(namedChildren!=''):
                namedChildren = namedChildren[:len(namedChildren)-1]
            targetString+='{ name="'+value["name"]+'", tasks="'+value["tasks"]+'"'
            targetString+=', distr={type="replay", params={samples=solver.fileToArray("'+self.targetDirectory+value["name"]+'.txt")}}'
            targetString+=', pre={'+namedParents+'}, post={'+namedChildren+'}},'
        targetString = '{'+targetString[:len(targetString)-1]+'}'
        print(targetString)



def main():
    args = sys.argv
    if len(args) != 5:
        print("Required args: [JOBS_FILE_CSV] [STAGE_FILE_CSV] [STAGE_REL_FILE_CSV] [DIRECTORY_FOR_OUTPUTTED_STRING]")
        exit(-1)
    else:
        parser = Parser(str(args[1]),str(args[2]),str(args[3]),str(args[4])+'/')

if(__name__=="__main__"):
    main()
