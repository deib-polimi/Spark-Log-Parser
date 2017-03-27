## Copyright 2017 Eugenio Gianniti <eugenio.gianniti@polimi.it>
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
    def __init__(self, jobsFile, stagesFile, stagesRelfile, targetDirectory):
        self.targetDirectory = targetDirectory
        self.stageJobMap = {}
        self.stagesRows = []
        self.jobsMap = {}
        self.jobsFile = jobsFile
        self.stagesRelFile = stagesRelfile
        self.stagesFile = stagesFile
        self.availableIDs = None

        map(lambda x: self.fileValidation(x), [jobsFile, stagesFile, stagesRelfile])
        self.parseJobs()
        self.buildJobHierarchy()

        with open(stagesFile, "r") as infile:
            self.stagesRows = self.orderStages(csv.DictReader(infile))

        self.buildTimeFiles()
        self.buildOutputString()


    def fileValidation(self, filename):
        """Check the existence of the given file path."""
        if not os.path.exists(filename):
            print("The file "+filename+" does not exists")
            sys.exit(1)


    def parseJobs(self):
        """Read job records from a CSV file and build a dict based upon them."""
        jobs = {}

        with open(self.jobsFile,"r") as infile:
            jobsReader = csv.DictReader(infile)

            for row in jobsReader:
                stageIds = row["Stage IDs"]
                jobId = row["Job ID"]
                completionTime = row["Completion Time"]
                submissionTime = row["Submission Time"]

                if(stageIds != "NOVAL"):
                    stagesList = self.parseStagesList(stageIds)

                    for stage in stagesList:
                        self.stageJobMap[stage] = jobId

                    self.jobsMap[jobId] = {
                        "stages": self.parseStagesList(stageIds),
                        "submissionTime": int(submissionTime),
                        "completionTime": 0,
                        "followers" : [],
                        "parents" : [],
                        "firstStages":[],
                        "lastStages" : []
                    }

                if completionTime != "NOVAL":
                    self.jobsMap[jobId]["completionTime"] = int(completionTime)


    def orderStages(self, stages):
        """Order the stages dict by 'Stage Id'."""
        return sorted(stages, key = lambda x: x["Stage ID"])


    def parseStagesList(self, stagesList):
        """Split correctly a list of stages."""
        return stagesList[1:-1].split(", ")


    def buildJobHierarchy(self):
        """Build a simple hierarchy among job based on the fact that
        a job is considered a parent of another one if it finishes before its start.
        """
        for key, value in self.jobsMap.iteritems():
            for key_1, value_1 in self.jobsMap.iteritems():
                if value["completionTime"] < value_1["submissionTime"] and key != key_1:
                    self.jobsMap[key_1]["parents"].append(key)

        self.buidlComplexJobHierarchy()
        self.decorateWithFollowers(self.jobsMap)


    def buidlComplexJobHierarchy(self):
        """Build a complex job hierarchy from a simple one."""
        counter = 0
        tmp = []

        #Order the parents of a job per temporal distance from the job
        for key_, value in self.jobsMap.iteritems():
            value["parents"] = sorted(value["parents"], key=lambda x: self.jobsMap[key_]["submissionTime"] - self.jobsMap[x]["completionTime"])

        """Exclude for each job, those parents which are also parents of other parents of the job
        e.g job0 -> parents = [job3,job4,job5]
        job4 is not the parent of job3, but job5 is the parent of job3, so job5 must be excluded.
        """
        for key,value in self.jobsMap.iteritems():
            parents = value["parents"]

            if len(parents) != 0:
                tmp.append(parents[0])

            for index, parent in enumerate(parents):
                if(index != 0):
                    for index_1, parent_1 in enumerate(parents[:index]):
                        if parent not in self.jobsMap[parents[index_1]]["parents"]:
                            counter=counter+1

                    if counter == len(parents[:index]):
                        tmp.append(parent)

                    counter = 0

            value["parents"] = tmp
            tmp = []


    def decorateWithFollowers(self, jobsMap):
        """From a map in which each node contains just a 'parents' field,
        decorate such nodes with a proper 'followers' field."""
        for key,value in jobsMap.iteritems():
            for key_1, value_1 in jobsMap.iteritems():
                if key != key_1 and key in value_1["parents"]:
                    value["followers"].append(key_1)


    def buildTimeFiles(self):
        """Build .txt files containing the execution time of each task in a stage."""
        batch = []
        lastRow = None

        for row in self.stagesRows:
            if lastRow != None and lastRow["Stage ID"] != row["Stage ID"]:
                with open(self.targetDirectory+"/J"+self.stageJobMap[lastRow["Stage ID"]]+"S"+lastRow["Stage ID"]+".txt","w") as outfile:
                    outfile.write("\n".join(batch))

                batch = []

            batch.append(row["Executor Run Time"])
            lastRow = row

        with open(self.targetDirectory+"/J"+self.stageJobMap[lastRow["Stage ID"]]+"S"+lastRow["Stage ID"]+".txt","w") as outfile:
            outfile.write("\n".join(batch))


    def stagesRel(self):
        """Build parent-child dependencies among stages in the context of a single job."""
        with open(self.stagesRelFile, "r") as infile:
            rows = self.orderStages(csv.DictReader(infile))
            self.availableIDs = [r["Stage ID"] for r in rows]
            stagesMap = {r["Stage ID"]: {
                "parents": None,
                "children": [],
                "tasks": r["Number of Tasks"],
                "name": "S{}".format (r["Stage ID"])
            } for r in rows}

            for row in rows:
                parentIds = row["Parent IDs"]
                stageId = row["Stage ID"]
                parents = self.parseStagesList(parentIds)

                if len(parents) == 1 and parents[0] == '':
                    parents = []

                parents = sorted (p for p in parents if p in self.availableIDs)

                stagesMap[stageId]["parents"] = parents

                for parent in parents:
                    stagesMap[parent]["children"].append(stageId)

        return stagesMap


    def perJobStagesRel(self):
        """Build parent-child dependencies among stages considering the
        parent-child dependencies among jobs."""
        stagesMap = self.stagesRel()
        tmpFirst = []
        tmpLast = []
        newMap = []

        """For each job retrieve the first stages and the last stages"""
        for key, job in self.jobsMap.iteritems():
            cleanStages = (s for s in job["stages"] if s in self.availableIDs)

            for stage in cleanStages:
                stagesMap[stage]["name"] = "J"+key+stagesMap[stage]["name"]

                if len(stagesMap[stage]["children"]) == 0:
                    tmpLast.append(stage)

                if len(stagesMap[stage]["parents"]) == 0:
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
                for nextJob in job["followers"]:
                    for stage_1 in self.jobsMap[nextJob]["first"]:
                        stagesMap[stage_1]["parents"].append(stage)
                        stagesMap[stage]["children"].append(stage_1)

        return stagesMap


    def buildOutputString(self):
        """Build a string, to be passed to the DAGSimulator, that represents
        the hierarchies among stages created with the other methods."""
        stagesDict = self.perJobStagesRel()
        targetString = ''

        for key, value in stagesDict.iteritems():
            namedParents = map(lambda x: stagesDict[x]["name"], value["parents"])
            namedChildren = map(lambda x: stagesDict[x]["name"], value["children"])
            namedParents = reduce(lambda accumul, current: accumul+'"'+current+'",',namedParents, '' )
            namedChildren = reduce(lambda accumul, current: accumul+'"'+current+'",',namedChildren, '' )

            if namedParents != '':
                namedParents = namedParents[:-1]

            if namedChildren != '':
                namedChildren = namedChildren[:-1]

            targetString += '{ name="'+value["name"]+'", tasks="'+value["tasks"]+'"'
            targetString += ', distr={type="replay", params={samples=solver.fileToArray("'+self.targetDirectory+value["name"]+'.txt")}}'
            targetString += ', pre={'+namedParents+'}, post={'+namedChildren+'}},'

        targetString = '{'+targetString[:-1]+'}'
        print(targetString)


def main():
    args = sys.argv

    if len(args) != 5:
        print("Required args: [JOBS_FILE_CSV] [STAGE_FILE_CSV] [STAGE_REL_FILE_CSV] [DIRECTORY_FOR_OUTPUTTED_STRING]")
        sys.exit(2)
    else:
        parser = Parser(str(args[1]), str(args[2]), str(args[3]), str(args[4])+'/')


if(__name__=="__main__"):
    main()
