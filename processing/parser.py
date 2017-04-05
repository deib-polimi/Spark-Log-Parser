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

from __future__ import print_function

import json
import csv
import os
import sys


class SparkParser:
    def __init__(self,filename,appId,outputDir):
        if os.path.exists(outputDir):
            self.outputDir = outputDir
        else:
            print("The inserted output directory does not exists", file = sys.stderr)
            sys.exit(1)

        if os.path.exists(filename):
            self.filename = filename
        else:
            print("The inserted file does not exists", file = sys.stderr)
            sys.exit(1)

        #Class props
        self.appId = appId
        self.tasksCSVInfo = []
        self.stagesCSVInfo = []
        self.jobsCSVInfo = []
        self.appCSVInfo = []

        self.stageHeaders = {
            "Stage Info" : [
                "Stage ID",
                "Stage Name",
                "Parent IDs",
                "Number of Tasks",
                "Submission Time",
                "Completion Time"
            ]
        }
        self.jobHeaders = {"_":["Job ID","Submission Time","Stage IDs","Completion Time"]}
        self.tasksHeaders = {
            #Default nesting level
            "_" : [
                "Stage ID",
                "Task Type",
            ],
            #Inside Task Info
            "Task Info": [
                "Task ID",
                "Host",
                "Executor ID",
                "Locality",
                "Launch Time",
                "Finish Time",
                "Getting Result Time",
            ],
            #Inside Task Metrics
            "Task Metrics": [
                "Executor Run Time",
                "Executor Deserialize Time",
                "JVM GC Time",
                "Result Size",
                "Memory Bytes Spilled",
                "Disk Bytes Spilled",
            ],
            "Shuffle Write Metrics": [
                "Shuffle Bytes Written",
                "Shuffle Write Time",
                "Shuffle Records Written"
            ]
        }
        self.applicationHeaders = {
            "_": [
                "App ID",
                "Timestamp",
            ]
        }


    def run(self):
        print("Start parsing")
        self.parseSwitch()
        print("Start saving files")
        self.produceCSVs()
        print("Finished")


    def parse(self, data, headers, csvinfo):
        record = []

        for field,value in headers.iteritems():
            for sub_field in value:
                try:
                    if field == "_":
                        record.append(data[sub_field])
                    elif field == "Shuffle Write Metrics":
                        record.append(data["Task Metrics"][field][sub_field])
                    else:
                        record.append(data[field][sub_field])
                except KeyError:
                    record.append("NOVAL")

        csvinfo.append(record)


    def parseSwitch(self):
        with open(self.filename) as infile:
            for line in infile:
                try:
                    data = json.loads(line)
                    event = data["Event"]

                    if event == "SparkListenerTaskEnd" and not data["Task Info"]["Failed"]:
                        self.parse(data, self.tasksHeaders, self.tasksCSVInfo)
                    elif event == "SparkListenerStageCompleted":
                        self.parse(data, self.stageHeaders, self.stagesCSVInfo)
                    elif event == "SparkListenerJobStart" or event == "SparkListenerJobEnd":
                        self.parse(data, self.jobHeaders, self.jobsCSVInfo)
                    elif event == "SparkListenerApplicationStart" or event == "SparkListenerApplicationEnd":
                        self.parse(data, self.applicationHeaders, self.appCSVInfo)

                except Exception as e:
                    print("Error "+str(e), file = sys.stderr)


    def normalizeHeaders(self, headersDict):
        returnList = []

        for inner in headersDict.itervalues ():
            for subfield in inner:
                returnList.append(subfield)

        return returnList


    def produceCSVs(self):
        csvTasks = [
            {
                "filename": self.outputDir+"/tasks_"+self.appId+".csv",
                "records": self.tasksCSVInfo,
                "headers": self.normalizeHeaders(self.tasksHeaders)
            },
            {
                "filename": self.outputDir+"/jobs_"+self.appId+".csv",
                "records": self.jobsCSVInfo,
                "headers": self.normalizeHeaders(self.jobHeaders)
            },
            {
                "filename" : self.outputDir+"/stages_"+self.appId+".csv",
                "records" : self.stagesCSVInfo,
                "headers" : self.normalizeHeaders(self.stageHeaders)
            },
            {
                "filename": self.outputDir+"/app_"+self.appId+".csv",
                "records": self.appCSVInfo,
                "headers": self.normalizeHeaders(self.applicationHeaders)
            }
        ]

        for item in csvTasks:
            with open(item["filename"], "w") as outfile:
                writer = csv.writer(outfile, delimiter=',', lineterminator='\n')
                writer.writerow(item["headers"])
                writer.writerows(item["records"])


def main():
    args = sys.argv
    if len(args) != 4:
        print("Required args: [LOG_FILE_TO_PARS] [ID_FOR_CSV_NAMING] [OUTPUTDIR]", file = sys.stderr)
        sys.exit(2)
    else:
        parser = SparkParser(str(args[1]), str(args[2]), str(args[3])).run()


if __name__ == "__main__":
    main()
