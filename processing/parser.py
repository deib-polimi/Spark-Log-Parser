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

import csv
import json
import os
import sys


class SparkParser:
    def __init__(self,filename,appId,outputDir):
        if os.path.exists(outputDir):
            self.outputDir = outputDir
        else:
            print ("error: the inserted output directory '{}' does not exist".format(outputDir),
                   file = sys.stderr)
            sys.exit(1)

        if os.path.exists(filename):
            self.filename = filename
        else:
            print ("error: the inserted file '{}' does not exist".format(filename),
                   file = sys.stderr)
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
        self.jobHeaders = {
            "_" : [
                "Job ID",
                "Submission Time",
                "Stage IDs",
                "Completion Time"
            ]
        }
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
            ],
            "Task End Reason": [
                "Reason"
            ]
        }
        self.applicationHeaders = {
            "_": [
                "App ID",
                "Timestamp",
            ]
        }


    def run(self):
        self.parseSwitch()
        self.produceCSVs()


    def parse(self, data, headers, csvinfo):
        record = {}

        for field, inner in headers.iteritems ():
            for subfield in inner:
                try:
                    if field == "_":
                        value = data[subfield]
                    elif field == "Shuffle Write Metrics":
                        value = data["Task Metrics"][field][subfield]
                    else:
                        value = data[field][subfield]
                except KeyError:
                    value = "NOVAL"

                record[subfield] = value

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
                        if "Failure Reason" in data["Stage Info"]:
                            print ("error: stage {} failed"
                                   .format (data["Stage Info"]["Stage ID"]),
                                   file = sys.stderr)
                            sys.exit (3)
                        else:
                            self.parse(data, self.stageHeaders, self.stagesCSVInfo)
                    elif event == "SparkListenerJobStart" or event == "SparkListenerJobEnd":
                        self.parse(data, self.jobHeaders, self.jobsCSVInfo)
                    elif event == "SparkListenerApplicationStart" or event == "SparkListenerApplicationEnd":
                        self.parse(data, self.applicationHeaders, self.appCSVInfo)

                except Exception as e:
                    print ("error: "+str(e), file = sys.stderr)


    def normalizeHeaders(self, headersDict):
        return [subfield for inner in headersDict.itervalues ()
                for subfield in inner]


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
            with open (item["filename"], "w") as outfile:
                writer = csv.DictWriter (outfile, fieldnames = item["headers"])
                writer.writeheader ()
                writer.writerows (item["records"])


def main():
    args = sys.argv
    if len(args) != 4:
        print ("Required args: [LOG_FILE_TO_PARS] [ID_FOR_CSV_NAMING] [OUTPUTDIR]",
               file = sys.stderr)
        sys.exit(2)
    else:
        parser = SparkParser(str(args[1]), str(args[2]), str(args[3])).run()


if __name__ == "__main__":
    main()
