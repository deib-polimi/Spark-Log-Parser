#! /usr/bin/env python3

## Copyright 2018 Eugenio Gianniti <eugenio.gianniti@polimi.it>
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
import json
import os
import sys


class SparkParser:
    def __init__(self, filename, appId, outputDir):
        if os.path.exists(outputDir):
            self.outputDir = outputDir
        else:
            print ("error: the inserted output directory '{}' does not exist"
                   .format(outputDir), file = sys.stderr)
            sys.exit(1)

        if os.path.exists(filename):
            self.filename = filename
        else:
            print ("error: the inserted file '{}' does not exist"
                   .format(filename), file = sys.stderr)
            sys.exit(1)

        #Class props
        self.appId = appId
        self.tasksCSVInfo = []
        self.stagesCSVInfo = []
        self.jobsCSVInfo = []
        self.appCSVInfo = []
        self.executorsCSVInfo = []

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
        self.executorsHeaders = {
            "_": [
                "Executor ID",
                "Timestamp"
            ],
            "Executor Info": [
                "Host",
                "Total Cores"
            ]
        }


    def run(self):
        self.parseSwitch()
        self.produceCSVs()


    def parse(self, data, headers):
        record = {}

        for field, inner in headers.items ():
            for subfield in inner:
                try:
                    if field == "_":
                        value = data[subfield]
                    elif field == "Shuffle Write Metrics":
                        value = data["Task Metrics"][field][subfield]
                    else:
                        value = data[field][subfield]

                    record[subfield] = value
                except KeyError:
                    pass

        return record


    def parseSwitch(self):
        jobData = {}

        with open(self.filename) as infile:
            for line in infile:
                try:
                    data = json.loads(line)
                    event = data["Event"]

                    if event == "SparkListenerTaskEnd" and not data["Task Info"]["Failed"]:
                        record = self.parse(data, self.tasksHeaders)
                        self.tasksCSVInfo.append (record)
                    elif event == "SparkListenerStageCompleted":
                        if "Failure Reason" in data["Stage Info"]:
                            print ("error: stage {id} failed in '{name}'"
                                   .format (id = data["Stage Info"]["Stage ID"],
                                            name = self.filename),
                                   file = sys.stderr)
                            sys.exit (3)
                        else:
                            record = self.parse(data, self.stageHeaders)
                            self.stagesCSVInfo.append (record)
                    elif event == "SparkListenerJobStart":
                        record = self.parse(data, self.jobHeaders)
                        jobData[record["Job ID"]] = record
                    elif event == "SparkListenerJobEnd":
                        record = self.parse(data, self.jobHeaders)
                        jobId = record["Job ID"]

                        try:
                            previous = jobData.pop (jobId)
                        except KeyError:
                            print ("error: job {} ended without starting"
                                   .format (jobId), file = sys.stderr)
                            sys.exit (3)

                        record.update (previous)
                        self.jobsCSVInfo.append (record)
                    elif event == "SparkListenerApplicationStart":
                        record = self.parse(data, self.applicationHeaders)
                        appData = {"App ID": record["App ID"],
                                   "Submission Time": record["Timestamp"]}
                    elif event == "SparkListenerApplicationEnd":
                        record = self.parse(data, self.applicationHeaders)
                        appData["Completion Time"] = record["Timestamp"]
                        self.appCSVInfo.append (appData)
                    elif event == "SparkListenerExecutorAdded":
                        record = self.parse(data, self.executorsHeaders)
                        self.executorsCSVInfo.append (record)

                except Exception as e:
                    print ("warning: {}".format (e), file = sys.stderr)


    def normalizeHeaders(self, headersDict):
        return [subfield for inner in headersDict.values ()
                for subfield in inner]


    def produceCSVs(self):
        csvTasks = [
            {
                "filename": os.path.join(self.outputDir, "tasks_{}.csv".format(self.appId)),
                "records": self.tasksCSVInfo,
                "headers": self.normalizeHeaders(self.tasksHeaders)
            },
            {
                "filename": os.path.join(self.outputDir, "jobs_{}.csv".format(self.appId)),
                "records": self.jobsCSVInfo,
                "headers": self.normalizeHeaders(self.jobHeaders)
            },
            {
                "filename": os.path.join(self.outputDir, "stages_{}.csv".format(self.appId)),
                "records": self.stagesCSVInfo,
                "headers": self.normalizeHeaders(self.stageHeaders)
            },
            {
                "filename": os.path.join(self.outputDir, "app_{}.csv".format(self.appId)),
                "records": self.appCSVInfo,
                "headers": ["App ID", "Submission Time", "Completion Time"]
            },
            {
                "filename": os.path.join(self.outputDir, "executors_{}.csv".format(self.appId)),
                "records": self.executorsCSVInfo,
                "headers": self.normalizeHeaders(self.executorsHeaders)
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
        print ("Required args: [LOG_FILE_TO_PARSE] [ID_FOR_CSV_NAMING] [OUTPUTDIR]",
               file = sys.stderr)
        sys.exit(2)
    else:
        parser = SparkParser(str(args[1]), str(args[2]), str(args[3]))
        parser.run()


if __name__ == "__main__":
    main()
