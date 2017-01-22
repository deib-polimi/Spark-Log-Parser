#!/bin/bash

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

#DAGSIM parameters
# Number of computation nodes in the system
export DAGSIM_CONTAINERS=16
# Number of users accessing the system
export DAGSIM_USERS=1
#Distribution of the think time for the users. This element is a distribution with the same
#format as the task running times
export DAGSIM_UTHINKTIMEDISTR_TYPE="exp"
export DAGSIM_UTHINKTIMEDISTR_PARAMS="{rate = 0.001}"

#Total number of jobs to simulate
export DAGSIM_MAXJOBS=1000
#Coefficient for the Confidence Intervals
#99%	2.576
#98%	2.326
#95%	1.96
#90%	1.645
export DAGSIM_CONFINTCOEFF=1.96
