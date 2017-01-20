#!/bin/bash
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