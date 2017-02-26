-- DAG definition: it is encoded as an array of stages.
Stages = @@STAGES@@;

-- Number of computation nodes in the system
Nodes = @@CONTAINERS@@;

-- Number of users accessing the system
Users = @@USERS@@;

-- Distribution of the think time for the users
UThinkTimeDistr = {type = "@@TYPE@@", params = @@PARAMS@@};

-- Total number of jobs to simulate
maxJobs = @@MAXJOBS@@;

-- Coefficient for the Confidence Intervals
confIntCoeff = @@COEFF@@;
