
-- Model specification

-- DAG definition: it is encoded as an array of stages.
-- Each stage is spcified by the following parameters:
--    name   (String)   : name of the stage
--    tasks  (Int)      : number of tasks in this stage
--    distr  (Distrib.) : distribution of the task running time
--                        Distributions are characterized by the following data:
--                             type   : the name of the distribution to be generated
--                             params : parameters specific to the chosen distribution type
--    pre    (String[]) : name of the stages that must be completed for this one to start
--    post   (String[]) : name of the stages that can start after this one has finished
--
Stages = [STAGES];
-- Number of computation nodes in the system
Containers = [CONTAINERS];
-- Number of users accessing the system
Users = [USERS];
-- Distribution of the think time for the users. This element is a distribution with the same
-- format as the task running times
UThinkTimeDistr = {type = "[TYPE]", params = [PARAMS]};

-- Total number of jobs to simulate
maxJobs = [MAXJOBS];
-- Coefficient for the Confidence Intervals
-- 99%	2.576
-- 98%	2.326
-- 95%	1.96
-- 90%	1.645
confIntCoeff = [COEFF];

-- -- -- -- -- -- -- -- -- -- -- --
-- -- -- -- supported distributions:
-- -- -- -- -- -- -- -- -- -- -- --
-- -- Type:   "exp"		: Exponential distribution
-- -- Params: rate		: -- rate of the exponential distribution
-- -- --
-- -- -- Example        : {type = "exp", params = {rate = 0.1}}
-- -- -- 				  Generates an exponential random variable with average 10
-- -- -- -- -- -- -- -- -- -- -- --
-- -- Type:   "erl"		: Erlang distribution
-- -- Params: k			: -- number of stages
-- -- 		  lambda	: -- rate of each stage
-- -- --
-- -- -- Example        : {type = "erl", params = {k = 4, lambda = 0.4}}
-- -- -- 				  Generates a four stages Ernalng random variable with average 10, and c.v. = 0.5
-- -- -- -- -- -- -- -- -- -- -- --
-- -- Type:   "det"		: Deterministic distribution
-- -- Params: val		: -- value returned
-- -- --
-- -- -- Example        : {type = "det", params = {val = 10.0}}
-- -- -- 				  Always generates 10
-- -- -- -- -- -- -- -- -- -- -- --
-- -- Type:   "unif"	: Uniform distribution
-- -- Params: min		: -- minimum value
-- -- 		  max		: -- maximum value
-- -- --
-- -- -- Example        : {type = "det", params = {min = 5.0, max = 15.0}}
-- -- -- 				  Generates a uniform random number between 5 and 15 (average 10)
-- -- -- -- -- -- -- -- -- -- -- --
-- -- Type:   "HSE"		 : Hyper Sum of Erlangs: a probabilistic choice of a set of sum of erlang distributions
-- -- Params: {<BLOCK>, ...} : -- an sequence of 1 or more blocks describing a probabilistic choice
-- --             <BLOCK> -> {prob, <STAGE>, ...} -- prob		  : is the probability of the block
-- --											  -- <STAGE>, ... : is a sequence of 1 or more stages
-- --			  <STAGE> -> {[k], lambda} -- The description of an Erlang block:
-- --											  -- [k]	: an optional number of stages. k=1 if missing
-- --											  -- lambda	: the rate of each stage of the corresponding Erlang
--[[
-- -- -- Example        : {type   = "HSE",
						   params = {{0.5,{k = 2, lambda = 0.5},
						   			      {k = 4, lambda = 1}},
						   			 {0.5,{lambda = 1/15.0}}
						   }
						  }
-- -- -- 				  Generates a random variable that:
-- -- --						 50% of the times is the sum of the Erlangs: Erlang(2, 1/2)+Erlang(4, 1)
-- -- --						 50% of the times is exponential, with rate 1/15
-- -- --
-- -- -- The "HSE" distribution can be used also to generate:
-- -- -- 	1) Exponential
-- -- --	2) Erlang
-- -- --	3) Hyper-exponential
-- -- --	4) Hypo-exponential
-- -- --	5) Hyper-Erlang
-- -- -- with a more compact description with respect to a PH type distribution.
-- -- -- Examples:
-- -- -- 	1) Exponential of rate 0.1
-- -- --	   {type   = "HSE", params = {{1.0,{lambda = 0.1}}} }
-- -- -- 	2) Erlang with 4 stages of rate 0.4
-- -- --	   {type   = "HSE", params = {{1.0,{k = 4, lambda = 0.4}}} }
-- -- -- 	3) Hyper-exponential of rate 0.2 (with prob 0.5) and 1/15.0 (with prob 0.5)
-- -- --	   {type   = "HSE", params = {{0.5,{lambda = 0.05}}, {0.5,{lambda = 1/15.0}}} }
-- -- -- 	4) Hypo-exponential of rates 0.5 and 0.125
-- -- --	   {type   = "HSE", params = {{1.0,{lambda = 0.5},{lambda = 0.125}}} }
-- -- -- 	5) Hyper-Erlang of rate 1, 5 stages (with prob 0.5) and 0.2, 3 stages (with prob 0.5)
-- -- --	   {type   = "HSE", params = {{0.5,{k = 5, lambda = 1.0}}, {0.5,{k = 3, lambda = 0.2}}} }
-- -- -- -- -- -- -- -- -- -- -- --
-- -- Type:   "PH"		: Phase Type distribution
-- -- Params: alpha		: -- Initial state probability vector
-- -- 		  Q			: -- transition matrix of the phase-type distribution
--[[
-- -- -- Example        : {type   = "PH",
						   params = {
								alpha = {1.0, 0.0, 0.0},
								Q = {
									 {-0.2,  0.2,  0.0},
									 { 0.0, -0.4,  0.2},
									 { 0.0,  0.0, -0.2}
								}
							  }
							}
]]--
-- -- -- 				  Generates a random number that follows a PH distribution with 3 states.
-- -- --				  Always starst from state 1, goes to state 2 after 1/0.2, then
-- -- --                  It either finish or goes to state 3 after 1/0.4, with both alternatives equally prob.
