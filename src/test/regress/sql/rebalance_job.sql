CREATE SCHEMA rebalance_job;
SET search_path TO rebalance_job;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 3536400;

CREATE TABLE results (a int);

-- simple job that inserts 1 into results to show that query runs
SELECT a FROM results WHERE a = 1; -- verify result is not in there
INSERT INTO pg_dist_rebalance_jobs (command) VALUES ($job$ INSERT INTO rebalance_job.results VALUES ( 1 ); $job$) RETURNING jobid \gset
SELECT citus_wait_for_rebalance_job(:jobid); -- wait for the job to be finished
SELECT a FROM results WHERE a = 1; -- verify result is there

SET client_min_messages TO WARNING;
DROP SCHEMA rebalance_job CASCADE;
