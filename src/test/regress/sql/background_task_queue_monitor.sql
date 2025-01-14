CREATE SCHEMA background_task_queue_monitor;
SET search_path TO background_task_queue_monitor;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 3536400;

ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
SELECT pg_reload_conf();

CREATE TABLE results (a int);

-- simple job that inserts 1 into results to show that query runs
SELECT a FROM results WHERE a = 1; -- verify result is not in there
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify background execution') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ INSERT INTO background_task_queue_monitor.results VALUES ( 1 ); $job$) RETURNING task_id \gset
SELECT citus_job_wait(:job_id); -- wait for the job to be finished
SELECT a FROM results WHERE a = 1; -- verify result is there

-- cancel a scheduled job
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job','cancel a scheduled job') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id \gset

SELECT citus_job_cancel(:job_id);
SELECT citus_job_wait(:job_id);

-- verify we get an error when waiting for a job to reach a specific status while it is already in a different terminal status
SELECT citus_job_wait(:job_id, desired_status => 'finished');

-- show that the status has been cancelled
SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message = '') AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- cancel a running job
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job','cancelling a task after it started') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id \gset

SELECT citus_job_wait(:job_id, desired_status => 'running');
SELECT citus_job_cancel(:job_id);
SELECT citus_job_wait(:job_id);

-- show that the status has been cancelled
SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message = '') AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- test a failing task becomes runnable in the future again
-- we cannot fully test the backoff strategy currently as it is hard coded to take about 50 minutes.
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'failure test due to division by zero') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ SELECT 1/0; $job$) RETURNING task_id \gset

SELECT citus_job_wait(:job_id, desired_status => 'running');
SELECT pg_sleep(.1); -- make sure it has time to error after it started running

SELECT status, pid, retry_count, NOT(message = '') AS has_message, (not_before > now()) AS scheduled_into_the_future FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- test cancelling a failed/retrying job
SELECT citus_job_cancel(:job_id);
SELECT citus_job_wait(:job_id);

SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message = '') AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- test running two dependant tasks
TRUNCATE TABLE results;
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify background execution') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ INSERT INTO background_task_queue_monitor.results VALUES ( 5 ); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_task (job_id, status, command) VALUES (:job_id, 'blocked', $job$ UPDATE background_task_queue_monitor.results SET a = a * 7; $job$) RETURNING task_id AS task_id2 \gset
INSERT INTO pg_dist_background_task (job_id, status, command) VALUES (:job_id, 'blocked', $job$ UPDATE background_task_queue_monitor.results SET a = a + 13; $job$) RETURNING task_id AS task_id3 \gset
INSERT INTO pg_dist_background_task_depend (job_id, task_id, depends_on) VALUES (:job_id, :task_id2, :task_id1);
INSERT INTO pg_dist_background_task_depend (job_id, task_id, depends_on) VALUES (:job_id, :task_id3, :task_id2);
COMMIT;

SELECT citus_job_wait(:job_id); -- wait for the job to be finished
SELECT a FROM results;

-- test running two dependant tasks, with a failing task that we cancel
TRUNCATE TABLE results;
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify background execution') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ INSERT INTO background_task_queue_monitor.results VALUES ( 5 ); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_task (job_id, status, command) VALUES (:job_id, 'blocked', $job$ SELECT 1/0; $job$) RETURNING task_id AS task_id2 \gset
INSERT INTO pg_dist_background_task (job_id, status, command) VALUES (:job_id, 'blocked', $job$ UPDATE background_task_queue_monitor.results SET a = a + 13; $job$) RETURNING task_id AS task_id3 \gset
INSERT INTO pg_dist_background_task_depend (job_id, task_id, depends_on) VALUES (:job_id, :task_id2, :task_id1);
INSERT INTO pg_dist_background_task_depend (job_id, task_id, depends_on) VALUES (:job_id, :task_id3, :task_id2);
COMMIT;

SELECT citus_job_wait(:job_id, desired_status => 'running'); -- wait for the job to be running, and possibly hitting a failure
SELECT pg_sleep(.1); -- improve chances of hitting the failure

SELECT citus_job_cancel(:job_id);
SELECT citus_job_wait(:job_id); -- wait for the job to be cancelled
SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message = '') AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- verify that we do not allow parallel task executors more than citus.max_background_task_executors(4 by default)
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id3 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id2, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id4 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id3 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id3, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id5 \gset
COMMIT;

SELECT pg_sleep(2); -- we assume this is enough time for all tasks to be in running status except the last one due to parallel worker limit

SELECT job_id, task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY job_id, task_id; -- show that last task is not running but ready to run(runnable)

SELECT citus_job_cancel(:job_id2); -- when a job with 1 task is cancelled, the last runnable task will be running
SELECT citus_job_wait(:job_id2); -- wait for the job to be cancelled
SELECT citus_job_wait(:job_id3, desired_status => 'running');
SELECT job_id, task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY job_id, task_id;  -- show that last task is running

SELECT citus_job_cancel(:job_id1);
SELECT citus_job_cancel(:job_id3);
SELECT citus_job_wait(:job_id1);
SELECT citus_job_wait(:job_id3);
SELECT job_id, task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY job_id, task_id;  -- show that multiple cancels worked


-- verify that a task, previously not started due to lack of workers, is executed after we increase max worker count
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id3 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id2, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id4 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id3 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id3, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id5 \gset
COMMIT;

SELECT pg_sleep(2); -- we assume this is enough time for all tasks to be in running status except the last one due to parallel worker limit

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY task_id; -- show that last task is not running but ready to run(runnable)

ALTER SYSTEM SET citus.max_background_task_executors TO 5;
SELECT pg_reload_conf(); -- the last runnable task will be running after change
SELECT citus_job_wait(:job_id3, desired_status => 'running');
SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY task_id;  -- show that last task is running

SELECT citus_job_cancel(:job_id1);
SELECT citus_job_cancel(:job_id2);
SELECT citus_job_cancel(:job_id3);
SELECT citus_job_wait(:job_id1);
SELECT citus_job_wait(:job_id2);
SELECT citus_job_wait(:job_id3);

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY task_id; -- show that all tasks are cancelled

-- verify that upon termination signal, all tasks fail and retry policy sets their status back to runnable
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify termination on monitor') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(500); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify termination on monitor') RETURNING job_id AS job_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id2, $job$ SELECT pg_sleep(500); $job$) RETURNING task_id AS task_id2 \gset
COMMIT;

SELECT citus_job_wait(:job_id1, desired_status => 'running');
SELECT citus_job_wait(:job_id2, desired_status => 'running');

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY task_id;


SELECT pid AS monitor_pid FROM pg_stat_activity WHERE application_name ~ 'task queue monitor' \gset
SELECT pg_terminate_backend(:monitor_pid); -- terminate monitor process

SELECT pg_sleep(2); -- wait enough to show that tasks are terminated

SELECT task_id, status, retry_count, message FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY task_id; -- show that all tasks are runnable by retry policy after termination signal

SELECT citus_job_cancel(:job_id1);
SELECT citus_job_cancel(:job_id2);
SELECT citus_job_wait(:job_id1);
SELECT citus_job_wait(:job_id2);

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY task_id; -- show that all tasks are cancelled

-- verify that upon cancellation signal, all tasks are cancelled
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify cancellation on monitor') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(500); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify cancellation on monitor') RETURNING job_id AS job_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id2, $job$ SELECT pg_sleep(500); $job$) RETURNING task_id AS task_id2 \gset
COMMIT;

SELECT citus_job_wait(:job_id1, desired_status => 'running');
SELECT citus_job_wait(:job_id2, desired_status => 'running');

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY task_id;


SELECT pid AS monitor_pid FROM pg_stat_activity WHERE application_name ~ 'task queue monitor' \gset
SELECT pg_cancel_backend(:monitor_pid); -- cancel monitor process

SELECT pg_sleep(2); -- wait enough to show that tasks are cancelled

SELECT citus_job_wait(:job_id1);
SELECT citus_job_wait(:job_id2);

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY task_id; -- show that all tasks are cancelled

-- verify that task is not starved by currently long running task
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify task execution starvation') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5000); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify task execution starvation') RETURNING job_id AS job_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id2, $job$ SELECT 1; $job$) RETURNING task_id AS task_id2 \gset
COMMIT;

SELECT citus_job_wait(:job_id1, desired_status => 'running');
SELECT citus_job_wait(:job_id2, desired_status => 'finished');
SELECT job_id, task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY job_id, task_id;  -- show that last task is finished without starvation
SELECT citus_job_cancel(:job_id1);
SELECT citus_job_wait(:job_id1);

SELECT job_id, task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY job_id, task_id;  -- show that task is cancelled

SET client_min_messages TO WARNING;
DROP SCHEMA background_task_queue_monitor CASCADE;

ALTER SYSTEM RESET citus.background_task_queue_interval;
SELECT pg_reload_conf();
