-- From: Michael Zedeler <michael@zedeler.dk>
-- Date: July 30, 2007 7:31:55 AM PDT
-- To: cpan@sixapart.com
-- Subject: TheSchwartz database schema for postgresql
--
-- Hi.
--
-- I couldn't find any useful postgresql compatible schema file for  
-- the tables that TheSchwartz seems to depend on, so I rewrote the  
-- one supplied in the package.
--
-- Here it is. Feel free to include it in the next release.
--
-- Regards,
--
-- Michael.


CREATE TABLE funcmap (
        funcid SERIAL,
        funcname       VARCHAR(255) NOT NULL,
        UNIQUE(funcname)
);

CREATE TABLE job (
        jobid           SERIAL,
        funcid          INT NOT NULL,
        arg             BYTEA,
        uniqkey         VARCHAR(255) NULL,
        insert_time     INTEGER,
        run_after       INTEGER NOT NULL,
        grabbed_until   INTEGER NOT NULL,
        priority        SMALLINT,
        coalesce        VARCHAR(255)
);

CREATE UNIQUE INDEX job_funcid_uniqkey ON job (funcid, uniqkey);

CREATE INDEX job_funcid_runafter ON job (funcid, run_after);

CREATE INDEX job_funcid_coalesce ON job (funcid, coalesce);

CREATE TABLE note (
        jobid           BIGINT NOT NULL,
        notekey         VARCHAR(255),
        PRIMARY KEY (jobid, notekey),
        value           BYTEA
);

CREATE TABLE error (
        error_time      INTEGER NOT NULL,
        jobid           BIGINT NOT NULL,
        message         VARCHAR(255) NOT NULL,
        funcid          INT NOT NULL DEFAULT 0
);

CREATE INDEX error_funcid_errortime ON error (funcid, error_time);
CREATE INDEX error_time ON error (error_time);
CREATE INDEX error_jobid ON error (jobid);

CREATE TABLE exitstatus (
        jobid           BIGINT PRIMARY KEY NOT NULL,
        funcid          INT NOT NULL DEFAULT 0,
        status          SMALLINT,
        completion_time INTEGER,
        delete_after    INTEGER
);

CREATE INDEX exitstatus_funcid ON exitstatus (funcid);
CREATE INDEX exitstatus_deleteafter ON exitstatus (delete_after);

