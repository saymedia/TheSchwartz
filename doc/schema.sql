CREATE TABLE job (
        jobid           BIGINT UNSIGNED PRIMARY KEY NOT NULL,
        funcname        VARCHAR(255) NOT NULL,
        arg             MEDIUMBLOB,
        uniqkey         VARCHAR(255) NULL,
        insert_time     DATETIME,
        run_after       DATETIME,
        grabbed_until   DATETIME,
        priority        SMALLINT UNSIGNED,
        coalesce        VARCHAR(255),
        UNIQUE(uniqkey)
);

CREATE TABLE note (
        jobid,
        notekey,
        value
)


CREATE TABLE error (


)


CREATE TABLE exitstatus (
        jobid
        status
        completition_time
        delete_after
)