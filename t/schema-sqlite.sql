CREATE TABLE job (
        jobid           INTEGER PRIMARY KEY NOT NULL,
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

CREATE TABLE error(
        jobid           INTEGER NOT NULL,
        message         VARCHAR(255) NOT NULL
);
