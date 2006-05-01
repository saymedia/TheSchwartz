CREATE TABLE job (
        jobid           INTEGER PRIMARY KEY NOT NULL,
        funcname        VARCHAR(255) NOT NULL,
        arg             MEDIUMBLOB,
        uniqkey         VARCHAR(255) NULL,
        insert_time     INTEGER UNSIGNED,
        run_after       INTEGER UNSIGNED,
        grabbed_until   INTEGER UNSIGNED,
        priority        SMALLINT UNSIGNED,
        coalesce        VARCHAR(255),
        UNIQUE(uniqkey)
);

CREATE TABLE error (
        jobid           INTEGER NOT NULL,
        message         VARCHAR(255) NOT NULL
);

CREATE TABLE exitstatus (
        jobid           BIGINT UNSIGNED NOT NULL,
        status          SMALLINT UNSIGNED,
        completion_time INTEGER UNSIGNED,
        delete_after    INTEGER UNSIGNED
);
