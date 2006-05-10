CREATE TABLE job (
        jobid           BIGINT UNSIGNED PRIMARY KEY NOT NULL,
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

CREATE TABLE note (
        jobid           BIGINT UNSIGNED NOT NULL,
        notekey         VARCHAR(255),
        PRIMARY KEY (jobid, notekey),
        value           MEDIUMBLOB
);

CREATE TABLE error (
        jobid           BIGINT UNSIGNED NOT NULL,
        message         VARCHAR(255) NOT NULL
);

CREATE TABLE exitstatus (
        jobid           BIGINT UNSIGNED PRIMARY KEY NOT NULL,
        status          SMALLINT UNSIGNED,
        completion_time INTEGER UNSIGNED,
        delete_after    INTEGER UNSIGNED
);
