
CREATE TABLE job (
        jobid           BIGINT UNSIGNED PRIMARY KEY NOT NULL,
        funcname
        args
        uniqkey         VARCHAR(255) NULL,
        insert_time
        run_after
        grabbed_until
        priority
        coalescing
        UNIQUE(uniqkey),
)

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
        delete_after
)