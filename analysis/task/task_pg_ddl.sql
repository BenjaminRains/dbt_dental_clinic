-- Table: public.task

-- DROP TABLE IF EXISTS public.task;

CREATE TABLE IF NOT EXISTS public.task
(
    "TaskNum" integer NOT NULL DEFAULT nextval('"task_TaskNum_seq"'::regclass),
    "TaskListNum" bigint,
    "DateTask" date,
    "KeyNum" bigint,
    "Descript" text COLLATE pg_catalog."default",
    "TaskStatus" smallint,
    "IsRepeating" smallint,
    "DateType" smallint,
    "FromNum" bigint,
    "ObjectType" smallint,
    "DateTimeEntry" timestamp without time zone,
    "UserNum" bigint,
    "DateTimeFinished" timestamp without time zone,
    "PriorityDefNum" bigint,
    "ReminderGroupId" character varying(20) COLLATE pg_catalog."default",
    "ReminderType" smallint,
    "ReminderFrequency" integer,
    "DateTimeOriginal" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "DescriptOverride" character varying(255) COLLATE pg_catalog."default",
    "IsReadOnly" boolean DEFAULT false,
    "TriageCategory" bigint,
    CONSTRAINT task_pkey PRIMARY KEY ("TaskNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.task
    OWNER to postgres;
-- Index: task_DateTimeOriginal

-- DROP INDEX IF EXISTS public."task_DateTimeOriginal";

CREATE INDEX IF NOT EXISTS "task_DateTimeOriginal"
    ON public.task USING btree
    ("DateTimeOriginal" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: task_KeyNum

-- DROP INDEX IF EXISTS public."task_KeyNum";

CREATE INDEX IF NOT EXISTS "task_KeyNum"
    ON public.task USING btree
    ("KeyNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: task_PriorityDefNum

-- DROP INDEX IF EXISTS public."task_PriorityDefNum";

CREATE INDEX IF NOT EXISTS "task_PriorityDefNum"
    ON public.task USING btree
    ("PriorityDefNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: task_SecDateTEdit

-- DROP INDEX IF EXISTS public."task_SecDateTEdit";

CREATE INDEX IF NOT EXISTS "task_SecDateTEdit"
    ON public.task USING btree
    ("SecDateTEdit" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: task_TaskStatus

-- DROP INDEX IF EXISTS public."task_TaskStatus";

CREATE INDEX IF NOT EXISTS "task_TaskStatus"
    ON public.task USING btree
    ("TaskStatus" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: task_TriageCategory

-- DROP INDEX IF EXISTS public."task_TriageCategory";

CREATE INDEX IF NOT EXISTS "task_TriageCategory"
    ON public.task USING btree
    ("TriageCategory" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: task_UserNum

-- DROP INDEX IF EXISTS public."task_UserNum";

CREATE INDEX IF NOT EXISTS "task_UserNum"
    ON public.task USING btree
    ("UserNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: task_indexTaskListNum

-- DROP INDEX IF EXISTS public."task_indexTaskListNum";

CREATE INDEX IF NOT EXISTS "task_indexTaskListNum"
    ON public.task USING btree
    ("TaskListNum" ASC NULLS LAST)
    TABLESPACE pg_default;