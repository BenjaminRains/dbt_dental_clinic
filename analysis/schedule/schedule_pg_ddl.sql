-- Table: public.schedule

-- DROP TABLE IF EXISTS public.schedule;

CREATE TABLE IF NOT EXISTS public.schedule
(
    "ScheduleNum" integer NOT NULL DEFAULT nextval('"schedule_ScheduleNum_seq"'::regclass),
    "SchedDate" date,
    "StartTime" time without time zone,
    "StopTime" time without time zone,
    "SchedType" smallint DEFAULT 0,
    "ProvNum" bigint,
    "BlockoutType" bigint,
    "Note" text COLLATE pg_catalog."default",
    "Status" smallint DEFAULT 0,
    "EmployeeNum" bigint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "ClinicNum" bigint,
    CONSTRAINT schedule_pkey PRIMARY KEY ("ScheduleNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.schedule
    OWNER to postgres;
-- Index: schedule_BlockoutType

-- DROP INDEX IF EXISTS public."schedule_BlockoutType";

CREATE INDEX IF NOT EXISTS "schedule_BlockoutType"
    ON public.schedule USING btree
    ("BlockoutType" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: schedule_ClinicNumSchedType

-- DROP INDEX IF EXISTS public."schedule_ClinicNumSchedType";

CREATE INDEX IF NOT EXISTS "schedule_ClinicNumSchedType"
    ON public.schedule USING btree
    ("ClinicNum" ASC NULLS LAST, "SchedType" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: schedule_EmpDateTypeStopTime

-- DROP INDEX IF EXISTS public."schedule_EmpDateTypeStopTime";

CREATE INDEX IF NOT EXISTS "schedule_EmpDateTypeStopTime"
    ON public.schedule USING btree
    ("EmployeeNum" ASC NULLS LAST, "SchedDate" ASC NULLS LAST, "SchedType" ASC NULLS LAST, "StopTime" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: schedule_ProvNum

-- DROP INDEX IF EXISTS public."schedule_ProvNum";

CREATE INDEX IF NOT EXISTS "schedule_ProvNum"
    ON public.schedule USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: schedule_SchedDate

-- DROP INDEX IF EXISTS public."schedule_SchedDate";

CREATE INDEX IF NOT EXISTS "schedule_SchedDate"
    ON public.schedule USING btree
    ("SchedDate" ASC NULLS LAST)
    TABLESPACE pg_default;