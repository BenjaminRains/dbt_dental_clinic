-- Table: public.scheduleop

-- DROP TABLE IF EXISTS public.scheduleop;

CREATE TABLE IF NOT EXISTS public.scheduleop
(
    "ScheduleOpNum" integer NOT NULL DEFAULT nextval('"scheduleop_ScheduleOpNum_seq"'::regclass),
    "ScheduleNum" bigint,
    "OperatoryNum" bigint,
    CONSTRAINT scheduleop_pkey PRIMARY KEY ("ScheduleOpNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.scheduleop
    OWNER to postgres;
-- Index: scheduleop_OperatoryNum

-- DROP INDEX IF EXISTS public."scheduleop_OperatoryNum";

CREATE INDEX IF NOT EXISTS "scheduleop_OperatoryNum"
    ON public.scheduleop USING btree
    ("OperatoryNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: scheduleop_ScheduleNum

-- DROP INDEX IF EXISTS public."scheduleop_ScheduleNum";

CREATE INDEX IF NOT EXISTS "scheduleop_ScheduleNum"
    ON public.scheduleop USING btree
    ("ScheduleNum" ASC NULLS LAST)
    TABLESPACE pg_default;