-- Table: public.timeadjust

-- DROP TABLE IF EXISTS public.timeadjust;

CREATE TABLE IF NOT EXISTS public.timeadjust
(
    "TimeAdjustNum" integer NOT NULL DEFAULT nextval('"timeadjust_TimeAdjustNum_seq"'::regclass),
    "EmployeeNum" bigint,
    "TimeEntry" timestamp without time zone,
    "RegHours" time without time zone,
    "OTimeHours" time without time zone,
    "Note" text COLLATE pg_catalog."default",
    "IsAuto" smallint,
    "ClinicNum" bigint,
    "PtoDefNum" bigint DEFAULT 0,
    "PtoHours" time without time zone,
    "IsUnpaidProtectedLeave" smallint DEFAULT 0,
    "SecuUserNumEntry" bigint DEFAULT 0,
    CONSTRAINT timeadjust_pkey PRIMARY KEY ("TimeAdjustNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.timeadjust
    OWNER to postgres;
-- Index: timeadjust_ClinicNum

-- DROP INDEX IF EXISTS public."timeadjust_ClinicNum";

CREATE INDEX IF NOT EXISTS "timeadjust_ClinicNum"
    ON public.timeadjust USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: timeadjust_PtoDefNum

-- DROP INDEX IF EXISTS public."timeadjust_PtoDefNum";

CREATE INDEX IF NOT EXISTS "timeadjust_PtoDefNum"
    ON public.timeadjust USING btree
    ("PtoDefNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: timeadjust_SecuUserNumEntry

-- DROP INDEX IF EXISTS public."timeadjust_SecuUserNumEntry";

CREATE INDEX IF NOT EXISTS "timeadjust_SecuUserNumEntry"
    ON public.timeadjust USING btree
    ("SecuUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: timeadjust_indexEmployeeNum

-- DROP INDEX IF EXISTS public."timeadjust_indexEmployeeNum";

CREATE INDEX IF NOT EXISTS "timeadjust_indexEmployeeNum"
    ON public.timeadjust USING btree
    ("EmployeeNum" ASC NULLS LAST)
    TABLESPACE pg_default;