-- Table: public.histappointment

-- DROP TABLE IF EXISTS public.histappointment;

CREATE TABLE IF NOT EXISTS public.histappointment
(
    "HistApptNum" integer NOT NULL DEFAULT nextval('"histappointment_HistApptNum_seq"'::regclass),
    "HistUserNum" bigint,
    "HistDateTStamp" timestamp without time zone,
    "HistApptAction" smallint,
    "ApptSource" smallint,
    "AptNum" bigint,
    "PatNum" bigint,
    "AptStatus" smallint,
    "Pattern" character varying(255) COLLATE pg_catalog."default",
    "Confirmed" bigint,
    "TimeLocked" smallint,
    "Op" bigint,
    "Note" text COLLATE pg_catalog."default",
    "ProvNum" bigint,
    "ProvHyg" bigint,
    "AptDateTime" timestamp without time zone,
    "NextAptNum" bigint,
    "UnschedStatus" bigint,
    "IsNewPatient" smallint,
    "ProcDescript" character varying(255) COLLATE pg_catalog."default",
    "Assistant" bigint,
    "ClinicNum" bigint,
    "IsHygiene" smallint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "DateTimeArrived" timestamp without time zone,
    "DateTimeSeated" timestamp without time zone,
    "DateTimeDismissed" timestamp without time zone,
    "InsPlan1" bigint,
    "InsPlan2" bigint,
    "DateTimeAskedToArrive" timestamp without time zone,
    "ProcsColored" text COLLATE pg_catalog."default",
    "ColorOverride" integer,
    "AppointmentTypeNum" bigint,
    "SecUserNumEntry" bigint,
    "SecDateTEntry" timestamp without time zone,
    "Priority" smallint,
    "ProvBarText" character varying(60) COLLATE pg_catalog."default",
    "PatternSecondary" character varying(255) COLLATE pg_catalog."default",
    "SecurityHash" character varying(255) COLLATE pg_catalog."default",
    "ItemOrderPlanned" integer,
    CONSTRAINT histappointment_pkey PRIMARY KEY ("HistApptNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.histappointment
    OWNER to postgres;
-- Index: histappointment_AppointmentTypeNum

-- DROP INDEX IF EXISTS public."histappointment_AppointmentTypeNum";

CREATE INDEX IF NOT EXISTS "histappointment_AppointmentTypeNum"
    ON public.histappointment USING btree
    ("AppointmentTypeNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_AptNum

-- DROP INDEX IF EXISTS public."histappointment_AptNum";

CREATE INDEX IF NOT EXISTS "histappointment_AptNum"
    ON public.histappointment USING btree
    ("AptNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_Assistant

-- DROP INDEX IF EXISTS public."histappointment_Assistant";

CREATE INDEX IF NOT EXISTS "histappointment_Assistant"
    ON public.histappointment USING btree
    ("Assistant" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_ClinicNum

-- DROP INDEX IF EXISTS public."histappointment_ClinicNum";

CREATE INDEX IF NOT EXISTS "histappointment_ClinicNum"
    ON public.histappointment USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_Confirmed

-- DROP INDEX IF EXISTS public."histappointment_Confirmed";

CREATE INDEX IF NOT EXISTS "histappointment_Confirmed"
    ON public.histappointment USING btree
    ("Confirmed" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_HistDateTStamp

-- DROP INDEX IF EXISTS public."histappointment_HistDateTStamp";

CREATE INDEX IF NOT EXISTS "histappointment_HistDateTStamp"
    ON public.histappointment USING btree
    ("HistDateTStamp" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_HistUserNum

-- DROP INDEX IF EXISTS public."histappointment_HistUserNum";

CREATE INDEX IF NOT EXISTS "histappointment_HistUserNum"
    ON public.histappointment USING btree
    ("HistUserNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_InsPlan1

-- DROP INDEX IF EXISTS public."histappointment_InsPlan1";

CREATE INDEX IF NOT EXISTS "histappointment_InsPlan1"
    ON public.histappointment USING btree
    ("InsPlan1" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_InsPlan2

-- DROP INDEX IF EXISTS public."histappointment_InsPlan2";

CREATE INDEX IF NOT EXISTS "histappointment_InsPlan2"
    ON public.histappointment USING btree
    ("InsPlan2" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_NextAptNum

-- DROP INDEX IF EXISTS public."histappointment_NextAptNum";

CREATE INDEX IF NOT EXISTS "histappointment_NextAptNum"
    ON public.histappointment USING btree
    ("NextAptNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_Op

-- DROP INDEX IF EXISTS public."histappointment_Op";

CREATE INDEX IF NOT EXISTS "histappointment_Op"
    ON public.histappointment USING btree
    ("Op" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_PatNum

-- DROP INDEX IF EXISTS public."histappointment_PatNum";

CREATE INDEX IF NOT EXISTS "histappointment_PatNum"
    ON public.histappointment USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_ProvHyg

-- DROP INDEX IF EXISTS public."histappointment_ProvHyg";

CREATE INDEX IF NOT EXISTS "histappointment_ProvHyg"
    ON public.histappointment USING btree
    ("ProvHyg" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_ProvNum

-- DROP INDEX IF EXISTS public."histappointment_ProvNum";

CREATE INDEX IF NOT EXISTS "histappointment_ProvNum"
    ON public.histappointment USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_SecUserNumEntry

-- DROP INDEX IF EXISTS public."histappointment_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "histappointment_SecUserNumEntry"
    ON public.histappointment USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: histappointment_UnschedStatus

-- DROP INDEX IF EXISTS public."histappointment_UnschedStatus";

CREATE INDEX IF NOT EXISTS "histappointment_UnschedStatus"
    ON public.histappointment USING btree
    ("UnschedStatus" ASC NULLS LAST)
    TABLESPACE pg_default;