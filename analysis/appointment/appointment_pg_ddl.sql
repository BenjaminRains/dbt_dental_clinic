-- Table: public.appointment

-- DROP TABLE IF EXISTS public.appointment;

CREATE TABLE IF NOT EXISTS public.appointment
(
    "AptNum" integer NOT NULL DEFAULT nextval('"appointment_AptNum_seq"'::regclass),
    "PatNum" bigint,
    "AptStatus" smallint DEFAULT 0,
    "Pattern" character varying(255) COLLATE pg_catalog."default",
    "Confirmed" bigint,
    "TimeLocked" boolean,
    "Op" bigint,
    "Note" text COLLATE pg_catalog."default",
    "ProvNum" bigint,
    "ProvHyg" bigint,
    "AptDateTime" timestamp without time zone,
    "NextAptNum" bigint,
    "UnschedStatus" bigint,
    "IsNewPatient" smallint DEFAULT 0,
    "ProcDescript" text COLLATE pg_catalog."default",
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
    CONSTRAINT appointment_pkey PRIMARY KEY ("AptNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.appointment
    OWNER to postgres;
-- Index: appointment_AppointmentTypeNum

-- DROP INDEX IF EXISTS public."appointment_AppointmentTypeNum";

CREATE INDEX IF NOT EXISTS "appointment_AppointmentTypeNum"
    ON public.appointment USING btree
    ("AppointmentTypeNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_ClinicNum

-- DROP INDEX IF EXISTS public."appointment_ClinicNum";

CREATE INDEX IF NOT EXISTS "appointment_ClinicNum"
    ON public.appointment USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_DateTStamp

-- DROP INDEX IF EXISTS public."appointment_DateTStamp";

CREATE INDEX IF NOT EXISTS "appointment_DateTStamp"
    ON public.appointment USING btree
    ("DateTStamp" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_DateTimeArrived

-- DROP INDEX IF EXISTS public."appointment_DateTimeArrived";

CREATE INDEX IF NOT EXISTS "appointment_DateTimeArrived"
    ON public.appointment USING btree
    ("DateTimeArrived" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_InsPlan1

-- DROP INDEX IF EXISTS public."appointment_InsPlan1";

CREATE INDEX IF NOT EXISTS "appointment_InsPlan1"
    ON public.appointment USING btree
    ("InsPlan1" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_InsPlan2

-- DROP INDEX IF EXISTS public."appointment_InsPlan2";

CREATE INDEX IF NOT EXISTS "appointment_InsPlan2"
    ON public.appointment USING btree
    ("InsPlan2" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_Op

-- DROP INDEX IF EXISTS public."appointment_Op";

CREATE INDEX IF NOT EXISTS "appointment_Op"
    ON public.appointment USING btree
    ("Op" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_Priority

-- DROP INDEX IF EXISTS public."appointment_Priority";

CREATE INDEX IF NOT EXISTS "appointment_Priority"
    ON public.appointment USING btree
    ("Priority" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_SecUserNumEntry

-- DROP INDEX IF EXISTS public."appointment_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "appointment_SecUserNumEntry"
    ON public.appointment USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_StatusDate

-- DROP INDEX IF EXISTS public."appointment_StatusDate";

CREATE INDEX IF NOT EXISTS "appointment_StatusDate"
    ON public.appointment USING btree
    ("AptStatus" ASC NULLS LAST, "AptDateTime" ASC NULLS LAST, "ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_UnschedStatus

-- DROP INDEX IF EXISTS public."appointment_UnschedStatus";

CREATE INDEX IF NOT EXISTS "appointment_UnschedStatus"
    ON public.appointment USING btree
    ("UnschedStatus" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_indexAptDateTime

-- DROP INDEX IF EXISTS public."appointment_indexAptDateTime";

CREATE INDEX IF NOT EXISTS "appointment_indexAptDateTime"
    ON public.appointment USING btree
    ("AptDateTime" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_indexNextAptNum

-- DROP INDEX IF EXISTS public."appointment_indexNextAptNum";

CREATE INDEX IF NOT EXISTS "appointment_indexNextAptNum"
    ON public.appointment USING btree
    ("NextAptNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_indexPatNum

-- DROP INDEX IF EXISTS public."appointment_indexPatNum";

CREATE INDEX IF NOT EXISTS "appointment_indexPatNum"
    ON public.appointment USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_indexProvHyg

-- DROP INDEX IF EXISTS public."appointment_indexProvHyg";

CREATE INDEX IF NOT EXISTS "appointment_indexProvHyg"
    ON public.appointment USING btree
    ("ProvHyg" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: appointment_indexProvNum

-- DROP INDEX IF EXISTS public."appointment_indexProvNum";

CREATE INDEX IF NOT EXISTS "appointment_indexProvNum"
    ON public.appointment USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;