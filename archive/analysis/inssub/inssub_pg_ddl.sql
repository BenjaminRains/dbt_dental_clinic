-- Table: public.inssub

-- DROP TABLE IF EXISTS public.inssub;

CREATE TABLE IF NOT EXISTS public.inssub
(
    "InsSubNum" integer NOT NULL DEFAULT nextval('"inssub_InsSubNum_seq"'::regclass),
    "PlanNum" bigint,
    "Subscriber" bigint,
    "DateEffective" date,
    "DateTerm" date,
    "ReleaseInfo" smallint,
    "AssignBen" smallint,
    "SubscriberID" character varying(255) COLLATE pg_catalog."default",
    "BenefitNotes" text COLLATE pg_catalog."default",
    "SubscNote" text COLLATE pg_catalog."default",
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT inssub_pkey PRIMARY KEY ("InsSubNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.inssub
    OWNER to postgres;
-- Index: inssub_PlanNum

-- DROP INDEX IF EXISTS public."inssub_PlanNum";

CREATE INDEX IF NOT EXISTS "inssub_PlanNum"
    ON public.inssub USING btree
    ("PlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: inssub_SecUserNumEntry

-- DROP INDEX IF EXISTS public."inssub_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "inssub_SecUserNumEntry"
    ON public.inssub USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: inssub_Subscriber

-- DROP INDEX IF EXISTS public."inssub_Subscriber";

CREATE INDEX IF NOT EXISTS "inssub_Subscriber"
    ON public.inssub USING btree
    ("Subscriber" ASC NULLS LAST)
    TABLESPACE pg_default;