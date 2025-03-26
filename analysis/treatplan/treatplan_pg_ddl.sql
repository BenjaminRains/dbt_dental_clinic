-- Table: public.treatplan

-- DROP TABLE IF EXISTS public.treatplan;

CREATE TABLE IF NOT EXISTS public.treatplan
(
    "TreatPlanNum" integer NOT NULL DEFAULT nextval('"treatplan_TreatPlanNum_seq"'::regclass),
    "PatNum" bigint,
    "DateTP" date,
    "Heading" character varying(255) COLLATE pg_catalog."default",
    "Note" text COLLATE pg_catalog."default",
    "Signature" text COLLATE pg_catalog."default",
    "SigIsTopaz" boolean,
    "ResponsParty" bigint,
    "DocNum" bigint,
    "TPStatus" smallint,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "UserNumPresenter" bigint,
    "TPType" smallint,
    "SignaturePractice" text COLLATE pg_catalog."default",
    "DateTSigned" timestamp without time zone,
    "DateTPracticeSigned" timestamp without time zone,
    "SignatureText" character varying(255) COLLATE pg_catalog."default",
    "SignaturePracticeText" character varying(255) COLLATE pg_catalog."default",
    "MobileAppDeviceNum" bigint,
    CONSTRAINT treatplan_pkey PRIMARY KEY ("TreatPlanNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.treatplan
    OWNER to postgres;
-- Index: treatplan_DocNum

-- DROP INDEX IF EXISTS public."treatplan_DocNum";

CREATE INDEX IF NOT EXISTS "treatplan_DocNum"
    ON public.treatplan USING btree
    ("DocNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: treatplan_MobileAppDeviceNum

-- DROP INDEX IF EXISTS public."treatplan_MobileAppDeviceNum";

CREATE INDEX IF NOT EXISTS "treatplan_MobileAppDeviceNum"
    ON public.treatplan USING btree
    ("MobileAppDeviceNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: treatplan_SecUserNumEntry

-- DROP INDEX IF EXISTS public."treatplan_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "treatplan_SecUserNumEntry"
    ON public.treatplan USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: treatplan_UserNumPresenter

-- DROP INDEX IF EXISTS public."treatplan_UserNumPresenter";

CREATE INDEX IF NOT EXISTS "treatplan_UserNumPresenter"
    ON public.treatplan USING btree
    ("UserNumPresenter" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: treatplan_indexPatNum

-- DROP INDEX IF EXISTS public."treatplan_indexPatNum";

CREATE INDEX IF NOT EXISTS "treatplan_indexPatNum"
    ON public.treatplan USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;