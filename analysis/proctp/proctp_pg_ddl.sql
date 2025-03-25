-- Table: public.proctp

-- DROP TABLE IF EXISTS public.proctp;

CREATE TABLE IF NOT EXISTS public.proctp
(
    "ProcTPNum" integer NOT NULL DEFAULT nextval('"proctp_ProcTPNum_seq"'::regclass),
    "TreatPlanNum" bigint,
    "PatNum" bigint,
    "ProcNumOrig" bigint,
    "ItemOrder" smallint,
    "Priority" bigint,
    "ToothNumTP" character varying(255) COLLATE pg_catalog."default",
    "Surf" character varying(255) COLLATE pg_catalog."default",
    "ProcCode" character varying(15) COLLATE pg_catalog."default",
    "Descript" character varying(255) COLLATE pg_catalog."default",
    "FeeAmt" double precision,
    "PriInsAmt" double precision,
    "SecInsAmt" double precision,
    "PatAmt" double precision,
    "Discount" double precision,
    "Prognosis" character varying(255) COLLATE pg_catalog."default",
    "Dx" character varying(255) COLLATE pg_catalog."default",
    "ProcAbbr" character varying(50) COLLATE pg_catalog."default",
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "FeeAllowed" double precision,
    "TaxAmt" double precision,
    "ProvNum" bigint,
    "DateTP" date,
    "ClinicNum" bigint,
    "CatPercUCR" double precision,
    CONSTRAINT proctp_pkey PRIMARY KEY ("ProcTPNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.proctp
    OWNER to postgres;
-- Index: proctp_ClinicNum

-- DROP INDEX IF EXISTS public."proctp_ClinicNum";

CREATE INDEX IF NOT EXISTS "proctp_ClinicNum"
    ON public.proctp USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: proctp_ProcNumOrig

-- DROP INDEX IF EXISTS public."proctp_ProcNumOrig";

CREATE INDEX IF NOT EXISTS "proctp_ProcNumOrig"
    ON public.proctp USING btree
    ("ProcNumOrig" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: proctp_ProvNum

-- DROP INDEX IF EXISTS public."proctp_ProvNum";

CREATE INDEX IF NOT EXISTS "proctp_ProvNum"
    ON public.proctp USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: proctp_SecUserNumEntry

-- DROP INDEX IF EXISTS public."proctp_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "proctp_SecUserNumEntry"
    ON public.proctp USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: proctp_indexPatNum

-- DROP INDEX IF EXISTS public."proctp_indexPatNum";

CREATE INDEX IF NOT EXISTS "proctp_indexPatNum"
    ON public.proctp USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: proctp_indexTreatPlanNum

-- DROP INDEX IF EXISTS public."proctp_indexTreatPlanNum";

CREATE INDEX IF NOT EXISTS "proctp_indexTreatPlanNum"
    ON public.proctp USING btree
    ("TreatPlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;