-- Table: public.procedurecode

-- DROP TABLE IF EXISTS public.procedurecode;

CREATE TABLE IF NOT EXISTS public.procedurecode
(
    "CodeNum" integer NOT NULL DEFAULT nextval('"procedurecode_CodeNum_seq"'::regclass),
    "ProcCode" character varying(15) COLLATE pg_catalog."default",
    "Descript" character varying(255) COLLATE pg_catalog."default",
    "AbbrDesc" character varying(50) COLLATE pg_catalog."default",
    "ProcTime" character varying(24) COLLATE pg_catalog."default",
    "ProcCat" bigint,
    "TreatArea" smallint DEFAULT 0,
    "NoBillIns" smallint DEFAULT 0,
    "IsProsth" smallint DEFAULT 0,
    "DefaultNote" text COLLATE pg_catalog."default",
    "IsHygiene" smallint DEFAULT 0,
    "GTypeNum" smallint DEFAULT 0,
    "AlternateCode1" character varying(15) COLLATE pg_catalog."default",
    "MedicalCode" character varying(15) COLLATE pg_catalog."default",
    "IsTaxed" smallint,
    "PaintType" smallint,
    "GraphicColor" integer,
    "LaymanTerm" character varying(255) COLLATE pg_catalog."default",
    "IsCanadianLab" smallint,
    "PreExisting" boolean DEFAULT false,
    "BaseUnits" integer,
    "SubstitutionCode" character varying(25) COLLATE pg_catalog."default",
    "SubstOnlyIf" integer,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "IsMultiVisit" smallint,
    "DrugNDC" character varying(255) COLLATE pg_catalog."default",
    "RevenueCodeDefault" character varying(255) COLLATE pg_catalog."default",
    "ProvNumDefault" bigint,
    "CanadaTimeUnits" double precision,
    "IsRadiology" smallint,
    "DefaultClaimNote" text COLLATE pg_catalog."default",
    "DefaultTPNote" text COLLATE pg_catalog."default",
    "BypassGlobalLock" smallint,
    "TaxCode" character varying(16) COLLATE pg_catalog."default",
    "PaintText" character varying(255) COLLATE pg_catalog."default",
    "AreaAlsoToothRange" smallint,
    "DiagnosticCodes" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT procedurecode_pkey PRIMARY KEY ("CodeNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.procedurecode
    OWNER to postgres;
-- Index: procedurecode_ProcCode

-- DROP INDEX IF EXISTS public."procedurecode_ProcCode";

CREATE INDEX IF NOT EXISTS "procedurecode_ProcCode"
    ON public.procedurecode USING btree
    ("ProcCode" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procedurecode_ProvNumDefault

-- DROP INDEX IF EXISTS public."procedurecode_ProvNumDefault";

CREATE INDEX IF NOT EXISTS "procedurecode_ProvNumDefault"
    ON public.procedurecode USING btree
    ("ProvNumDefault" ASC NULLS LAST)
    TABLESPACE pg_default;