-- Table: public.insbluebook

-- DROP TABLE IF EXISTS public.insbluebook;

CREATE TABLE IF NOT EXISTS public.insbluebook
(
    "InsBlueBookNum" integer NOT NULL DEFAULT nextval('"insbluebook_InsBlueBookNum_seq"'::regclass),
    "ProcCodeNum" bigint,
    "CarrierNum" bigint,
    "PlanNum" bigint,
    "GroupNum" character varying(25) COLLATE pg_catalog."default",
    "InsPayAmt" double precision,
    "AllowedOverride" double precision,
    "DateTEntry" timestamp without time zone,
    "ProcNum" bigint,
    "ProcDate" date,
    "ClaimType" character varying(10) COLLATE pg_catalog."default",
    "ClaimNum" bigint,
    CONSTRAINT insbluebook_pkey PRIMARY KEY ("InsBlueBookNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.insbluebook
    OWNER to postgres;
-- Index: insbluebook_CarrierNum

-- DROP INDEX IF EXISTS public."insbluebook_CarrierNum";

CREATE INDEX IF NOT EXISTS "insbluebook_CarrierNum"
    ON public.insbluebook USING btree
    ("CarrierNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insbluebook_ClaimNum

-- DROP INDEX IF EXISTS public."insbluebook_ClaimNum";

CREATE INDEX IF NOT EXISTS "insbluebook_ClaimNum"
    ON public.insbluebook USING btree
    ("ClaimNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insbluebook_PlanNum

-- DROP INDEX IF EXISTS public."insbluebook_PlanNum";

CREATE INDEX IF NOT EXISTS "insbluebook_PlanNum"
    ON public.insbluebook USING btree
    ("PlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insbluebook_ProcCodeNum

-- DROP INDEX IF EXISTS public."insbluebook_ProcCodeNum";

CREATE INDEX IF NOT EXISTS "insbluebook_ProcCodeNum"
    ON public.insbluebook USING btree
    ("ProcCodeNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insbluebook_ProcNum

-- DROP INDEX IF EXISTS public."insbluebook_ProcNum";

CREATE INDEX IF NOT EXISTS "insbluebook_ProcNum"
    ON public.insbluebook USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;