-- Table: public.fee

-- DROP TABLE IF EXISTS public.fee;

CREATE TABLE IF NOT EXISTS public.fee
(
    "FeeNum" integer NOT NULL DEFAULT nextval('"fee_FeeNum_seq"'::regclass),
    "Amount" double precision DEFAULT 0,
    "OldCode" character varying(15) COLLATE pg_catalog."default",
    "FeeSched" bigint,
    "UseDefaultFee" smallint DEFAULT 0,
    "UseDefaultCov" smallint DEFAULT 0,
    "CodeNum" bigint,
    "ClinicNum" bigint,
    "ProvNum" bigint,
    "SecUserNumEntry" bigint,
    "SecDateEntry" date,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fee_pkey PRIMARY KEY ("FeeNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.fee
    OWNER to postgres;
-- Index: fee_ClinicNum

-- DROP INDEX IF EXISTS public."fee_ClinicNum";

CREATE INDEX IF NOT EXISTS "fee_ClinicNum"
    ON public.fee USING btree
    ("ClinicNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: fee_CodeNum

-- DROP INDEX IF EXISTS public."fee_CodeNum";

CREATE INDEX IF NOT EXISTS "fee_CodeNum"
    ON public.fee USING btree
    ("CodeNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: fee_FeeSchedCodeClinicProv

-- DROP INDEX IF EXISTS public."fee_FeeSchedCodeClinicProv";

CREATE INDEX IF NOT EXISTS "fee_FeeSchedCodeClinicProv"
    ON public.fee USING btree
    ("FeeSched" ASC NULLS LAST, "CodeNum" ASC NULLS LAST, "ClinicNum" ASC NULLS LAST, "ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: fee_ProvNum

-- DROP INDEX IF EXISTS public."fee_ProvNum";

CREATE INDEX IF NOT EXISTS "fee_ProvNum"
    ON public.fee USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: fee_SecUserNumEntry

-- DROP INDEX IF EXISTS public."fee_SecUserNumEntry";

CREATE INDEX IF NOT EXISTS "fee_SecUserNumEntry"
    ON public.fee USING btree
    ("SecUserNumEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: fee_indexADACode

-- DROP INDEX IF EXISTS public."fee_indexADACode";

CREATE INDEX IF NOT EXISTS "fee_indexADACode"
    ON public.fee USING btree
    ("OldCode" COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;