-- Table: public.insverifyhist

-- DROP TABLE IF EXISTS public.insverifyhist;

CREATE TABLE IF NOT EXISTS public.insverifyhist
(
    "InsVerifyHistNum" integer NOT NULL DEFAULT nextval('"insverifyhist_InsVerifyHistNum_seq"'::regclass),
    "InsVerifyNum" bigint,
    "DateLastVerified" date,
    "UserNum" bigint,
    "VerifyType" smallint,
    "FKey" bigint,
    "DefNum" bigint,
    "Note" text COLLATE pg_catalog."default",
    "DateLastAssigned" date,
    "DateTimeEntry" timestamp without time zone,
    "HoursAvailableForVerification" double precision,
    "VerifyUserNum" bigint,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT insverifyhist_pkey PRIMARY KEY ("InsVerifyHistNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.insverifyhist
    OWNER to postgres;
-- Index: insverifyhist_DefNum

-- DROP INDEX IF EXISTS public."insverifyhist_DefNum";

CREATE INDEX IF NOT EXISTS "insverifyhist_DefNum"
    ON public.insverifyhist USING btree
    ("DefNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverifyhist_FKey

-- DROP INDEX IF EXISTS public."insverifyhist_FKey";

CREATE INDEX IF NOT EXISTS "insverifyhist_FKey"
    ON public.insverifyhist USING btree
    ("FKey" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverifyhist_InsVerifyNum

-- DROP INDEX IF EXISTS public."insverifyhist_InsVerifyNum";

CREATE INDEX IF NOT EXISTS "insverifyhist_InsVerifyNum"
    ON public.insverifyhist USING btree
    ("InsVerifyNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverifyhist_SecDateTEdit

-- DROP INDEX IF EXISTS public."insverifyhist_SecDateTEdit";

CREATE INDEX IF NOT EXISTS "insverifyhist_SecDateTEdit"
    ON public.insverifyhist USING btree
    ("SecDateTEdit" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverifyhist_UserNum

-- DROP INDEX IF EXISTS public."insverifyhist_UserNum";

CREATE INDEX IF NOT EXISTS "insverifyhist_UserNum"
    ON public.insverifyhist USING btree
    ("UserNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverifyhist_VerifyUserNum

-- DROP INDEX IF EXISTS public."insverifyhist_VerifyUserNum";

CREATE INDEX IF NOT EXISTS "insverifyhist_VerifyUserNum"
    ON public.insverifyhist USING btree
    ("VerifyUserNum" ASC NULLS LAST)
    TABLESPACE pg_default;