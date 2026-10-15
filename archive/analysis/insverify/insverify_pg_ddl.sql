-- Table: public.insverify

-- DROP TABLE IF EXISTS public.insverify;

CREATE TABLE IF NOT EXISTS public.insverify
(
    "InsVerifyNum" integer NOT NULL DEFAULT nextval('"insverify_InsVerifyNum_seq"'::regclass),
    "DateLastVerified" date,
    "UserNum" bigint,
    "VerifyType" smallint,
    "FKey" bigint,
    "DefNum" bigint,
    "Note" text COLLATE pg_catalog."default",
    "DateLastAssigned" date,
    "DateTimeEntry" timestamp without time zone,
    "HoursAvailableForVerification" double precision,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT insverify_pkey PRIMARY KEY ("InsVerifyNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.insverify
    OWNER to postgres;
-- Index: insverify_DateLastAssigned

-- DROP INDEX IF EXISTS public."insverify_DateLastAssigned";

CREATE INDEX IF NOT EXISTS "insverify_DateLastAssigned"
    ON public.insverify USING btree
    ("DateLastAssigned" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverify_DateTimeEntry

-- DROP INDEX IF EXISTS public."insverify_DateTimeEntry";

CREATE INDEX IF NOT EXISTS "insverify_DateTimeEntry"
    ON public.insverify USING btree
    ("DateTimeEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverify_DefNum

-- DROP INDEX IF EXISTS public."insverify_DefNum";

CREATE INDEX IF NOT EXISTS "insverify_DefNum"
    ON public.insverify USING btree
    ("DefNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverify_FKey

-- DROP INDEX IF EXISTS public."insverify_FKey";

CREATE INDEX IF NOT EXISTS "insverify_FKey"
    ON public.insverify USING btree
    ("FKey" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverify_SecDateTEdit

-- DROP INDEX IF EXISTS public."insverify_SecDateTEdit";

CREATE INDEX IF NOT EXISTS "insverify_SecDateTEdit"
    ON public.insverify USING btree
    ("SecDateTEdit" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverify_UserNum

-- DROP INDEX IF EXISTS public."insverify_UserNum";

CREATE INDEX IF NOT EXISTS "insverify_UserNum"
    ON public.insverify USING btree
    ("UserNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: insverify_VerifyType

-- DROP INDEX IF EXISTS public."insverify_VerifyType";

CREATE INDEX IF NOT EXISTS "insverify_VerifyType"
    ON public.insverify USING btree
    ("VerifyType" ASC NULLS LAST)
    TABLESPACE pg_default;