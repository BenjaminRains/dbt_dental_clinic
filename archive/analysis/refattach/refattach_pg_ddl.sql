-- Table: public.refattach

-- DROP TABLE IF EXISTS public.refattach;

CREATE TABLE IF NOT EXISTS public.refattach
(
    "RefAttachNum" integer NOT NULL DEFAULT nextval('"refattach_RefAttachNum_seq"'::regclass),
    "ReferralNum" bigint,
    "PatNum" bigint,
    "ItemOrder" smallint DEFAULT 0,
    "RefDate" date,
    "RefType" smallint,
    "RefToStatus" smallint,
    "Note" text COLLATE pg_catalog."default",
    "IsTransitionOfCare" smallint,
    "ProcNum" bigint,
    "DateProcComplete" date,
    "ProvNum" bigint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT refattach_pkey PRIMARY KEY ("RefAttachNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.refattach
    OWNER to postgres;
-- Index: refattach_PatNum

-- DROP INDEX IF EXISTS public."refattach_PatNum";

CREATE INDEX IF NOT EXISTS "refattach_PatNum"
    ON public.refattach USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: refattach_ProcNum

-- DROP INDEX IF EXISTS public."refattach_ProcNum";

CREATE INDEX IF NOT EXISTS "refattach_ProcNum"
    ON public.refattach USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: refattach_ProvNum

-- DROP INDEX IF EXISTS public."refattach_ProvNum";

CREATE INDEX IF NOT EXISTS "refattach_ProvNum"
    ON public.refattach USING btree
    ("ProvNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: refattach_ReferralNum

-- DROP INDEX IF EXISTS public."refattach_ReferralNum";

CREATE INDEX IF NOT EXISTS "refattach_ReferralNum"
    ON public.refattach USING btree
    ("ReferralNum" ASC NULLS LAST)
    TABLESPACE pg_default;