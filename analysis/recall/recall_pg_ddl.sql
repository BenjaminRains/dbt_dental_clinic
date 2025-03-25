-- Table: public.recall

-- DROP TABLE IF EXISTS public.recall;

CREATE TABLE IF NOT EXISTS public.recall
(
    "RecallNum" integer NOT NULL DEFAULT nextval('"recall_RecallNum_seq"'::regclass),
    "PatNum" bigint,
    "DateDueCalc" date,
    "DateDue" date,
    "DatePrevious" date,
    "RecallInterval" integer DEFAULT 0,
    "RecallStatus" bigint,
    "Note" text COLLATE pg_catalog."default",
    "IsDisabled" smallint DEFAULT 0,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "RecallTypeNum" bigint,
    "DisableUntilBalance" double precision,
    "DisableUntilDate" date,
    "DateScheduled" date,
    "Priority" smallint,
    "TimePatternOverride" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT recall_pkey PRIMARY KEY ("RecallNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.recall
    OWNER to postgres;
-- Index: recall_DateDisabledType

-- DROP INDEX IF EXISTS public."recall_DateDisabledType";

CREATE INDEX IF NOT EXISTS "recall_DateDisabledType"
    ON public.recall USING btree
    ("DateDue" ASC NULLS LAST, "IsDisabled" ASC NULLS LAST, "RecallTypeNum" ASC NULLS LAST, "DateScheduled" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: recall_DatePrevious

-- DROP INDEX IF EXISTS public."recall_DatePrevious";

CREATE INDEX IF NOT EXISTS "recall_DatePrevious"
    ON public.recall USING btree
    ("DatePrevious" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: recall_DateScheduled

-- DROP INDEX IF EXISTS public."recall_DateScheduled";

CREATE INDEX IF NOT EXISTS "recall_DateScheduled"
    ON public.recall USING btree
    ("DateScheduled" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: recall_IsDisabled

-- DROP INDEX IF EXISTS public."recall_IsDisabled";

CREATE INDEX IF NOT EXISTS "recall_IsDisabled"
    ON public.recall USING btree
    ("IsDisabled" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: recall_PatNum

-- DROP INDEX IF EXISTS public."recall_PatNum";

CREATE INDEX IF NOT EXISTS "recall_PatNum"
    ON public.recall USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: recall_RecallTypeNum

-- DROP INDEX IF EXISTS public."recall_RecallTypeNum";

CREATE INDEX IF NOT EXISTS "recall_RecallTypeNum"
    ON public.recall USING btree
    ("RecallTypeNum" ASC NULLS LAST)
    TABLESPACE pg_default;