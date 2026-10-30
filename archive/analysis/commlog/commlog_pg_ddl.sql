-- Table: public.commlog

-- DROP TABLE IF EXISTS public.commlog;

CREATE TABLE IF NOT EXISTS public.commlog
(
    "CommlogNum" integer NOT NULL DEFAULT nextval('"commlog_CommlogNum_seq"'::regclass),
    "PatNum" bigint,
    "CommDateTime" timestamp without time zone,
    "CommType" bigint,
    "Note" text COLLATE pg_catalog."default",
    "Mode_" smallint DEFAULT 0,
    "SentOrReceived" smallint DEFAULT 0,
    "UserNum" bigint,
    "Signature" text COLLATE pg_catalog."default",
    "SigIsTopaz" smallint,
    "DateTStamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "DateTimeEnd" timestamp without time zone,
    "CommSource" smallint,
    "ProgramNum" bigint,
    "DateTEntry" timestamp without time zone,
    "ReferralNum" bigint,
    "CommReferralBehavior" smallint,
    CONSTRAINT commlog_pkey PRIMARY KEY ("CommlogNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.commlog
    OWNER to postgres;
-- Index: commlog_CommDateTime

-- DROP INDEX IF EXISTS public."commlog_CommDateTime";

CREATE INDEX IF NOT EXISTS "commlog_CommDateTime"
    ON public.commlog USING btree
    ("CommDateTime" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: commlog_CommType

-- DROP INDEX IF EXISTS public."commlog_CommType";

CREATE INDEX IF NOT EXISTS "commlog_CommType"
    ON public.commlog USING btree
    ("CommType" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: commlog_PatNum

-- DROP INDEX IF EXISTS public."commlog_PatNum";

CREATE INDEX IF NOT EXISTS "commlog_PatNum"
    ON public.commlog USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: commlog_ProgramNum

-- DROP INDEX IF EXISTS public."commlog_ProgramNum";

CREATE INDEX IF NOT EXISTS "commlog_ProgramNum"
    ON public.commlog USING btree
    ("ProgramNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: commlog_ReferralNum

-- DROP INDEX IF EXISTS public."commlog_ReferralNum";

CREATE INDEX IF NOT EXISTS "commlog_ReferralNum"
    ON public.commlog USING btree
    ("ReferralNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: commlog_UserNum

-- DROP INDEX IF EXISTS public."commlog_UserNum";

CREATE INDEX IF NOT EXISTS "commlog_UserNum"
    ON public.commlog USING btree
    ("UserNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: commlog_indexPNCDateCType

-- DROP INDEX IF EXISTS public."commlog_indexPNCDateCType";

CREATE INDEX IF NOT EXISTS "commlog_indexPNCDateCType"
    ON public.commlog USING btree
    ("PatNum" ASC NULLS LAST, "CommDateTime" ASC NULLS LAST, "CommType" ASC NULLS LAST)
    TABLESPACE pg_default;