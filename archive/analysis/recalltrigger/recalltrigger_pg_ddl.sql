-- Table: public.recalltrigger

-- DROP TABLE IF EXISTS public.recalltrigger;

CREATE TABLE IF NOT EXISTS public.recalltrigger
(
    "RecallTriggerNum" integer NOT NULL DEFAULT nextval('"recalltrigger_RecallTriggerNum_seq"'::regclass),
    "RecallTypeNum" bigint,
    "CodeNum" bigint,
    CONSTRAINT recalltrigger_pkey PRIMARY KEY ("RecallTriggerNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.recalltrigger
    OWNER to postgres;
-- Index: recalltrigger_CodeNum

-- DROP INDEX IF EXISTS public."recalltrigger_CodeNum";

CREATE INDEX IF NOT EXISTS "recalltrigger_CodeNum"
    ON public.recalltrigger USING btree
    ("CodeNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: recalltrigger_RecallTypeNum

-- DROP INDEX IF EXISTS public."recalltrigger_RecallTypeNum";

CREATE INDEX IF NOT EXISTS "recalltrigger_RecallTypeNum"
    ON public.recalltrigger USING btree
    ("RecallTypeNum" ASC NULLS LAST)
    TABLESPACE pg_default;