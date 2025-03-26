-- Table: public.treatplanparam

-- DROP TABLE IF EXISTS public.treatplanparam;

CREATE TABLE IF NOT EXISTS public.treatplanparam
(
    "TreatPlanParamNum" integer NOT NULL DEFAULT nextval('"treatplanparam_TreatPlanParamNum_seq"'::regclass),
    "PatNum" bigint,
    "TreatPlanNum" bigint,
    "ShowDiscount" smallint,
    "ShowMaxDed" smallint,
    "ShowSubTotals" smallint,
    "ShowTotals" smallint,
    "ShowCompleted" smallint,
    "ShowFees" smallint,
    "ShowIns" smallint,
    CONSTRAINT treatplanparam_pkey PRIMARY KEY ("TreatPlanParamNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.treatplanparam
    OWNER to postgres;
-- Index: treatplanparam_PatNum

-- DROP INDEX IF EXISTS public."treatplanparam_PatNum";

CREATE INDEX IF NOT EXISTS "treatplanparam_PatNum"
    ON public.treatplanparam USING btree
    ("PatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: treatplanparam_TreatPlanNum

-- DROP INDEX IF EXISTS public."treatplanparam_TreatPlanNum";

CREATE INDEX IF NOT EXISTS "treatplanparam_TreatPlanNum"
    ON public.treatplanparam USING btree
    ("TreatPlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;