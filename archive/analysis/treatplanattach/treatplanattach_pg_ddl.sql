-- Table: public.treatplanattach

-- DROP TABLE IF EXISTS public.treatplanattach;

CREATE TABLE IF NOT EXISTS public.treatplanattach
(
    "TreatPlanAttachNum" integer NOT NULL DEFAULT nextval('"treatplanattach_TreatPlanAttachNum_seq"'::regclass),
    "TreatPlanNum" bigint,
    "ProcNum" bigint,
    "Priority" bigint,
    CONSTRAINT treatplanattach_pkey PRIMARY KEY ("TreatPlanAttachNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.treatplanattach
    OWNER to postgres;
-- Index: treatplanattach_Priority

-- DROP INDEX IF EXISTS public."treatplanattach_Priority";

CREATE INDEX IF NOT EXISTS "treatplanattach_Priority"
    ON public.treatplanattach USING btree
    ("Priority" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: treatplanattach_ProcNum

-- DROP INDEX IF EXISTS public."treatplanattach_ProcNum";

CREATE INDEX IF NOT EXISTS "treatplanattach_ProcNum"
    ON public.treatplanattach USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: treatplanattach_TreatPlanNum

-- DROP INDEX IF EXISTS public."treatplanattach_TreatPlanNum";

CREATE INDEX IF NOT EXISTS "treatplanattach_TreatPlanNum"
    ON public.treatplanattach USING btree
    ("TreatPlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;