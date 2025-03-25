-- Table: public.periomeasure

-- DROP TABLE IF EXISTS public.periomeasure;

CREATE TABLE IF NOT EXISTS public.periomeasure
(
    "PerioMeasureNum" integer NOT NULL DEFAULT nextval('"periomeasure_PerioMeasureNum_seq"'::regclass),
    "PerioExamNum" bigint,
    "SequenceType" smallint DEFAULT 0,
    "IntTooth" smallint,
    "ToothValue" smallint,
    "MBvalue" smallint,
    "Bvalue" smallint,
    "DBvalue" smallint,
    "MLvalue" smallint,
    "Lvalue" smallint,
    "DLvalue" smallint,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT periomeasure_pkey PRIMARY KEY ("PerioMeasureNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.periomeasure
    OWNER to postgres;
-- Index: periomeasure_PerioExamNum

-- DROP INDEX IF EXISTS public."periomeasure_PerioExamNum";

CREATE INDEX IF NOT EXISTS "periomeasure_PerioExamNum"
    ON public.periomeasure USING btree
    ("PerioExamNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: periomeasure_SecDateTEdit

-- DROP INDEX IF EXISTS public."periomeasure_SecDateTEdit";

CREATE INDEX IF NOT EXISTS "periomeasure_SecDateTEdit"
    ON public.periomeasure USING btree
    ("SecDateTEdit" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: periomeasure_SecDateTEntry

-- DROP INDEX IF EXISTS public."periomeasure_SecDateTEntry";

CREATE INDEX IF NOT EXISTS "periomeasure_SecDateTEntry"
    ON public.periomeasure USING btree
    ("SecDateTEntry" ASC NULLS LAST)
    TABLESPACE pg_default;