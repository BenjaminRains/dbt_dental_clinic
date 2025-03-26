-- Table: public.benefit

-- DROP TABLE IF EXISTS public.benefit;

CREATE TABLE IF NOT EXISTS public.benefit
(
    "BenefitNum" integer NOT NULL DEFAULT nextval('"benefit_BenefitNum_seq"'::regclass),
    "PlanNum" bigint,
    "PatPlanNum" bigint,
    "CovCatNum" bigint,
    "BenefitType" smallint,
    "Percent" smallint,
    "MonetaryAmt" double precision,
    "TimePeriod" smallint,
    "QuantityQualifier" smallint,
    "Quantity" smallint,
    "CodeNum" bigint,
    "CoverageLevel" integer,
    "SecDateTEntry" timestamp without time zone,
    "SecDateTEdit" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    "CodeGroupNum" bigint,
    "TreatArea" smallint,
    CONSTRAINT benefit_pkey PRIMARY KEY ("BenefitNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.benefit
    OWNER to postgres;
-- Index: benefit_BenefitType

-- DROP INDEX IF EXISTS public."benefit_BenefitType";

CREATE INDEX IF NOT EXISTS "benefit_BenefitType"
    ON public.benefit USING btree
    ("BenefitType" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_CodeGroupNum

-- DROP INDEX IF EXISTS public."benefit_CodeGroupNum";

CREATE INDEX IF NOT EXISTS "benefit_CodeGroupNum"
    ON public.benefit USING btree
    ("CodeGroupNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_CodeNum

-- DROP INDEX IF EXISTS public."benefit_CodeNum";

CREATE INDEX IF NOT EXISTS "benefit_CodeNum"
    ON public.benefit USING btree
    ("CodeNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_CovCatNum

-- DROP INDEX IF EXISTS public."benefit_CovCatNum";

CREATE INDEX IF NOT EXISTS "benefit_CovCatNum"
    ON public.benefit USING btree
    ("CovCatNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_CoverageLevel

-- DROP INDEX IF EXISTS public."benefit_CoverageLevel";

CREATE INDEX IF NOT EXISTS "benefit_CoverageLevel"
    ON public.benefit USING btree
    ("CoverageLevel" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_MonetaryAmt

-- DROP INDEX IF EXISTS public."benefit_MonetaryAmt";

CREATE INDEX IF NOT EXISTS "benefit_MonetaryAmt"
    ON public.benefit USING btree
    ("MonetaryAmt" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_Percent

-- DROP INDEX IF EXISTS public."benefit_Percent";

CREATE INDEX IF NOT EXISTS "benefit_Percent"
    ON public.benefit USING btree
    ("Percent" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_Quantity

-- DROP INDEX IF EXISTS public."benefit_Quantity";

CREATE INDEX IF NOT EXISTS "benefit_Quantity"
    ON public.benefit USING btree
    ("Quantity" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_QuantityQualifier

-- DROP INDEX IF EXISTS public."benefit_QuantityQualifier";

CREATE INDEX IF NOT EXISTS "benefit_QuantityQualifier"
    ON public.benefit USING btree
    ("QuantityQualifier" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_SecDateTEdit

-- DROP INDEX IF EXISTS public."benefit_SecDateTEdit";

CREATE INDEX IF NOT EXISTS "benefit_SecDateTEdit"
    ON public.benefit USING btree
    ("SecDateTEdit" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_SecDateTEntry

-- DROP INDEX IF EXISTS public."benefit_SecDateTEntry";

CREATE INDEX IF NOT EXISTS "benefit_SecDateTEntry"
    ON public.benefit USING btree
    ("SecDateTEntry" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_TimePeriod

-- DROP INDEX IF EXISTS public."benefit_TimePeriod";

CREATE INDEX IF NOT EXISTS "benefit_TimePeriod"
    ON public.benefit USING btree
    ("TimePeriod" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_indexPatPlanNum

-- DROP INDEX IF EXISTS public."benefit_indexPatPlanNum";

CREATE INDEX IF NOT EXISTS "benefit_indexPatPlanNum"
    ON public.benefit USING btree
    ("PatPlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: benefit_indexPlanNum

-- DROP INDEX IF EXISTS public."benefit_indexPlanNum";

CREATE INDEX IF NOT EXISTS "benefit_indexPlanNum"
    ON public.benefit USING btree
    ("PlanNum" ASC NULLS LAST)
    TABLESPACE pg_default;