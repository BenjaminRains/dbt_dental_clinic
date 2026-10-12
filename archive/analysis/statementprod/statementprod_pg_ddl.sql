-- Table: public.statementprod

-- DROP TABLE IF EXISTS public.statementprod;

CREATE TABLE IF NOT EXISTS public.statementprod
(
    "StatementProdNum" integer NOT NULL DEFAULT nextval('"statementprod_StatementProdNum_seq"'::regclass),
    "StatementNum" bigint,
    "FKey" bigint,
    "ProdType" smallint,
    "LateChargeAdjNum" bigint,
    "DocNum" bigint,
    CONSTRAINT statementprod_pkey PRIMARY KEY ("StatementProdNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.statementprod
    OWNER to postgres;
-- Index: statementprod_DocNum

-- DROP INDEX IF EXISTS public."statementprod_DocNum";

CREATE INDEX IF NOT EXISTS "statementprod_DocNum"
    ON public.statementprod USING btree
    ("DocNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: statementprod_FKey

-- DROP INDEX IF EXISTS public."statementprod_FKey";

CREATE INDEX IF NOT EXISTS "statementprod_FKey"
    ON public.statementprod USING btree
    ("FKey" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: statementprod_LateChargeAdjNum

-- DROP INDEX IF EXISTS public."statementprod_LateChargeAdjNum";

CREATE INDEX IF NOT EXISTS "statementprod_LateChargeAdjNum"
    ON public.statementprod USING btree
    ("LateChargeAdjNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: statementprod_ProdType

-- DROP INDEX IF EXISTS public."statementprod_ProdType";

CREATE INDEX IF NOT EXISTS "statementprod_ProdType"
    ON public.statementprod USING btree
    ("ProdType" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: statementprod_StatementNum

-- DROP INDEX IF EXISTS public."statementprod_StatementNum";

CREATE INDEX IF NOT EXISTS "statementprod_StatementNum"
    ON public.statementprod USING btree
    ("StatementNum" ASC NULLS LAST)
    TABLESPACE pg_default;