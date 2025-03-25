-- Table: public.procgroupitem

-- DROP TABLE IF EXISTS public.procgroupitem;

CREATE TABLE IF NOT EXISTS public.procgroupitem
(
    "ProcGroupItemNum" integer NOT NULL DEFAULT nextval('"procgroupitem_ProcGroupItemNum_seq"'::regclass),
    "ProcNum" bigint,
    "GroupNum" bigint,
    CONSTRAINT procgroupitem_pkey PRIMARY KEY ("ProcGroupItemNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.procgroupitem
    OWNER to postgres;
-- Index: procgroupitem_GroupNum

-- DROP INDEX IF EXISTS public."procgroupitem_GroupNum";

CREATE INDEX IF NOT EXISTS "procgroupitem_GroupNum"
    ON public.procgroupitem USING btree
    ("GroupNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: procgroupitem_ProcNum

-- DROP INDEX IF EXISTS public."procgroupitem_ProcNum";

CREATE INDEX IF NOT EXISTS "procgroupitem_ProcNum"
    ON public.procgroupitem USING btree
    ("ProcNum" ASC NULLS LAST)
    TABLESPACE pg_default;