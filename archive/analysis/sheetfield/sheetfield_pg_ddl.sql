-- Table: public.sheetfield

-- DROP TABLE IF EXISTS public.sheetfield;

CREATE TABLE IF NOT EXISTS public.sheetfield
(
    "SheetFieldNum" integer NOT NULL DEFAULT nextval('"sheetfield_SheetFieldNum_seq"'::regclass),
    "SheetNum" bigint,
    "FieldType" integer,
    "FieldName" character varying(255) COLLATE pg_catalog."default",
    "FieldValue" text COLLATE pg_catalog."default",
    "FontSize" real,
    "FontName" character varying(255) COLLATE pg_catalog."default",
    "FontIsBold" smallint,
    "XPos" integer,
    "YPos" integer,
    "Width" integer,
    "Height" integer,
    "GrowthBehavior" integer,
    "RadioButtonValue" character varying(255) COLLATE pg_catalog."default",
    "RadioButtonGroup" character varying(255) COLLATE pg_catalog."default",
    "IsRequired" smallint,
    "TabOrder" integer,
    "ReportableName" character varying(255) COLLATE pg_catalog."default",
    "TextAlign" smallint,
    "ItemColor" integer DEFAULT '-16777216'::integer,
    "DateTimeSig" timestamp without time zone,
    "IsLocked" smallint,
    "TabOrderMobile" integer,
    "UiLabelMobile" text COLLATE pg_catalog."default",
    "UiLabelMobileRadioButton" text COLLATE pg_catalog."default",
    "SheetFieldDefNum" bigint,
    "CanElectronicallySign" smallint,
    "IsSigProvRestricted" smallint,
    CONSTRAINT sheetfield_pkey PRIMARY KEY ("SheetFieldNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sheetfield
    OWNER to postgres;
-- Index: sheetfield_FieldType

-- DROP INDEX IF EXISTS public."sheetfield_FieldType";

CREATE INDEX IF NOT EXISTS "sheetfield_FieldType"
    ON public.sheetfield USING btree
    ("FieldType" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: sheetfield_SheetFieldDefNum

-- DROP INDEX IF EXISTS public."sheetfield_SheetFieldDefNum";

CREATE INDEX IF NOT EXISTS "sheetfield_SheetFieldDefNum"
    ON public.sheetfield USING btree
    ("SheetFieldDefNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: sheetfield_SheetNum

-- DROP INDEX IF EXISTS public."sheetfield_SheetNum";

CREATE INDEX IF NOT EXISTS "sheetfield_SheetNum"
    ON public.sheetfield USING btree
    ("SheetNum" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: sheetfield_SheetNumFieldType

-- DROP INDEX IF EXISTS public."sheetfield_SheetNumFieldType";

CREATE INDEX IF NOT EXISTS "sheetfield_SheetNumFieldType"
    ON public.sheetfield USING btree
    ("SheetNum" ASC NULLS LAST, "FieldType" ASC NULLS LAST)
    TABLESPACE pg_default;