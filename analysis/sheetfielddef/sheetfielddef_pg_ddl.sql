-- Table: public.sheetfielddef

-- DROP TABLE IF EXISTS public.sheetfielddef;

CREATE TABLE IF NOT EXISTS public.sheetfielddef
(
    "SheetFieldDefNum" integer NOT NULL DEFAULT nextval('"sheetfielddef_SheetFieldDefNum_seq"'::regclass),
    "SheetDefNum" bigint,
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
    "IsPaymentOption" smallint,
    "ItemColor" integer DEFAULT '-16777216'::integer,
    "IsLocked" smallint,
    "TabOrderMobile" integer,
    "UiLabelMobile" text COLLATE pg_catalog."default",
    "UiLabelMobileRadioButton" text COLLATE pg_catalog."default",
    "LayoutMode" smallint,
    "Language" character varying(255) COLLATE pg_catalog."default",
    "CanElectronicallySign" smallint,
    "IsSigProvRestricted" smallint,
    CONSTRAINT sheetfielddef_pkey PRIMARY KEY ("SheetFieldDefNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.sheetfielddef
    OWNER to postgres;
-- Index: sheetfielddef_SheetDefNum

-- DROP INDEX IF EXISTS public."sheetfielddef_SheetDefNum";

CREATE INDEX IF NOT EXISTS "sheetfielddef_SheetDefNum"
    ON public.sheetfielddef USING btree
    ("SheetDefNum" ASC NULLS LAST)
    TABLESPACE pg_default;