-- Table: public.appointmenttype

-- DROP TABLE IF EXISTS public.appointmenttype;

CREATE TABLE IF NOT EXISTS public.appointmenttype
(
    "AppointmentTypeNum" integer NOT NULL DEFAULT nextval('"appointmenttype_AppointmentTypeNum_seq"'::regclass),
    "AppointmentTypeName" character varying(255) COLLATE pg_catalog."default",
    "AppointmentTypeColor" integer,
    "ItemOrder" integer,
    "IsHidden" smallint,
    "Pattern" character varying(255) COLLATE pg_catalog."default",
    "CodeStr" character varying(4000) COLLATE pg_catalog."default",
    "CodeStrRequired" character varying(4000) COLLATE pg_catalog."default",
    "RequiredProcCodesNeeded" smallint,
    "BlockoutTypes" character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT appointmenttype_pkey PRIMARY KEY ("AppointmentTypeNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.appointmenttype
    OWNER to postgres;