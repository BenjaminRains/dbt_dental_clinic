-- Table: public.employee

-- DROP TABLE IF EXISTS public.employee;

CREATE TABLE IF NOT EXISTS public.employee
(
    "EmployeeNum" integer NOT NULL DEFAULT nextval('"employee_EmployeeNum_seq"'::regclass),
    "LName" character varying(255) COLLATE pg_catalog."default",
    "FName" character varying(255) COLLATE pg_catalog."default",
    "MiddleI" character varying(255) COLLATE pg_catalog."default",
    "IsHidden" smallint DEFAULT 0,
    "ClockStatus" character varying(255) COLLATE pg_catalog."default",
    "PhoneExt" integer,
    "PayrollID" character varying(255) COLLATE pg_catalog."default",
    "WirelessPhone" character varying(255) COLLATE pg_catalog."default",
    "EmailWork" character varying(255) COLLATE pg_catalog."default",
    "EmailPersonal" character varying(255) COLLATE pg_catalog."default",
    "IsFurloughed" smallint,
    "IsWorkingHome" smallint,
    "ReportsTo" bigint,
    CONSTRAINT employee_pkey PRIMARY KEY ("EmployeeNum")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.employee
    OWNER to postgres;