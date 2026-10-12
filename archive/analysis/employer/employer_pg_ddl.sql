-- public.employer definition

-- Drop table

-- DROP TABLE public.employer;

CREATE TABLE public.employer (
	"EmployerNum" serial4 NOT NULL,
	"EmpName" varchar(255) NULL,
	"Address" varchar(255) NULL,
	"Address2" varchar(255) NULL,
	"City" varchar(255) NULL,
	"State" varchar(255) NULL,
	"Zip" varchar(255) NULL,
	"Phone" varchar(255) NULL,
	CONSTRAINT employer_pkey PRIMARY KEY ("EmployerNum")
);