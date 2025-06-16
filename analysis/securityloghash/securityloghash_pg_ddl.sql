-- public.securityloghash definition

-- Drop table

-- DROP TABLE public.securityloghash;

CREATE TABLE public.securityloghash (
	"SecurityLogHashNum" serial4 NOT NULL,
	"SecurityLogNum" int8 NULL,
	"LogHash" varchar(255) NULL,
	CONSTRAINT securityloghash_pkey PRIMARY KEY ("SecurityLogHashNum")
);
CREATE INDEX "securityloghash_SecurityLogNum" ON public.securityloghash USING btree ("SecurityLogNum");