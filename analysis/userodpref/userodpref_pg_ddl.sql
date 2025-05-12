-- public.userodpref definition

-- Drop table

-- DROP TABLE public.userodpref;

CREATE TABLE public.userodpref (
	"UserOdPrefNum" serial4 NOT NULL,
	"UserNum" int8 NULL,
	"Fkey" int8 NULL,
	"FkeyType" int2 NULL,
	"ValueString" text NULL,
	"ClinicNum" int8 NULL,
	CONSTRAINT userodpref_pkey PRIMARY KEY ("UserOdPrefNum")
);
CREATE INDEX "userodpref_ClinicNum" ON public.userodpref USING btree ("ClinicNum");
CREATE INDEX "userodpref_Fkey" ON public.userodpref USING btree ("Fkey");
CREATE INDEX "userodpref_UserNum" ON public.userodpref USING btree ("UserNum");