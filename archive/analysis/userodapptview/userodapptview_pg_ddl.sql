-- public.userodapptview definition

-- Drop table

-- DROP TABLE public.userodapptview;

CREATE TABLE public.userodapptview (
	"UserodApptViewNum" serial4 NOT NULL,
	"UserNum" int8 NULL,
	"ClinicNum" int8 NULL,
	"ApptViewNum" int8 NULL,
	CONSTRAINT userodapptview_pkey PRIMARY KEY ("UserodApptViewNum")
);
CREATE INDEX "userodapptview_ApptViewNum" ON public.userodapptview USING btree ("ApptViewNum");
CREATE INDEX "userodapptview_ClinicNum" ON public.userodapptview USING btree ("ClinicNum");
CREATE INDEX "userodapptview_UserNum" ON public.userodapptview USING btree ("UserNum");