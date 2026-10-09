-- public.apptview definition

-- Drop table

-- DROP TABLE public.apptview;

CREATE TABLE public.apptview (
	"ApptViewNum" serial4 NOT NULL,
	"Description" varchar(255) NULL,
	"ItemOrder" int2 DEFAULT 0 NULL,
	"RowsPerIncr" int2 DEFAULT 1 NULL,
	"OnlyScheduledProvs" int2 NULL,
	"OnlySchedBeforeTime" time NULL,
	"OnlySchedAfterTime" time NULL,
	"StackBehavUR" int2 NULL,
	"StackBehavLR" int2 NULL,
	"ClinicNum" int8 NULL,
	"ApptTimeScrollStart" time NULL,
	"IsScrollStartDynamic" int2 NULL,
	"IsApptBubblesDisabled" int2 NULL,
	"WidthOpMinimum" int2 NULL,
	"WaitingRmName" int2 NULL,
	"OnlyScheduledProvDays" int2 NULL,
	CONSTRAINT apptview_pkey PRIMARY KEY ("ApptViewNum")
);
CREATE INDEX "apptview_ClinicNum" ON public.apptview USING btree ("ClinicNum");