-- public.claimsnapshot definition

-- Drop table

-- DROP TABLE public.claimsnapshot;

CREATE TABLE public.claimsnapshot (
	"ClaimSnapshotNum" serial4 NOT NULL,
	"ProcNum" int8 NULL,
	"ClaimType" varchar(255) NULL,
	"Writeoff" float8 NULL,
	"InsPayEst" float8 NULL,
	"Fee" float8 NULL,
	"DateTEntry" timestamp NULL,
	"ClaimProcNum" int8 NULL,
	"SnapshotTrigger" int2 NULL,
	CONSTRAINT claimsnapshot_pkey PRIMARY KEY ("ClaimSnapshotNum")
);
CREATE INDEX "claimsnapshot_ClaimProcNum" ON public.claimsnapshot USING btree ("ClaimProcNum");
CREATE INDEX "claimsnapshot_ProcNum" ON public.claimsnapshot USING btree ("ProcNum");