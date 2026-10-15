-- public.medication definition

-- Drop table

-- DROP TABLE public.medication;

CREATE TABLE public.medication (
	"MedicationNum" serial4 NOT NULL,
	"MedName" varchar(255) NULL,
	"GenericNum" int8 NULL,
	"Notes" text NULL,
	"DateTStamp" timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	"RxCui" int8 NULL,
	CONSTRAINT medication_pkey PRIMARY KEY ("MedicationNum")
);
CREATE INDEX "medication_RxCui" ON public.medication USING btree ("RxCui");