-- public.eobattach definition

-- Drop table

-- DROP TABLE public.eobattach;

CREATE TABLE public.eobattach (
	"EobAttachNum" serial4 NOT NULL,
	"ClaimPaymentNum" int8 NULL,
	"DateTCreated" timestamp NULL,
	"FileName" varchar(255) NULL,
	"RawBase64" text NULL,
	CONSTRAINT eobattach_pkey PRIMARY KEY ("EobAttachNum")
);
CREATE INDEX "eobattach_ClaimPaymentNum" ON public.eobattach USING btree ("ClaimPaymentNum");