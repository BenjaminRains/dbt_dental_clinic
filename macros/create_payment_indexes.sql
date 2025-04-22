{% macro create_payment_indexes() %}

-- Indexes for stg_opendental__payment
CREATE INDEX IF NOT EXISTS idx_payment_payment_id ON {{ ref('stg_opendental__payment') }}(payment_id);
CREATE INDEX IF NOT EXISTS idx_payment_patient_id ON {{ ref('stg_opendental__payment') }}(patient_id);
CREATE INDEX IF NOT EXISTS idx_payment_payment_date ON {{ ref('stg_opendental__payment') }}(payment_date);
CREATE INDEX IF NOT EXISTS idx_payment_payment_type ON {{ ref('stg_opendental__payment') }}(payment_type_id);

-- Indexes for stg_opendental__paysplit
CREATE INDEX IF NOT EXISTS idx_paysplit_payment_id ON {{ ref('stg_opendental__paysplit') }}(payment_id);
CREATE INDEX IF NOT EXISTS idx_paysplit_procedure_id ON {{ ref('stg_opendental__paysplit') }}(procedure_id);
CREATE INDEX IF NOT EXISTS idx_paysplit_unearned_type ON {{ ref('stg_opendental__paysplit') }}(unearned_type);

-- Indexes for stg_opendental__claimpayment
CREATE INDEX IF NOT EXISTS idx_claimpayment_claim_payment_id ON {{ ref('stg_opendental__claimpayment') }}(claim_payment_id);
CREATE INDEX IF NOT EXISTS idx_claimpayment_check_date ON {{ ref('stg_opendental__claimpayment') }}(check_date);

-- Indexes for stg_opendental__claimproc
CREATE INDEX IF NOT EXISTS idx_claimproc_claim_payment_id ON {{ ref('stg_opendental__claimproc') }}(claim_payment_id);
CREATE INDEX IF NOT EXISTS idx_claimproc_status ON {{ ref('stg_opendental__claimproc') }}(status);
CREATE INDEX IF NOT EXISTS idx_claimproc_patient_id ON {{ ref('stg_opendental__claimproc') }}(patient_id);
CREATE INDEX IF NOT EXISTS idx_claimproc_procedure_id ON {{ ref('stg_opendental__claimproc') }}(procedure_id);

-- Indexes for stg_opendental__procedurelog
CREATE INDEX IF NOT EXISTS idx_procedurelog_procedure_id ON {{ ref('stg_opendental__procedurelog') }}(procedure_id);
CREATE INDEX IF NOT EXISTS idx_procedurelog_provider_id ON {{ ref('stg_opendental__procedurelog') }}(provider_id);

{% endmacro %} 