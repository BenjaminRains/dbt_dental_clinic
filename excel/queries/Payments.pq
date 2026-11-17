// Daily Close - Payments.
// The mart_daily_payments row for the BusinessDate parameter.
// Host, port, and database come from the opendental_analytics_bi DSN.
// The bi_user login is entered once at the Power Query credential prompt.
let
    Source = Odbc.DataSource("dsn=opendental_analytics_bi", [HierarchicalNavigation = true]),
    Database = Source{[Name = "opendental_analytics", Kind = "Database"]}[Data],
    Marts = Database{[Name = "marts", Kind = "Schema"]}[Data],
    DailyPayments = Marts{[Name = "mart_daily_payments", Kind = "Table"]}[Data],
    ForBusinessDate = Table.SelectRows(DailyPayments, each [payment_date] = BusinessDate),
    Columns = Table.SelectColumns(
        ForBusinessDate,
        {
            "payment_date",
            "patient_payment_amount",
            "insurance_payment_amount",
            "net_collections_amount",
            "income_amount",
            "refund_amount",
            "other_payment_amount",
            "patient_payment_count",
            "insurance_payment_count",
            "payment_count",
            "other_payment_count"
        }
    )
in
    Columns
