// Daily Close - Production.
// The mart_daily_production_by_procedure rows for the BusinessDate parameter,
// one per procedure code.
// Host, port, and database come from the opendental_analytics_bi DSN.
// The bi_user login is entered once at the Power Query credential prompt.
let
    Source = Odbc.DataSource("dsn=opendental_analytics_bi", [HierarchicalNavigation = true]),
    Database = Source{[Name = "opendental_analytics", Kind = "Database"]}[Data],
    Marts = Database{[Name = "marts", Kind = "Schema"]}[Data],
    DailyProduction = Marts{[Name = "mart_daily_production_by_procedure", Kind = "Table"]}[Data],
    ForBusinessDate = Table.SelectRows(DailyProduction, each [production_date] = BusinessDate),
    Columns = Table.SelectColumns(
        ForBusinessDate,
        {
            "production_date",
            "procedure_code",
            "procedure_description",
            "procedure_category",
            "procedure_quantity",
            "total_fees",
            "average_fee"
        }
    ),
    Sorted = Table.Sort(Columns, {{"procedure_code", Order.Ascending}})
in
    Sorted
