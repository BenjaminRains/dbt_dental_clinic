// Daily Close - business date parameter.
// DailyClose.bas rewrites this formula on every run. Paste it once so the
// Payments and Production queries have a parameter to filter on.
#date(2026, 1, 1) meta [IsParameterQuery = true, Type = "Date", IsParameterQueryRequired = true]
