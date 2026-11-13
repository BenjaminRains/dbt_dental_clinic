Attribute VB_Name = "DailyClose"
Option Explicit

' Daily Close: one printable sheet for one business date.
'
' Reads two dbt marts through Power Query and never writes to the database.
'   Payments    marts.mart_daily_payments                 one row for the date
'   Production  marts.mart_daily_production_by_procedure  one row per procedure code
'
' This module owns the date prompt, the BusinessDate parameter, the refresh,
' and the print layout. The queries live in excel/queries/*.pq. No password
' is stored here or in the workbook.
'
' One-time setup (the workbook stays local and is gitignored):
'   1. Install the 64-bit PostgreSQL ODBC driver (psqlODBC).
'   2. Create a User DSN named opendental_analytics_bi with the host, port,
'      and database from dbt_dental_models/.env_local. Leave the user name
'      and password blank.
'   3. As a superuser, run the two Excel daily close GRANT lines in
'      docs/analytics/sql/bi_user_marts_grants.sql. Run them again after a
'      dbt run rebuilds the marts.
'   4. Save a new workbook as excel/DailyClose.xlsm. In the Power Query
'      editor add three blank queries named BusinessDate, Payments, and
'      Production, and paste the matching file from excel/queries/ into each.
'      Load Payments and Production to tables. Keep BusinessDate as a
'      connection only.
'   5. At the credential prompt choose Database and enter POSTGRES_BI_USER
'      and POSTGRES_BI_PASSWORD from powerbi/.env_local.
'   6. In the VBA editor choose File > Import File and pick this file.
'
' Opening the workbook asks for the date. RunDailyClose can also be assigned
' to a button.

Private Const PARAMETER_QUERY As String = "BusinessDate"
Private Const PAYMENTS_TABLE As String = "Payments"
Private Const PRODUCTION_TABLE As String = "Production"
Private Const REPORT_SHEET As String = "Daily Close"
Private Const DATE_INPUT_FORMAT As String = "yyyy-mm-dd"
Private Const MONEY_FORMAT As String = "#,##0.00"
Private Const COUNT_FORMAT As String = "#,##0"
Private Const TEXT_FORMAT As String = "@"

' Report columns. The payment lines reuse the count and amount columns.
Private Const COL_CODE As Long = 1
Private Const COL_DESCRIPTION As Long = 2
Private Const COL_CATEGORY As Long = 3
Private Const COL_COUNT As Long = 4
Private Const COL_AMOUNT As Long = 5
Private Const COL_AVERAGE As Long = 6

' Runs when the workbook is opened by hand.
Public Sub Auto_Open()
    RunDailyClose
End Sub

Public Sub RunDailyClose()
    Dim businessDate As Date

    If PromptForBusinessDate(businessDate) Then RunForDate businessDate
End Sub

Public Sub RunForDate(ByVal businessDate As Date)
    Dim payments As ListObject
    Dim production As ListObject

    On Error GoTo Failed
    Application.ScreenUpdating = False

    Set payments = FindTable(PAYMENTS_TABLE)
    Set production = FindTable(PRODUCTION_TABLE)

    SetBusinessDate businessDate
    payments.QueryTable.Refresh BackgroundQuery:=False
    production.QueryTable.Refresh BackgroundQuery:=False

    RequireDate payments, "payment_date", businessDate
    RequireDate production, "production_date", businessDate

    ' The report is rewritten only after both refreshes succeed.
    WriteReport businessDate, payments, production

    Application.ScreenUpdating = True
    Exit Sub

Failed:
    Application.ScreenUpdating = True
    MsgBox "Daily Close did not finish." & vbCrLf & vbCrLf & Err.Description, _
        vbExclamation, REPORT_SHEET
End Sub

' Returns False when the prompt is cancelled or left blank.
Private Function PromptForBusinessDate(ByRef businessDate As Date) As Boolean
    Dim message As String
    Dim answer As String

    message = "Business date (" & DATE_INPUT_FORMAT & ")"
    Do
        answer = Trim$(InputBox(message, REPORT_SHEET, Format$(Date - 1, DATE_INPUT_FORMAT)))
        If Len(answer) = 0 Then Exit Function
        If IsDate(answer) Then
            If DateValue(answer) <= Date Then
                businessDate = DateValue(answer)
                PromptForBusinessDate = True
                Exit Function
            End If
        End If
        message = "Enter a date on or before today (" & DATE_INPUT_FORMAT & ")"
    Loop
End Function

' Both mart queries filter on this Power Query parameter.
Private Sub SetBusinessDate(ByVal businessDate As Date)
    Dim parameterQuery As WorkbookQuery

    On Error Resume Next
    Set parameterQuery = ThisWorkbook.Queries(PARAMETER_QUERY)
    On Error GoTo 0
    If parameterQuery Is Nothing Then
        Err.Raise vbObjectError + 1, "DailyClose", _
            "The " & PARAMETER_QUERY & " query is missing. See the setup steps at the top of DailyClose.bas."
    End If

    parameterQuery.Formula = _
        "#date(" & Year(businessDate) & ", " & Month(businessDate) & ", " & Day(businessDate) & ")" & _
        " meta [IsParameterQuery = true, Type = ""Date"", IsParameterQueryRequired = true]"
End Sub

Private Function FindTable(ByVal tableName As String) As ListObject
    Dim sheet As Worksheet
    Dim candidate As ListObject

    For Each sheet In ThisWorkbook.Worksheets
        For Each candidate In sheet.ListObjects
            If StrComp(candidate.Name, tableName, vbTextCompare) = 0 Then
                Set FindTable = candidate
                Exit Function
            End If
        Next candidate
    Next sheet

    Err.Raise vbObjectError + 2, "DailyClose", _
        "The " & tableName & " table is missing. Load the " & tableName & _
        " query to a table. See the setup steps at the top of DailyClose.bas."
End Function

' Stops the run when a refreshed table holds rows for any other date, so the
' sheet cannot print one day's numbers under another day's heading.
Private Sub RequireDate(ByVal source As ListObject, ByVal dateColumn As String, ByVal businessDate As Date)
    Dim dates As Range

    If source.ListRows.Count = 0 Then Exit Sub

    Set dates = source.ListColumns(dateColumn).DataBodyRange
    If Application.WorksheetFunction.Min(dates) <> CLng(businessDate) _
        Or Application.WorksheetFunction.Max(dates) <> CLng(businessDate) Then
        Err.Raise vbObjectError + 3, "DailyClose", _
            "The " & source.Name & " table holds rows for a date other than " & _
            Format$(businessDate, DATE_INPUT_FORMAT) & ". Check that the " & source.Name & _
            " query filters on the " & PARAMETER_QUERY & " parameter."
    End If
End Sub

Private Sub WriteReport(ByVal businessDate As Date, ByVal payments As ListObject, ByVal production As ListObject)
    Dim report As Worksheet
    Dim lastPaymentRow As Long
    Dim lastRow As Long
    Dim titleRow As Long

    Set report = ReportSheet()
    report.Cells.Clear

    With report.Cells(1, COL_CODE)
        .Value = REPORT_SHEET
        .Font.Bold = True
        .Font.Size = 14
    End With
    With report.Cells(2, COL_CODE)
        .NumberFormat = TEXT_FORMAT
        .Value = Format$(businessDate, "dddd, mmmm d, yyyy")
    End With

    lastPaymentRow = WritePayments(report, 4, payments)
    lastRow = WriteProduction(report, lastPaymentRow + 2, production, titleRow)

    report.Columns(COL_CODE).ColumnWidth = 12
    report.Columns(COL_DESCRIPTION).ColumnWidth = 48
    report.Columns(COL_CATEGORY).ColumnWidth = 22
    report.Columns(COL_COUNT).ColumnWidth = 8
    report.Columns(COL_AMOUNT).ColumnWidth = 14
    report.Columns(COL_AVERAGE).ColumnWidth = 12

    SetPrintLayout report, lastRow, titleRow
    report.Activate
End Sub

' Writes the payment totals and returns the last row used.
Private Function WritePayments(ByVal report As Worksheet, ByVal firstRow As Long, ByVal payments As ListObject) As Long
    report.Cells(firstRow, COL_CODE).Value = "Payments"
    report.Cells(firstRow, COL_COUNT).Value = "Count"
    report.Cells(firstRow, COL_AMOUNT).Value = "Amount"
    FormatHeadingRow report, firstRow

    If payments.ListRows.Count = 0 Then
        report.Cells(firstRow + 1, COL_CODE).Value = "No row in mart_daily_payments for this date."
        WritePayments = firstRow + 1
        Exit Function
    End If

    WritePaymentLine report, firstRow + 1, "Patient payments", payments, "patient_payment_count", "patient_payment_amount"
    WritePaymentLine report, firstRow + 2, "Insurance payments", payments, "insurance_payment_count", "insurance_payment_amount"
    WritePaymentLine report, firstRow + 3, "Net collections", payments, "payment_count", "net_collections_amount"
    report.Range(report.Cells(firstRow + 3, COL_CODE), report.Cells(firstRow + 3, COL_AVERAGE)).Font.Bold = True

    WritePaymentLine report, firstRow + 5, "Income", payments, "", "income_amount"
    WritePaymentLine report, firstRow + 6, "Refunds", payments, "", "refund_amount"
    WritePaymentLine report, firstRow + 7, "Unmapped pay types (within patient payments)", payments, "other_payment_count", "other_payment_amount"

    WritePayments = firstRow + 7
End Function

Private Sub WritePaymentLine(ByVal report As Worksheet, ByVal rowNumber As Long, ByVal lineLabel As String, _
    ByVal payments As ListObject, ByVal countColumn As String, ByVal amountColumn As String)

    report.Cells(rowNumber, COL_CODE).Value = lineLabel
    If Len(countColumn) > 0 Then
        report.Cells(rowNumber, COL_COUNT).NumberFormat = COUNT_FORMAT
        report.Cells(rowNumber, COL_COUNT).Value = FirstValue(payments, countColumn)
    End If
    report.Cells(rowNumber, COL_AMOUNT).NumberFormat = MONEY_FORMAT
    report.Cells(rowNumber, COL_AMOUNT).Value = FirstValue(payments, amountColumn)
End Sub

Private Function FirstValue(ByVal source As ListObject, ByVal columnName As String) As Variant
    FirstValue = source.ListColumns(columnName).DataBodyRange.Cells(1, 1).Value
End Function

' Writes the procedure lines and returns the last row used. titleRow is the
' column heading row to repeat on each printed page, or 0 when there are no lines.
Private Function WriteProduction(ByVal report As Worksheet, ByVal firstRow As Long, _
    ByVal production As ListObject, ByRef titleRow As Long) As Long

    Dim lineCount As Long
    Dim firstLine As Long
    Dim lastLine As Long
    Dim totalRow As Long

    report.Cells(firstRow, COL_CODE).Value = "Production by procedure"
    report.Cells(firstRow, COL_CODE).Font.Bold = True

    lineCount = production.ListRows.Count
    If lineCount = 0 Then
        titleRow = 0
        report.Cells(firstRow + 1, COL_CODE).Value = "No rows in mart_daily_production_by_procedure for this date."
        WriteProduction = firstRow + 1
        Exit Function
    End If

    titleRow = firstRow + 1
    report.Cells(titleRow, COL_CODE).Value = "Code"
    report.Cells(titleRow, COL_DESCRIPTION).Value = "Description"
    report.Cells(titleRow, COL_CATEGORY).Value = "Category"
    report.Cells(titleRow, COL_COUNT).Value = "Qty"
    report.Cells(titleRow, COL_AMOUNT).Value = "Total fees"
    report.Cells(titleRow, COL_AVERAGE).Value = "Average fee"
    FormatHeadingRow report, titleRow

    firstLine = titleRow + 1
    lastLine = titleRow + lineCount
    CopyColumn production, "procedure_code", report, firstLine, COL_CODE, TEXT_FORMAT
    CopyColumn production, "procedure_description", report, firstLine, COL_DESCRIPTION, TEXT_FORMAT
    CopyColumn production, "procedure_category", report, firstLine, COL_CATEGORY, TEXT_FORMAT
    CopyColumn production, "procedure_quantity", report, firstLine, COL_COUNT, COUNT_FORMAT
    CopyColumn production, "total_fees", report, firstLine, COL_AMOUNT, MONEY_FORMAT
    CopyColumn production, "average_fee", report, firstLine, COL_AVERAGE, MONEY_FORMAT

    With report.Range(report.Cells(firstLine, COL_CODE), report.Cells(lastLine, COL_AVERAGE))
        .VerticalAlignment = xlTop
        .Columns(COL_DESCRIPTION).WrapText = True
        .EntireRow.AutoFit
    End With

    totalRow = lastLine + 1
    report.Cells(totalRow, COL_CODE).Value = "Total"
    report.Cells(totalRow, COL_COUNT).NumberFormat = COUNT_FORMAT
    report.Cells(totalRow, COL_COUNT).Value = _
        Application.WorksheetFunction.Sum(production.ListColumns("procedure_quantity").DataBodyRange)
    report.Cells(totalRow, COL_AMOUNT).NumberFormat = MONEY_FORMAT
    report.Cells(totalRow, COL_AMOUNT).Value = _
        Application.WorksheetFunction.Sum(production.ListColumns("total_fees").DataBodyRange)
    With report.Range(report.Cells(totalRow, COL_CODE), report.Cells(totalRow, COL_AVERAGE))
        .Font.Bold = True
        .Borders(xlEdgeTop).LineStyle = xlContinuous
    End With

    WriteProduction = totalRow
End Function

Private Sub CopyColumn(ByVal source As ListObject, ByVal columnName As String, ByVal report As Worksheet, _
    ByVal firstLine As Long, ByVal reportColumn As Long, ByVal cellFormat As String)

    Dim sourceCells As Range

    Set sourceCells = source.ListColumns(columnName).DataBodyRange
    With report.Cells(firstLine, reportColumn).Resize(sourceCells.Rows.Count, 1)
        .NumberFormat = cellFormat
        .Value = sourceCells.Value
    End With
End Sub

Private Sub FormatHeadingRow(ByVal report As Worksheet, ByVal rowNumber As Long)
    With report.Range(report.Cells(rowNumber, COL_CODE), report.Cells(rowNumber, COL_AVERAGE))
        .Font.Bold = True
        .Borders(xlEdgeBottom).LineStyle = xlContinuous
    End With
    report.Range(report.Cells(rowNumber, COL_COUNT), report.Cells(rowNumber, COL_AVERAGE)).HorizontalAlignment = xlRight
End Sub

Private Sub SetPrintLayout(ByVal report As Worksheet, ByVal lastRow As Long, ByVal titleRow As Long)
    With report.PageSetup
        .PrintArea = report.Range(report.Cells(1, COL_CODE), report.Cells(lastRow, COL_AVERAGE)).Address
        If titleRow > 0 Then
            .PrintTitleRows = report.Rows(titleRow).Address
        Else
            .PrintTitleRows = ""
        End If
        .Orientation = xlPortrait
        .Zoom = False
        .FitToPagesWide = 1
        .FitToPagesTall = False
        .LeftFooter = "Refreshed " & Format$(Now, "yyyy-mm-dd hh:nn")
        .RightFooter = "Page &P of &N"
    End With
End Sub

Private Function ReportSheet() As Worksheet
    Dim sheet As Worksheet

    For Each sheet In ThisWorkbook.Worksheets
        If sheet.Name = REPORT_SHEET Then
            Set ReportSheet = sheet
            Exit Function
        End If
    Next sheet

    Set sheet = ThisWorkbook.Worksheets.Add(Before:=ThisWorkbook.Worksheets(1))
    sheet.Name = REPORT_SHEET
    Set ReportSheet = sheet
End Function
