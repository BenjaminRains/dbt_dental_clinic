-- OpenDental Aging of A/R, as of 2026-10-12, same date as the mart snapshot.
-- Shifted from the 2026-09-30 Generate Query. Outer query sums the report columns.
-- No patient names. MySQL source database opendental.
--
-- Bucket windows (as-of 2026-10-12, 30-day blocks counted back from that date):
--   >90    TranDate <  2026-07-14
--   61-90  2026-07-14 <= TranDate <  2026-08-13
--   31-60  2026-08-13 <= TranDate <  2026-09-12
--   0-30   2026-09-12 <= TranDate <= 2026-10-12
-- PayPlanDue still uses ChargeDate <= 2026-10-22 (query day + 10). That column is not printed.
-- Credits apply to the oldest charges first.
-- A family is on the report when any bucket is at least 0.005.

SELECT
    COUNT(*) AS families,
    ROUND(SUM(Bal_0_30), 2) AS bal_0_30,
    ROUND(SUM(Bal_31_60), 2) AS bal_31_60,
    ROUND(SUM(Bal_61_90), 2) AS bal_61_90,
    ROUND(SUM(BalOver90), 2) AS bal_over_90,
    ROUND(SUM(BalTotal), 2) AS bal_total,
    ROUND(SUM(InsWoEst), 2) AS ins_wo_est,
    ROUND(SUM(InsPayEst), 2) AS ins_pay_est,
    ROUND(SUM(patient_portion), 2) AS patient_portion
FROM (
    SELECT
        guarAging.Bal_0_30,
        guarAging.Bal_31_60,
        guarAging.Bal_61_90,
        guarAging.BalOver90,
        guarAging.BalTotal,
        guarAging.InsWoEst,
        guarAging.InsPayEst,
        guarAging.BalTotal - guarAging.InsPayEst - guarAging.InsWoEst AS patient_portion
    FROM (
        SELECT
            tSums.PatNum,
            ROUND(CASE WHEN tSums.TotalCredits >= tSums.ChargesOver90 THEN 0 ELSE tSums.ChargesOver90 - tSums.TotalCredits END, 3) BalOver90,
            ROUND(CASE WHEN tSums.TotalCredits <= tSums.ChargesOver90 THEN tSums.Charges_61_90 WHEN tSums.ChargesOver90 + tSums.Charges_61_90 <= tSums.TotalCredits THEN 0 ELSE tSums.ChargesOver90 + tSums.Charges_61_90 - tSums.TotalCredits END, 3) Bal_61_90,
            ROUND(CASE WHEN tSums.TotalCredits < tSums.ChargesOver90 + tSums.Charges_61_90 THEN tSums.Charges_31_60 WHEN tSums.ChargesOver90 + tSums.Charges_61_90 + tSums.Charges_31_60 <= tSums.TotalCredits THEN 0 ELSE tSums.ChargesOver90 + tSums.Charges_61_90 + tSums.Charges_31_60 - tSums.TotalCredits END, 3) Bal_31_60,
            ROUND(CASE WHEN tSums.TotalCredits < tSums.ChargesOver90 + tSums.Charges_61_90 + tSums.Charges_31_60 THEN tSums.Charges_0_30 WHEN tSums.ChargesOver90 + tSums.Charges_61_90 + tSums.Charges_31_60 + tSums.Charges_0_30 <= tSums.TotalCredits THEN 0 ELSE tSums.ChargesOver90 + tSums.Charges_61_90 + tSums.Charges_31_60 + tSums.Charges_0_30 - tSums.TotalCredits END, 3) Bal_0_30,
            ROUND(tSums.BalTotal, 3) BalTotal,
            ROUND(tSums.InsWoEst, 3) InsWoEst,
            ROUND(tSums.InsPayEst, 3) InsPayEst,
            ROUND(tSums.PayPlanDue, 3) PayPlanDue
        FROM (
            SELECT
                transSums.PatNum,
                GREATEST(transSums.ChargesOver90, 0) ChargesOver90,
                GREATEST(transSums.Charges_61_90, 0) Charges_61_90,
                GREATEST(transSums.Charges_31_60, 0) Charges_31_60,
                GREATEST(transSums.Charges_0_30, 0) Charges_0_30,
                transSums.TotalCredits - LEAST(transSums.ChargesOver90, 0) - LEAST(transSums.Charges_61_90, 0) - LEAST(transSums.Charges_31_60, 0) - LEAST(transSums.Charges_0_30, 0) TotalCredits,
                transSums.BalTotal,
                transSums.InsWoEst,
                transSums.InsPayEst,
                transSums.PayPlanDue
            FROM (
                SELECT
                    p.Guarantor PatNum,
                    SUM(CASE WHEN ((trans.TranAmount > 0 AND trans.TranType != 'PatPay') OR trans.TranType IN ('WriteoffOrig', 'SumByProcAndDate')) AND trans.TranDate < '2026-07-14' THEN trans.TranAmount ELSE 0 END) ChargesOver90,
                    SUM(CASE WHEN ((trans.TranAmount > 0 AND trans.TranType != 'PatPay') OR trans.TranType IN ('WriteoffOrig', 'SumByProcAndDate')) AND trans.TranDate < '2026-08-13' AND trans.TranDate >= '2026-07-14' THEN trans.TranAmount ELSE 0 END) Charges_61_90,
                    SUM(CASE WHEN ((trans.TranAmount > 0 AND trans.TranType != 'PatPay') OR trans.TranType IN ('WriteoffOrig', 'SumByProcAndDate')) AND trans.TranDate < '2026-09-12' AND trans.TranDate >= '2026-08-13' THEN trans.TranAmount ELSE 0 END) Charges_31_60,
                    SUM(CASE WHEN ((trans.TranAmount > 0 AND trans.TranType != 'PatPay') OR trans.TranType IN ('WriteoffOrig', 'SumByProcAndDate')) AND trans.TranDate <= '2026-10-12' AND trans.TranDate >= '2026-09-12' THEN trans.TranAmount ELSE 0 END) Charges_0_30,
                    -SUM(CASE WHEN ((trans.TranAmount < 0 AND NOT (trans.TranType IN ('WriteoffOrig', 'SumByProcAndDate'))) OR trans.TranType = 'PatPay') AND trans.TranDate <= '2026-10-12' THEN trans.TranAmount ELSE 0 END) TotalCredits,
                    SUM(CASE WHEN trans.TranAmount != 0 AND trans.TranDate <= '2026-10-12' THEN trans.TranAmount ELSE 0 END) BalTotal,
                    SUM(trans.InsWoEst) InsWoEst,
                    SUM(trans.InsPayEst) InsPayEst,
                    SUM(trans.PayPlanAmount) PayPlanDue
                FROM (
                    SELECT
                        (CASE WHEN tranbyproc.AgedProcNum = 0 THEN tranbyproc.TranType ELSE 'SumByProcAndDate' END) TranType,
                        0 PriKey,
                        tranbyproc.PatNum,
                        (CASE WHEN tranbyproc.AgedProcNum != 0 AND SUM(tranbyproc.TranAmount) < 0 THEN tranbyproc.AgedProcDate ELSE tranbyproc.TranDate END) TranDate,
                        SUM(tranbyproc.TranAmount) TranAmount,
                        SUM(tranbyproc.PayPlanAmount) PayPlanAmount,
                        SUM(tranbyproc.InsWoEst) InsWoEst,
                        SUM(tranbyproc.InsPayEst) InsPayEst
                    FROM (
                        SELECT 'Proc' TranType, pl.ProcNum PriKey, pl.PatNum, pl.ProcDate TranDate, pl.ProcFee * (pl.UnitQty + pl.BaseUnits) TranAmount,
                            0 PayPlanAmount, 0 InsWoEst, 0 InsPayEst, 0 AgedProcNum, '0001-01-01' AgedProcDate
                        FROM procedurelog pl
                        WHERE pl.ProcStatus = 2 AND pl.ProcFee != 0
                        UNION ALL
                        SELECT 'Claimproc' TranType, cp.ClaimProcNum PriKey, cp.PatNum, cp.DateCP TranDate,
                            (CASE WHEN cp.Status != 0 THEN (CASE WHEN cp.PayPlanNum = 0 THEN -cp.InsPayAmt ELSE 0 END) - cp.WriteOff ELSE 0 END) TranAmount,
                            0 PayPlanAmount,
                            (CASE WHEN cp.ProcDate <= '2026-10-12' AND (cp.Status = 0 OR (cp.Status = 1 AND cp.DateCP > '2026-10-12')) THEN cp.WriteOff ELSE 0 END) InsWoEst,
                            (CASE WHEN cp.ProcDate <= '2026-10-12' AND (cp.Status = 0 OR (cp.Status = 1 AND cp.DateCP > '2026-10-12')) THEN cp.InsPayEst ELSE 0 END) InsPayEst,
                            0 AgedProcNum, '0001-01-01' AgedProcDate
                        FROM claimproc cp
                        LEFT JOIN procedurelog p ON cp.ProcNum = p.ProcNum
                        LEFT JOIN payplan pp ON cp.PayPlanNum = pp.PayPlanNum
                        WHERE cp.Status IN (0, 1, 4, 5, 7) AND (cp.ProcNum = 0 OR p.ProcStatus = 2)
                        HAVING TranAmount != 0 OR InsWoEst != 0 OR InsPayEst != 0
                        UNION ALL
                        SELECT 'Adj' TranType, a.AdjNum PriKey, a.PatNum, a.AdjDate TranDate, a.AdjAmt TranAmount,
                            0 PayPlanAmount, 0 InsWoEst, 0 InsPayEst, a.ProcNum AgedProcNum, a.ProcDate AgedProcDate
                        FROM adjustment a
                        WHERE a.AdjAmt != 0
                        UNION ALL
                        SELECT 'PatPay' TranType, ps.SplitNum PriKey, ps.PatNum, ps.DatePay TranDate, -ps.SplitAmt TranAmount,
                            (CASE WHEN ps.PayPlanNum != 0 AND COALESCE(pp.IsClosed, 0) = 0 THEN -ps.SplitAmt ELSE 0 END) PayPlanAmount,
                            0 InsWoEst, 0 InsPayEst, 0 AgedProcNum, '0001-01-01' AgedProcDate
                        FROM paysplit ps
                        LEFT JOIN payplan pp ON ps.PayPlanNum = pp.PayPlanNum
                        WHERE ps.SplitAmt != 0 AND ps.UnearnedType NOT IN (439, 598) AND ps.DatePay <= '2026-10-12'
                        UNION ALL
                        SELECT 'PPCharge' TranType, ppc.PayPlanChargeNum PriKey, ppc.Guarantor PatNum, ppc.ChargeDate TranDate,
                            0 TranAmount, ppc.Principal + ppc.Interest PayPlanAmount,
                            0 InsWoEst, 0 InsPayEst, 0 AgedProcNum, '0001-01-01' AgedProcDate
                        FROM payplancharge ppc
                        INNER JOIN payplan pp ON ppc.PayPlanNum = pp.PayPlanNum
                        WHERE ppc.ChargeDate <= '2026-10-22' AND ppc.ChargeType = 0 AND NOT pp.IsClosed
                            AND ppc.Principal + ppc.Interest != 0 AND pp.PlanNum = 0
                        UNION ALL
                        SELECT 'PPCComplete' TranType, ppc.PayPlanChargeNum PriKey,
                            (CASE WHEN ppc.ChargeType = 0 THEN ppc.Guarantor ELSE pp.PatNum END) PatNum,
                            ppc.ChargeDate TranDate,
                            (CASE WHEN ppc.ChargeType != 0 THEN -ppc.Principal WHEN pp.PlanNum = 0 THEN ppc.Principal + ppc.Interest ELSE 0 END) TranAmount,
                            0 PayPlanAmount, 0 InsWoEst, 0 InsPayEst,
                            (CASE WHEN ppc.ChargeType = 1 THEN ppc.ProcNum ELSE 0 END) AgedProcNum,
                            pl.ProcDate AgedProcDate
                        FROM payplancharge ppc
                        LEFT JOIN payplan pp ON pp.PayPlanNum = ppc.PayPlanNum
                        LEFT JOIN procedurelog pl ON pl.ProcNum = ppc.ProcNum
                        WHERE ppc.ChargeDate <= '2026-10-12'
                        UNION ALL
                        SELECT 'PayPlanLink' TranType, ppl.PayPlanLinkNum PriKey, prodlink.PatNum PatNum, DATE(ppl.SecDateTEntry) TranDate,
                            (CASE WHEN ppl.AmountOverride = 0 THEN -prodlink.Fee ELSE -ppl.AmountOverride END) TranAmount,
                            0 PayPlanAmount, 0 InsWoEst, 0 InsPayEst, prodlink.ProcNum AgedProcNum, prodlink.AgeDate AgedProcDate
                        FROM payplanlink ppl
                        LEFT JOIN (
                            SELECT procedurelog.PatNum,
                                (procedurelog.ProcFee * (procedurelog.UnitQty + procedurelog.BaseUnits)
                                    + COALESCE(procAdj.AdjAmt, 0) + COALESCE(procClaimProc.InsPay, 0)
                                    + COALESCE(procClaimProc.WriteOff, 0)
                                    + COALESCE(procSplit.SplitAmt, 0)
                                ) Fee,
                                payplanlink.PayPlanLinkNum LinkNum, procedurelog.ProcNum, procedurelog.ProcDate AgeDate
                            FROM payplanlink
                            INNER JOIN payplan ON payplanlink.PayPlanNum = payplan.PayPlanNum
                            INNER JOIN procedurelog ON procedurelog.ProcNum = payplanlink.FKey
                                AND payplanlink.LinkType = 2
                                AND procedurelog.ProcStatus = 2
                            LEFT JOIN (
                                SELECT SUM(adjustment.AdjAmt) AdjAmt, adjustment.ProcNum, adjustment.PatNum, adjustment.ProvNum, adjustment.ClinicNum
                                FROM adjustment
                                GROUP BY adjustment.ProcNum, adjustment.PatNum, adjustment.ProvNum, adjustment.ClinicNum
                            ) procAdj ON procAdj.ProcNum = procedurelog.ProcNum
                                AND procAdj.PatNum = procedurelog.PatNum
                                AND procAdj.ProvNum = procedurelog.ProvNum
                                AND procAdj.ClinicNum = procedurelog.ClinicNum
                            LEFT JOIN (
                                SELECT SUM(COALESCE((CASE WHEN claimproc.Status IN (1, 4, 7) THEN claimproc.InsPayAmt
                                    WHEN claimproc.InsEstTotalOverride != -1 THEN claimproc.InsEstTotalOverride
                                    ELSE claimproc.InsPayEst END), 0) * -1) InsPay,
                                    SUM(COALESCE((CASE WHEN claimproc.Status IN (1, 4, 7) THEN claimproc.WriteOff
                                        WHEN claimproc.WriteOffEstOverride != -1 THEN claimproc.WriteOffEstOverride
                                        WHEN claimproc.WriteOffEst != -1 THEN claimproc.WriteOffEst
                                        ELSE 0 END), 0) * -1) WriteOff,
                                    claimproc.ProcNum
                                FROM claimproc
                                WHERE claimproc.Status != 2
                                GROUP BY claimproc.ProcNum
                            ) procClaimProc ON procClaimProc.ProcNum = procedurelog.ProcNum
                            LEFT JOIN (
                                SELECT SUM(paysplit.SplitAmt) * -1 SplitAmt, paysplit.ProcNum
                                FROM paysplit
                                WHERE paysplit.PayPlanNum = 0
                                GROUP BY paysplit.ProcNum
                            ) procSplit ON procSplit.ProcNum = procedurelog.ProcNum
                            UNION ALL
                            SELECT adjustment.PatNum, adjustment.AdjAmt + COALESCE(adjSplit.SplitAmt, 0) Fee, payplanlink.PayPlanLinkNum LinkNum,
                                0 ProcNum, adjustment.AdjDate AgeDate
                            FROM payplanlink
                            INNER JOIN adjustment ON adjustment.AdjNum = payplanlink.FKey AND payplanlink.LinkType = 1
                            LEFT JOIN (
                                SELECT SUM(COALESCE(paysplit.SplitAmt, 0)) * -1 SplitAmt, paysplit.AdjNum
                                FROM paysplit
                                WHERE paysplit.PayPlanNum = 0
                                GROUP BY paysplit.AdjNum
                            ) adjSplit ON adjSplit.AdjNum = adjustment.AdjNum
                        ) prodlink ON prodlink.LinkNum = ppl.PayPlanLinkNum
                    ) tranbyproc
                    GROUP BY tranbyproc.PatNum, tranbyproc.AgedProcNum, tranbyproc.TranDate,
                        (CASE WHEN tranbyproc.AgedProcNum = 0 THEN tranbyproc.TranType ELSE 'SumByProcAndDate' END),
                        (CASE WHEN tranbyproc.AgedProcNum = 0 AND tranbyproc.TranAmount >= 0 THEN 'credit'
                              WHEN tranbyproc.AgedProcNum = 0 AND tranbyproc.TranAmount < 0 THEN 'charge'
                              ELSE 'SumByProcAndDate' END)
                ) trans
                INNER JOIN patient p ON p.PatNum = trans.PatNum
                GROUP BY p.Guarantor
            ) transSums
        ) tSums
    ) guarAging
    INNER JOIN patient ON patient.PatNum = guarAging.PatNum
    WHERE guarAging.BalOver90 >= 0.005
       OR guarAging.Bal_61_90 >= 0.005
       OR guarAging.Bal_31_60 >= 0.005
       OR guarAging.Bal_0_30 >= 0.005
) report;
