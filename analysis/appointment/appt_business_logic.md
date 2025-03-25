# Appointment Status Definitions

## Status Codes

| Status Code | Label | Description | Business Impact |
|------------|-------|-------------|-----------------|
| 1 | Scheduled | Active appointments that have been scheduled but not yet completed | 
- Used for current/future appointments
- Appears on schedule views
- Can be modified/rescheduled |
| 2 | Completed | Appointments that have been fulfilled and services rendered | 
- Used for billing/reporting
- Part of patient history
- Cannot be modified |
| 3 | Unknown | Status code requiring investigation | 
- Needs business verification
- May indicate data quality issues
- Should be monitored for frequency |
| 5 | Broken/Missed | Appointments where patient did not show up or cancelled too late | 
- Impacts productivity metrics
- May trigger follow-up workflows
- Used for no-show reporting |
| 6 | Unscheduled | Treatment planned but not yet scheduled | 
- Used for treatment planning
- Requires follow-up for scheduling
- Part of pending treatment reports |

## Procedure Codes

1. **Missed Appointments**
   - Code: D9986/626
   - Currently not used in system
   - Missed appointments tracked via Status 5 instead
   - Original procedure descriptions are retained when appointment is marked as missed

2. **Cancelled Appointments**
   - Code: D9987/627 (appears as '09987' in data)
   - Only found in Status 2 (Completed) appointments
   - Always appears alongside other procedure codes
   - Represents 0.46% of completed appointments
   - Used for tracking cancelled appointments that were rescheduled

## Business Rules

1. Status Transitions
   - New appointments default to Status 1 (Scheduled)
   - Only Scheduled (1) appointments can be marked as Completed (2)
   - Only Scheduled (1) appointments can be marked as Broken/Missed (5)
   - Unscheduled (6) appointments must be converted to Scheduled (1) when date/time is assigned

2. Reporting Implications
   - Completion Rate = Status 2 / (Status 2 + Status 5)
   - No-Show Rate = Status 5 / (Status 2 + Status 5)
   - Future Schedule Load = Count of Status 1
   - Treatment Plan Backlog = Count of Status 6

3. Data Quality Rules
   - Status 3 requires investigation and should not be used for new appointments
   - All appointments must have a valid status code
   - Historical appointments should not have Status 1 (Scheduled)
   - Future appointments should not have Status 2 (Completed)

4. Procedure Code Rules
   - Cancelled appointments (09987) remain in Status 2 with cancellation code added
   - Missed appointments do not use procedure code D9986/626
   - Status 5 (Broken/Missed) retains original planned procedure descriptions
   - Cancellation codes always appear with original planned procedures

## Usage in Analytics

1. Schedule Analysis
   ```sql
   -- Active schedule view
   SELECT *
   FROM stg_opendental__appointment
   WHERE appointment_status = 1  -- Scheduled
   AND appointment_datetime >= CURRENT_DATE
   ```

2. Completion Metrics
   ```sql
   -- Monthly completion rate
   SELECT 
     DATE_TRUNC('month', appointment_datetime) as month,
     COUNT(CASE WHEN appointment_status = 2 THEN 1 END) as completed,
     COUNT(CASE WHEN appointment_status = 5 THEN 1 END) as broken,
     ROUND(
       COUNT(CASE WHEN appointment_status = 2 THEN 1 END)::FLOAT / 
       NULLIF(COUNT(CASE WHEN appointment_status IN (2,5) THEN 1 END), 0) * 100,
       2
     ) as completion_rate
   FROM stg_opendental__appointment
   GROUP BY 1
   ORDER BY 1
   ```

3. Treatment Plan Follow-up
   ```sql
   -- Unscheduled treatment plans
   SELECT *
   FROM stg_opendental__appointment
   WHERE appointment_status = 6  -- Unscheduled
   ORDER BY date_timestamp DESC
   ```

## Data Quality Monitoring

1. Invalid Status Checks
   ```sql
   -- Find appointments with invalid status combinations
   SELECT 
     appointment_datetime::DATE as date,
     COUNT(*) as invalid_count
   FROM stg_opendental__appointment
   WHERE (
     appointment_datetime < CURRENT_DATE AND appointment_status = 1  -- Past scheduled
     OR
     appointment_datetime > CURRENT_DATE AND appointment_status = 2  -- Future completed
   )
   GROUP BY 1
   ORDER BY 1
   ```

2. Status 3 Investigation
   ```sql
   -- Monitor Status 3 appointments
   SELECT 
     DATE_TRUNC('month', appointment_datetime) as month,
     COUNT(*) as status_3_count,
     COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
   FROM stg_opendental__appointment
   WHERE appointment_status = 3
   GROUP BY 1
   ORDER BY 1
   ```

3. Cancelled Appointment Checks
   ```sql
   -- Find cancelled appointments
   SELECT 
     appointment_datetime::DATE as date,
     COUNT(*) as cancelled_count
   FROM stg_opendental__appointment
   WHERE appointment_status = 2 AND procedure_code = '09987'
   GROUP BY 1
   ORDER BY 1
   ```

## Notes

1. Historical Context
   - Status codes are native to OpenDental system
   - Status 3 may be legacy/deprecated but appears in historical data
   - System may have additional status codes not yet observed in data

2. Maintenance Requirements
   - Regular monitoring of Status 3 frequency
   - Periodic validation of completion rates
   - Review of unscheduled treatment plans aging
   - Verification of status transitions

3. Future Considerations
   - May need to add new status codes as system evolves
   - Consider tracking status change history
   - Potential for status-based automation workflows

4. Procedure Code Patterns
   - Cancellation tracking uses procedure codes (09987)
   - Missed appointment tracking uses status codes (Status 5)
   - System does not currently utilize D9986/626 for missed appointments
   - Cancelled appointments remain in completed status with added procedure code

5. Recommendations
   - Consider standardizing missed appointment tracking (either status or procedure code)
   - Review process of marking cancelled appointments as completed
   - Consider adding reason codes for Status 5 appointments
   - Evaluate if cancelled appointments should remain in Status 2