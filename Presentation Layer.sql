USE [KPI Database]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_unit_security_dashboard]') AND type in (N'V'))
		DROP VIEW KPI.vw_unit_security_dashboard
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_unit_security_dashboard_month]') AND type in (N'V'))
		DROP VIEW KPI.vw_unit_security_dashboard_month
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_unit_security_dashboard_week]') AND type in (N'V'))
		DROP VIEW KPI.vw_unit_security_dashboard_week
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_unit_security_dashboard_day]') AND type in (N'V'))
		DROP VIEW KPI.vw_unit_security_dashboard_day
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_unit_security_dashboard_month_end]') AND type in (N'V'))
		DROP VIEW KPI.vw_unit_security_dashboard_month_end
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_security_dashboard_month]') AND type in (N'V'))
		DROP VIEW KPI.vw_security_dashboard_month
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_security_dashboard_week]') AND type in (N'V'))
		DROP VIEW KPI.vw_security_dashboard_week
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_security_dashboard_day]') AND type in (N'V'))
		DROP VIEW KPI.vw_security_dashboard_day
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_security_dashboard_month_end]') AND type in (N'V'))
		DROP VIEW KPI.vw_security_dashboard_month_end
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_phishing_templates]') AND type in (N'V'))
		DROP VIEW KPI.vw_phishing_templates
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_phishing_template_types]') AND type in (N'V'))
		DROP VIEW KPI.vw_phishing_template_types
IF EXISTS (SELECT * FROM [KPI database].sys.schemas WHERE name = 'KPI')
	DROP SCHEMA KPI
GO
CREATE SCHEMA KPI
GO

CREATE OR ALTER VIEW KPI.vw_unit_security_dashboard
AS
WITH base_units AS (
    SELECT DISTINCT u.unit_key, u.unit_code, u.unit_naam
		FROM DWH.dim_unit u
		JOIN DWH.bridge_user_unit bu ON bu.unit_key = u.unit_key AND bu.is_current = 1
		WHERE u.is_current = 1),

	risk_agg AS (
		SELECT	u.unit_key, MIN(dd.[date]) AS periode_start, MAX(dd.[date]) AS periode_einde, AVG(CAST(f.current_risk_score AS float)) AS avg_risk_score_simple, 
				SUM(CAST(f.current_risk_score AS float) * ISNULL(bu.allocation_wt, 1.0)) / NULLIF(SUM(ISNULL(bu.allocation_wt, 1.0)), 0) AS avg_risk_score_weighted,
				AVG(CAST(f.phish_prone_pct AS float)) AS avg_phish_prone_pct
			FROM DWH.fact_user_security_snapshot f
			JOIN DWH.dim_date dd ON dd.date_key = f.snapshot_date_key
			JOIN DWH.bridge_user_unit bu ON bu.user_key = f.user_key AND f.snapshot_date_key BETWEEN bu.start_date_key AND ISNULL(bu.end_date_key, 99991231)
			JOIN DWH.dim_unit u ON u.unit_key = bu.unit_key AND u.is_current = 1
			WHERE dd.[date] BETWEEN DATEADD(DAY, -30, CAST(GETDATE() AS date)) AND CAST(GETDATE() AS date)
			GROUP BY u.unit_key),

	phish_agg AS (
		SELECT u.unit_key, SUM(COALESCE(fpr.delivered_count, 0) * ISNULL(bu.allocation_wt, 1.0)) AS delivered_total, SUM(COALESCE(fpr.reported_count,  0) * ISNULL(bu.allocation_wt, 1.0)) AS reported_total
			FROM DWH.fact_pst_recipient_result fpr
			JOIN DWH.bridge_user_unit bu ON bu.user_key = fpr.user_key AND bu.is_current = 1
			JOIN DWH.dim_unit u ON u.unit_key = bu.unit_key AND u.is_current = 1
			WHERE CAST(fpr.load_ts AS date) BETWEEN DATEADD(DAY, -30, CAST(GETDATE() AS date)) AND CAST(GETDATE() AS date)
		GROUP BY u.unit_key),

	train_all AS (
		SELECT	u.unit_key, ISNULL(bu.allocation_wt, 1.0) AS wt, CASE WHEN UPPER(LTRIM(RTRIM(fte.[status]))) = 'PASSED' THEN 1 ELSE 0 END AS is_passed, 
				CASE WHEN UPPER(dc.campaign_type) = 'POLICY' THEN 1 WHEN UPPER(dc.campaign_type) = 'TRAINING' THEN 0 ELSE 0 END AS is_policy
			FROM DWH.fact_training_enrollment fte 
			JOIN DWH.dim_campaign dc ON dc.campaign_key = fte.campaign_key AND dc.is_current = 1
			JOIN DWH.bridge_user_unit bu ON bu.user_key = fte.user_key AND bu.is_current = 1
			JOIN DWH.dim_unit u ON u.unit_key = bu.unit_key AND u.is_current = 1),

	train_agg AS (
		SELECT unit_key, SUM(wt) AS total_enrollments, SUM(wt * is_passed) AS passed_enrollments
			FROM train_all
			WHERE is_policy = 0
			GROUP BY unit_key),

	policy_agg AS (
		SELECT unit_key, SUM(wt) AS total_policy_enrollments, SUM(wt * is_passed) AS passed_policy_enrollments
			FROM train_all
			WHERE is_policy = 1
			GROUP BY unit_key)

SELECT	b.unit_naam, r.periode_start, r.periode_einde, CAST(ROUND(r.avg_risk_score_simple,
 2) AS DECIMAL(10,2)) AS avg_risk_score_simple,
		CAST(ROUND(r.avg_risk_score_weighted, 2) AS DECIMAL(10,2)) AS avg_risk_score_weighted, CAST(ROUND(r.avg_phish_prone_pct, 2) AS DECIMAL(10,2)) AS avg_phish_prone_pct,
		CAST(ROUND(p.delivered_total, 2) AS DECIMAL(10,2)) AS delivered_total, CAST(ROUND(p.reported_total,  2) AS DECIMAL(10,2)) AS reported_total,
		CAST(ROUND(CASE WHEN p.delivered_total > 0 THEN 100.0 * p.reported_total / p.delivered_total ELSE NULL END, 2) AS DECIMAL(10,2)) AS reported_pct_phishing,
		CAST(ROUND(t.total_enrollments,   2) AS DECIMAL(10,2)) AS total_enrollments, CAST(ROUND(t.passed_enrollments,  2) AS DECIMAL(10,2)) AS passed_enrollments,
		CAST(ROUND(CASE WHEN t.total_enrollments > 0 THEN 100.0 * t.passed_enrollments / t.total_enrollments ELSE NULL END, 2) AS DECIMAL(10,2)) AS passed_pct_training,
		CAST(ROUND(pa.total_policy_enrollments,  2) AS DECIMAL(10,2)) AS total_policy_enrollments, CAST(ROUND(pa.passed_policy_enrollments, 2) AS DECIMAL(10,2)) AS passed_policy_enrollments,
		CAST(ROUND(CASE WHEN pa.total_policy_enrollments > 0 THEN 100.0 * pa.passed_policy_enrollments / pa.total_policy_enrollments ELSE NULL END, 2) AS DECIMAL(10,2)) AS passed_pct_policy
	FROM base_units b
	LEFT JOIN risk_agg   r  ON r.unit_key  = b.unit_key
	LEFT JOIN phish_agg  p  ON p.unit_key  = b.unit_key
	LEFT JOIN train_agg  t  ON t.unit_key  = b.unit_key
	LEFT JOIN policy_agg pa ON pa.unit_key = b.unit_key;		
GO

CREATE OR ALTER VIEW [KPI].[vw_unit_security_dashboard_month]
AS
WITH PhishSnapshotMonthly AS (
    -- Gemiddelde phish prone & risk per unit per maand (op load_ts van fact)
    SELECT buu.unit_key, d.[year], d.[month], AVG(f.phish_prone_pct)    AS avg_phish_prone_pct, AVG(f.current_risk_score) AS avg_risk_score
		FROM DWH.fact_user_security_snapshot f JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY buu.unit_key, d.[year], d.[month]),

PhishResponseMonthly AS (
    -- Maandelijkse reported/delivered per unit (op load_ts van fact, alleen phishing)
    SELECT buu.unit_key, d.[year], d.[month], SUM(f.reported_count)  AS reported_month, SUM(f.delivered_count) AS delivered_month
		FROM DWH.fact_pst_recipient_result f 
		JOIN DWH.dim_campaign c ON f.campaign_key = c.campaign_key AND c.campaign_type = 'phishing'
		JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY buu.unit_key, d.[year], d.[month]),

TrainingMonthly AS (
    -- Maandelijkse training-enrollments/completions (op load_ts van fact)
    SELECT buu.unit_key, d.[year], d.[month], COUNT(*) AS enroll_month, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_month
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'training'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key   = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
    GROUP BY buu.unit_key, d.[year], d.[month]),

PolicyMonthly AS (
    -- Maandelijkse policy-enrollments/completions (op load_ts van fact)
    SELECT buu.unit_key, d.[year], d.[month], COUNT(*) AS enroll_month, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_month
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'policy'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key   = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
		GROUP BY buu.unit_key, d.[year], d.[month]),

Base AS (
    -- alle unit/year/month combinaties waar iéts gebeurt
    SELECT unit_key, [year], [month] FROM PhishSnapshotMonthly
    UNION
    SELECT unit_key, [year], [month] FROM PhishResponseMonthly
    UNION
    SELECT unit_key, [year], [month] FROM TrainingMonthly
    UNION
    SELECT unit_key, [year], [month] FROM PolicyMonthly
	)

SELECT u.unit_code,u.unit_naam,b.[year],b.[month],CAST(ps.avg_phish_prone_pct AS DECIMAL(9,2)) AS avg_phish_prone,CAST(ps.avg_risk_score AS DECIMAL(9,2)) AS avg_risk_score,
    -- RESPONSE
    CAST(1.0 * SUM(COALESCE(pr.reported_month, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(pr.delivered_month, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_response_cum,
    -- TRAINING
    CAST(1.0 * SUM(COALESCE(tr.completed_month, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(tr.enroll_month, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_training_completed_cum,
    -- POLICY
    CAST(1.0 * SUM(COALESCE(po.completed_month, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(po.enroll_month, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_policy_read_cum
FROM Base b
	JOIN DWH.dim_unit u ON u.unit_key = b.unit_key
	LEFT JOIN PhishSnapshotMonthly ps ON ps.unit_key = b.unit_key AND ps.[year] = b.[year] AND ps.[month] = b.[month]
	LEFT JOIN PhishResponseMonthly pr ON pr.unit_key = b.unit_key AND pr.[year] = b.[year] AND pr.[month]  = b.[month]
	LEFT JOIN TrainingMonthly tr ON tr.unit_key = b.unit_key AND tr.[year] = b.[year] AND tr.[month] = b.[month]
	LEFT JOIN PolicyMonthly po ON po.unit_key = b.unit_key AND po.[year] = b.[year] AND po.[month] = b.[month];
GO

CREATE OR ALTER VIEW [KPI].[vw_unit_security_dashboard_week]
AS
WITH PhishSnapshotWeekly AS (
    -- Gemiddelde phish prone & risk per unit per week (op load_ts van fact)
    SELECT buu.unit_key, d.[year], d.[week], AVG(f.phish_prone_pct) AS avg_phish_prone_pct, AVG(f.current_risk_score) AS avg_risk_score
		FROM DWH.fact_user_security_snapshot f JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY buu.unit_key, d.[year], d.[week]),

PhishResponseWeekly AS (
    -- Wekelijkse reported/delivered per unit (op load_ts van fact, alleen phishing)
    SELECT buu.unit_key, d.[year], d.[week], SUM(f.reported_count)  AS reported_week, SUM(f.delivered_count) AS delivered_week
		FROM DWH.fact_pst_recipient_result f 
		JOIN DWH.dim_campaign c ON f.campaign_key = c.campaign_key AND c.campaign_type = 'phishing'
		JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY buu.unit_key, d.[year], d.[week]),

TrainingWeekly AS (
    -- Wekelijkse training-enrollments/completions (op load_ts van fact)
    SELECT buu.unit_key, d.[year], d.[week], COUNT(*) AS enroll_week, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_week
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'training'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key   = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
    GROUP BY buu.unit_key, d.[year], d.[week]),

PolicyWeekly AS (
    -- Wekelijkse policy-enrollments/completions (op load_ts van fact)
    SELECT buu.unit_key, d.[year], d.[week], COUNT(*) AS enroll_week, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_week
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'policy'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key   = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
		GROUP BY buu.unit_key, d.[year], d.[week]),

Base AS (
    -- alle unit/year/month combinaties waar iéts gebeurt
    SELECT unit_key, [year], [week] FROM PhishSnapshotWeekly
    UNION
    SELECT unit_key, [year], [week] FROM PhishResponseWeekly
    UNION
    SELECT unit_key, [year], [week] FROM TrainingWeekly
    UNION
    SELECT unit_key, [year], [week] FROM PolicyWeekly
	)

SELECT u.unit_code,u.unit_naam,b.[year],b.[week],CAST(ps.avg_phish_prone_pct AS DECIMAL(9,2)) AS avg_phish_prone,CAST(ps.avg_risk_score AS DECIMAL(9,2)) AS avg_risk_score,
    -- RESPONSE
    CAST(1.0 * SUM(COALESCE(pr.reported_week, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(pr.delivered_week, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_response_cum,
    -- TRAINING
    CAST(1.0 * SUM(COALESCE(tr.completed_week, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(tr.enroll_week, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_training_completed_cum,
    -- POLICY
    CAST(1.0 * SUM(COALESCE(po.completed_week, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(po.enroll_week, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_policy_read_cum
FROM Base b
	JOIN DWH.dim_unit u ON u.unit_key = b.unit_key
	LEFT JOIN PhishSnapshotWeekly ps ON ps.unit_key = b.unit_key AND ps.[year] = b.[year] AND ps.[week] = b.[week]
	LEFT JOIN PhishResponseWeekly pr ON pr.unit_key = b.unit_key AND pr.[year] = b.[year] AND pr.[week]  = b.[week]
	LEFT JOIN TrainingWeekly tr ON tr.unit_key = b.unit_key AND tr.[year] = b.[year] AND tr.[week] = b.[week]
	LEFT JOIN PolicyWeekly po ON po.unit_key = b.unit_key AND po.[year] = b.[year] AND po.[week] = b.[week];
GO

CREATE OR ALTER VIEW [KPI].[vw_unit_security_dashboard_day]
AS
WITH PhishSnapshotdayly AS (
    -- Gemiddelde phish prone & risk per unit per dag (op load_ts van fact)
    SELECT buu.unit_key, d.[year], d.[month], d.[day], AVG(f.phish_prone_pct) AS avg_phish_prone_pct, AVG(f.current_risk_score) AS avg_risk_score
		FROM DWH.fact_user_security_snapshot f JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY buu.unit_key, d.[year], d.[month], d.[day]),

PhishResponseDayly AS (
    -- Dagelijkse reported/delivered per unit (op load_ts van fact, alleen phishing)
    SELECT buu.unit_key, d.[year], d.[month],d.[day], SUM(f.reported_count)  AS reported_day, SUM(f.delivered_count) AS delivered_day
		FROM DWH.fact_pst_recipient_result f 
		JOIN DWH.dim_campaign c ON f.campaign_key = c.campaign_key AND c.campaign_type = 'phishing'
		JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY buu.unit_key, d.[year], d.[month], d.[day]),

TrainingDayly AS (
    -- Dagelijkse training-enrollments/completions (op load_ts van fact)
    SELECT buu.unit_key, d.[year], d.[month],d.[day], COUNT(*) AS enroll_day, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_day
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'training'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key   = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
    GROUP BY buu.unit_key, d.[year], d.[month], d.[day]),

PolicyDayly AS (
    -- Dagelijkse policy-enrollments/completions (op load_ts van fact)
    SELECT buu.unit_key, d.[year], d.[month], d.[day], COUNT(*) AS enroll_day, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_day
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'policy'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.bridge_user_unit buu ON buu.user_key   = du.user_key AND buu.is_current = 1
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
		GROUP BY buu.unit_key, d.[year], d.[month], d.[day]),

Base AS (
    -- alle unit/year/month combinaties waar iéts gebeurt
    SELECT unit_key, [year], [month], [day] FROM PhishSnapshotdayly
    UNION
    SELECT unit_key, [year], [month], [day] FROM PhishResponseDayly
    UNION
    SELECT unit_key, [year], [month], [day] FROM TrainingDayly
    UNION
    SELECT unit_key, [year], [month], [day] FROM PolicyDayly
	)

SELECT u.unit_code,u.unit_naam,b.[year],b.[month], b.[day],CAST(ps.avg_phish_prone_pct AS DECIMAL(9,2)) AS avg_phish_prone,CAST(ps.avg_risk_score AS DECIMAL(9,2)) AS avg_risk_score,
    -- RESPONSE
    CAST(1.0 * SUM(COALESCE(pr.reported_day, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(pr.delivered_day, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_response_cum,
    -- TRAINING
    CAST(1.0 * SUM(COALESCE(tr.completed_day, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(tr.enroll_day, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_training_completed_cum,
    -- POLICY
    CAST(1.0 * SUM(COALESCE(po.completed_day, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(po.enroll_day, 0)) OVER (PARTITION BY b.unit_key, b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_policy_read_cum
FROM Base b
	JOIN DWH.dim_unit u ON u.unit_key = b.unit_key
	LEFT JOIN PhishSnapshotDayly ps ON ps.unit_key = b.unit_key AND ps.[year] = b.[year] AND ps.[month] = b.[month] AND ps.[day] = b.[day]
	LEFT JOIN PhishResponseDayly pr ON pr.unit_key = b.unit_key AND pr.[year] = b.[year] AND pr.[month] = b.[month] AND pr.[day] = b.[day]
	LEFT JOIN TrainingDayly tr ON tr.unit_key = b.unit_key AND tr.[year] = b.[year] AND tr.[month] = b.[month] AND tr.[day] = b.[day]
	LEFT JOIN PolicyDayly po ON po.unit_key = b.unit_key AND po.[year] = b.[year] AND po.[month] = b.[month]  AND po.[day] = b.[day];
GO

CREATE OR ALTER VIEW [KPI].vw_unit_security_dashboard_month_end
AS
WITH Ranked AS (
    SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.unit_code, d.[year], d.[month] ORDER BY d.[day] DESC) AS rn
		FROM KPI.vw_unit_security_dashboard_day d)
SELECT unit_code, unit_naam, [year], [month], [day], avg_phish_prone, avg_risk_score, pct_response_cum, pct_training_completed_cum, pct_policy_read_cum
	FROM Ranked
	WHERE rn = 1;
GO

CREATE OR ALTER VIEW [KPI].[vw_security_dashboard_month]
AS
WITH PhishSnapshotMonthly AS (
    -- Gemiddelde phish prone & risk per unit per maand (op load_ts van fact)
    SELECT d.[year], d.[month], AVG(f.phish_prone_pct)    AS avg_phish_prone_pct, AVG(f.current_risk_score) AS avg_risk_score
		FROM DWH.fact_user_security_snapshot f JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY d.[year], d.[month]),

PhishResponseMonthly AS (
    -- Maandelijkse reported/delivered per unit (op load_ts van fact, alleen phishing)
    SELECT d.[year], d.[month], SUM(f.reported_count)  AS reported_month, SUM(f.delivered_count) AS delivered_month
		FROM DWH.fact_pst_recipient_result f 
		JOIN DWH.dim_campaign c ON f.campaign_key = c.campaign_key AND c.campaign_type = 'phishing'
		JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY d.[year], d.[month]),

TrainingMonthly AS (
    -- Maandelijkse training-enrollments/completions (op load_ts van fact)
    SELECT d.[year], d.[month], COUNT(*) AS enroll_month, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_month
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'training'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
    GROUP BY d.[year], d.[month]),

PolicyMonthly AS (
    -- Maandelijkse policy-enrollments/completions (op load_ts van fact)
    SELECT d.[year], d.[month], COUNT(*) AS enroll_month, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_month
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'policy'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
		GROUP BY d.[year], d.[month]),

Base AS (
    -- alle unit/year/month combinaties waar iéts gebeurt
    SELECT [year], [month] FROM PhishSnapshotMonthly
    UNION
    SELECT [year], [month] FROM PhishResponseMonthly
    UNION
    SELECT [year], [month] FROM TrainingMonthly
    UNION
    SELECT [year], [month] FROM PolicyMonthly
	)

SELECT b.[year],b.[month],CAST(ps.avg_phish_prone_pct AS DECIMAL(9,2)) AS avg_phish_prone,CAST(ps.avg_risk_score AS DECIMAL(9,2)) AS avg_risk_score,
    -- RESPONSE
    CAST(1.0 * SUM(COALESCE(pr.reported_month, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(pr.delivered_month, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_response_cum,
    -- TRAINING
    CAST(1.0 * SUM(COALESCE(tr.completed_month, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(tr.enroll_month, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_training_completed_cum,
    -- POLICY
    CAST(1.0 * SUM(COALESCE(po.completed_month, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(po.enroll_month, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_policy_read_cum
FROM Base b
	LEFT JOIN PhishSnapshotMonthly ps ON ps.[year] = b.[year] AND ps.[month] = b.[month]
	LEFT JOIN PhishResponseMonthly pr ON pr.[year] = b.[year] AND pr.[month]  = b.[month]
	LEFT JOIN TrainingMonthly tr ON tr.[year] = b.[year] AND tr.[month] = b.[month]
	LEFT JOIN PolicyMonthly po ON po.[year] = b.[year] AND po.[month] = b.[month];
GO

CREATE OR ALTER VIEW [KPI].[vw_security_dashboard_week]
AS
WITH PhishSnapshotWeekly AS (
    -- Gemiddelde phish prone & risk per unit per week (op load_ts van fact)
    SELECT d.[year], d.[week], AVG(f.phish_prone_pct) AS avg_phish_prone_pct, AVG(f.current_risk_score) AS avg_risk_score
		FROM DWH.fact_user_security_snapshot f JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY d.[year], d.[week]),

PhishResponseWeekly AS (
    -- Wekelijkse reported/delivered per unit (op load_ts van fact, alleen phishing)
    SELECT d.[year], d.[week], SUM(f.reported_count)  AS reported_week, SUM(f.delivered_count) AS delivered_week
		FROM DWH.fact_pst_recipient_result f 
		JOIN DWH.dim_campaign c ON f.campaign_key = c.campaign_key AND c.campaign_type = 'phishing'
		JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY d.[year], d.[week]),

TrainingWeekly AS (
    -- Wekelijkse training-enrollments/completions (op load_ts van fact)
    SELECT d.[year], d.[week], COUNT(*) AS enroll_week, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_week
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'training'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
    GROUP BY d.[year], d.[week]),

PolicyWeekly AS (
    -- Wekelijkse policy-enrollments/completions (op load_ts van fact)
    SELECT d.[year], d.[week], COUNT(*) AS enroll_week, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_week
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'policy'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
		GROUP BY d.[year], d.[week]),

Base AS (
    -- alle unit/year/month combinaties waar iéts gebeurt
    SELECT [year], [week] FROM PhishSnapshotWeekly
    UNION
    SELECT [year], [week] FROM PhishResponseWeekly
    UNION
    SELECT [year], [week] FROM TrainingWeekly
    UNION
    SELECT [year], [week] FROM PolicyWeekly
	)

SELECT b.[year],b.[week],CAST(ps.avg_phish_prone_pct AS DECIMAL(9,2)) AS avg_phish_prone,CAST(ps.avg_risk_score AS DECIMAL(9,2)) AS avg_risk_score,
    -- RESPONSE
    CAST(1.0 * SUM(COALESCE(pr.reported_week, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(pr.delivered_week, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_response_cum,
    -- TRAINING
    CAST(1.0 * SUM(COALESCE(tr.completed_week, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(tr.enroll_week, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_training_completed_cum,
    -- POLICY
    CAST(1.0 * SUM(COALESCE(po.completed_week, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(po.enroll_week, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[week] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_policy_read_cum
FROM Base b
	LEFT JOIN PhishSnapshotWeekly ps ON ps.[year] = b.[year] AND ps.[week] = b.[week]
	LEFT JOIN PhishResponseWeekly pr ON pr.[year] = b.[year] AND pr.[week]  = b.[week]
	LEFT JOIN TrainingWeekly tr ON tr.[year] = b.[year] AND tr.[week] = b.[week]
	LEFT JOIN PolicyWeekly po ON po.[year] = b.[year] AND po.[week] = b.[week];
GO

CREATE OR ALTER VIEW [KPI].[vw_security_dashboard_day]
AS
WITH PhishSnapshotdayly AS (
    -- Gemiddelde phish prone & risk per unit per dag (op load_ts van fact)
    SELECT d.[year], d.[month], d.[day], AVG(f.phish_prone_pct) AS avg_phish_prone_pct, AVG(f.current_risk_score) AS avg_risk_score
		FROM DWH.fact_user_security_snapshot f JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY d.[year], d.[month], d.[day]),

PhishResponseDayly AS (
    -- Dagelijkse reported/delivered per unit (op load_ts van fact, alleen phishing)
    SELECT d.[year], d.[month],d.[day], SUM(f.reported_count)  AS reported_day, SUM(f.delivered_count) AS delivered_day
		FROM DWH.fact_pst_recipient_result f 
		JOIN DWH.dim_campaign c ON f.campaign_key = c.campaign_key AND c.campaign_type = 'phishing'
		JOIN DWH.dim_user du ON f.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(f.load_ts AS date)
		GROUP BY d.[year], d.[month], d.[day]),

TrainingDayly AS (
    -- Dagelijkse training-enrollments/completions (op load_ts van fact)
    SELECT d.[year], d.[month],d.[day], COUNT(*) AS enroll_day, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_day
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'training'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
    GROUP BY d.[year], d.[month], d.[day]),

PolicyDayly AS (
    -- Dagelijkse policy-enrollments/completions (op load_ts van fact)
    SELECT d.[year], d.[month], d.[day], COUNT(*) AS enroll_day, SUM(CASE WHEN te.[status] = 'Passed' THEN 1 ELSE 0 END) AS completed_day
		FROM DWH.fact_training_enrollment te
		JOIN DWH.dim_campaign c ON te.campaign_key = c.campaign_key AND c.campaign_type = 'policy'
		JOIN DWH.dim_user du ON te.user_key = du.user_key
		JOIN DWH.dim_date d ON d.[date] = CAST(te.load_ts AS date)
		GROUP BY d.[year], d.[month], d.[day]),

Base AS (
    -- alle unit/year/month combinaties waar iéts gebeurt
    SELECT [year], [month], [day] FROM PhishSnapshotdayly
    UNION
    SELECT [year], [month], [day] FROM PhishResponseDayly
    UNION
    SELECT [year], [month], [day] FROM TrainingDayly
    UNION
    SELECT [year], [month], [day] FROM PolicyDayly
	)

SELECT b.[year],b.[month], b.[day],CAST(ps.avg_phish_prone_pct AS DECIMAL(9,2)) AS avg_phish_prone,CAST(ps.avg_risk_score AS DECIMAL(9,2)) AS avg_risk_score,
    -- RESPONSE
    CAST(1.0 * SUM(COALESCE(pr.reported_day, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(pr.delivered_day, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_response_cum,
    -- TRAINING
    CAST(1.0 * SUM(COALESCE(tr.completed_day, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(tr.enroll_day, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_training_completed_cum,
    -- POLICY
    CAST(1.0 * SUM(COALESCE(po.completed_day, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
			NULLIF(SUM(COALESCE(po.enroll_day, 0)) OVER (PARTITION BY b.[year] ORDER BY b.[month], b.[day] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0) AS DECIMAL(9,2)) AS pct_policy_read_cum
FROM Base b
	LEFT JOIN PhishSnapshotDayly ps ON ps.[year] = b.[year] AND ps.[month] = b.[month] AND ps.[day] = b.[day]
	LEFT JOIN PhishResponseDayly pr ON pr.[year] = b.[year] AND pr.[month] = b.[month] AND pr.[day] = b.[day]
	LEFT JOIN TrainingDayly tr ON tr.[year] = b.[year] AND tr.[month] = b.[month] AND tr.[day] = b.[day]
	LEFT JOIN PolicyDayly po ON po.[year] = b.[year] AND po.[month] = b.[month]  AND po.[day] = b.[day];
GO

CREATE OR ALTER VIEW [KPI].vw_security_dashboard_month_end
AS
WITH Ranked AS (
    SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.[year], d.[month] ORDER BY d.[day] DESC) AS rn
		FROM KPI.vw_security_dashboard_day d)
SELECT [year], [month], [day], avg_phish_prone, avg_risk_score, pct_response_cum, pct_training_completed_cum, pct_policy_read_cum
	FROM Ranked
	WHERE rn = 1;
GO

CREATE OR ALTER VIEW [KPI].[vw_phishing_templates]
AS
SELECT r.template AS template_name,
		SUM(CASE WHEN r.clicked_at           IS NOT NULL THEN 1 ELSE 0 END) AS total_clicked,
		SUM(CASE WHEN r.replied_at           IS NOT NULL THEN 1 ELSE 0 END) AS total_replied,
		SUM(CASE WHEN r.attachment_opened_at IS NOT NULL THEN 1 ELSE 0 END) AS total_attachments_opened,
		SUM(CASE WHEN r.data_entered_at      IS NOT NULL THEN 1 ELSE 0 END) AS total_data_entered,
		-- totaal van de 4
		SUM(
			(CASE WHEN r.clicked_at           IS NOT NULL THEN 1 ELSE 0 END) +
			(CASE WHEN r.replied_at           IS NOT NULL THEN 1 ELSE 0 END) +
			(CASE WHEN r.attachment_opened_at IS NOT NULL THEN 1 ELSE 0 END) +
			(CASE WHEN r.data_entered_at      IS NOT NULL THEN 1 ELSE 0 END)
		) AS total_all
	FROM STG.Stg_kb4_Pst_Recipient r
WHERE r.template_id IS NOT NULL
GROUP BY r.template
HAVING
    SUM(
        (CASE WHEN r.clicked_at           IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN r.replied_at           IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN r.attachment_opened_at IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN r.data_entered_at      IS NOT NULL THEN 1 ELSE 0 END)
    ) > 0
GO

CREATE OR ALTER VIEW [KPI].[vw_phishing_template_types]
AS
	 WITH src AS (
		SELECT template_name, total_all
			FROM kpi.vw_phishing_templates
			WHERE template_name IS NOT NULL AND total_all > 0),
		xml_src AS (
			SELECT template_name, total_all,
				TRY_CAST('<root>' +	REPLACE(REPLACE(REPLACE(template_name, '&', '&amp;'), '(', '<t>'),')', '</t>') +'</root>' AS XML) AS x
			FROM src),
		types AS (
			SELECT LTRIM(RTRIM(n.value('.', 'nvarchar(200)'))) AS template_type, total_all
				FROM xml_src
				CROSS APPLY x.nodes('/root/t') AS ca(n)
				WHERE LTRIM(RTRIM(n.value('.', 'nvarchar(200)'))) <> '')
		SELECT
			template_type, COUNT(*) AS occurrences, SUM(total_all) AS total_reactions
			FROM types
			GROUP BY template_type
			HAVING SUM(total_all) > 0   -- extra safety; eigenlijk al gefilterd in src
GO
