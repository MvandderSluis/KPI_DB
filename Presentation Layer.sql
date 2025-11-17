USE [KPI Database]
GO
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[KPI].[vw_unit_security_dashboard]') AND type in (N'V'))
		DROP VIEW KPI.vw_unit_security_dashboard
IF EXISTS (SELECT * FROM [KPI database].sys.schemas WHERE name = 'KPI')
	DROP SCHEMA KPI
GO
CREATE SCHEMA KPI
GO

CREATE OR ALTER VIEW KPI.vw_unit_security_dashboard
AS
WITH base_units AS (
    SELECT DISTINCT
          u.unit_key
        , u.unit_code
        , u.unit_naam
    FROM DWH.dim_unit u
    JOIN DWH.bridge_user_unit bu
      ON bu.unit_key   = u.unit_key
     AND bu.is_current = 1
    WHERE u.is_current = 1
),

risk_agg AS (
    SELECT 
          u.unit_key
        , MIN(dd.[date])                                  AS periode_start
        , MAX(dd.[date])                                  AS periode_einde
        , AVG(CAST(f.current_risk_score AS float))        AS avg_risk_score_simple
        , SUM(CAST(f.current_risk_score AS float) 
              * ISNULL(bu.allocation_wt, 1.0))
          / NULLIF(SUM(ISNULL(bu.allocation_wt, 1.0)), 0) AS avg_risk_score_weighted
        , AVG(CAST(f.phish_prone_pct AS float))           AS avg_phish_prone_pct
    FROM DWH.fact_user_security_snapshot f
    JOIN DWH.dim_date dd
      ON dd.date_key = f.snapshot_date_key
    JOIN DWH.bridge_user_unit bu
      ON bu.user_key = f.user_key
     AND f.snapshot_date_key BETWEEN bu.start_date_key 
                                 AND ISNULL(bu.end_date_key, 99991231)
    JOIN DWH.dim_unit u
      ON u.unit_key   = bu.unit_key
     AND u.is_current = 1
    WHERE dd.[date] BETWEEN DATEADD(DAY, -30, CAST(GETDATE() AS date))
                        AND CAST(GETDATE() AS date)
    GROUP BY 
          u.unit_key
),

phish_agg AS (
    SELECT
          u.unit_key
        , SUM(COALESCE(fpr.delivered_count, 0) * ISNULL(bu.allocation_wt, 1.0)) AS delivered_total
        , SUM(COALESCE(fpr.reported_count,  0) * ISNULL(bu.allocation_wt, 1.0)) AS reported_total
    FROM DWH.fact_pst_recipient_result fpr
    JOIN DWH.bridge_user_unit bu
      ON bu.user_key   = fpr.user_key
     AND bu.is_current = 1
    JOIN DWH.dim_unit u
      ON u.unit_key   = bu.unit_key
     AND u.is_current = 1
    WHERE CAST(fpr.load_ts AS date) BETWEEN DATEADD(DAY, -30, CAST(GETDATE() AS date))
                                       AND CAST(GETDATE() AS date)
    GROUP BY
          u.unit_key
),

train_all AS (
    SELECT
          u.unit_key
        , ISNULL(bu.allocation_wt, 1.0)                                           AS wt
        , CASE WHEN UPPER(LTRIM(RTRIM(fte.[status]))) = 'PASSED' THEN 1 ELSE 0 END AS is_passed
        , CASE 
              WHEN UPPER(dc.campaign_type) = 'POLICY'   THEN 1
              WHEN UPPER(dc.campaign_type) = 'TRAINING' THEN 0
              ELSE 0
          END AS is_policy
    FROM DWH.fact_training_enrollment fte
    JOIN DWH.dim_campaign dc
      ON dc.campaign_key = fte.campaign_key
     AND dc.is_current   = 1
    JOIN DWH.bridge_user_unit bu
      ON bu.user_key      = fte.user_key
     AND bu.is_current    = 1
    JOIN DWH.dim_unit u
      ON u.unit_key       = bu.unit_key
     AND u.is_current     = 1
),

train_agg AS (
    SELECT
          unit_key
        , SUM(wt)             AS total_enrollments
        , SUM(wt * is_passed) AS passed_enrollments
    FROM train_all
    WHERE is_policy = 0
    GROUP BY unit_key
),

policy_agg AS (
    SELECT
          unit_key
        , SUM(wt)             AS total_policy_enrollments
        , SUM(wt * is_passed) AS passed_policy_enrollments
    FROM train_all
    WHERE is_policy = 1
    GROUP BY unit_key
)

SELECT
      b.unit_code
    , b.unit_naam
    , r.periode_start
    , r.periode_einde
    , CAST(ROUND(r.avg_risk_score_simple,   2) AS DECIMAL(10,2)) AS avg_risk_score_simple
    , CAST(ROUND(r.avg_risk_score_weighted, 2) AS DECIMAL(10,2)) AS avg_risk_score_weighted
    , CAST(ROUND(r.avg_phish_prone_pct,     2) AS DECIMAL(10,2)) AS avg_phish_prone_pct
    , CAST(ROUND(p.delivered_total, 2)          AS DECIMAL(10,2)) AS delivered_total
    , CAST(ROUND(p.reported_total,  2)          AS DECIMAL(10,2)) AS reported_total
    , CAST(ROUND(
          CASE 
              WHEN p.delivered_total > 0 
                  THEN 100.0 * p.reported_total / p.delivered_total
              ELSE NULL
          END
      , 2) AS DECIMAL(10,2)) AS reported_pct_phishing
    , CAST(ROUND(t.total_enrollments,   2) AS DECIMAL(10,2)) AS total_enrollments
    , CAST(ROUND(t.passed_enrollments,  2) AS DECIMAL(10,2)) AS passed_enrollments
    , CAST(ROUND(
          CASE 
              WHEN t.total_enrollments > 0
                  THEN 100.0 * t.passed_enrollments / t.total_enrollments
              ELSE NULL
          END
      , 2) AS DECIMAL(10,2)) AS passed_pct_training
    , CAST(ROUND(pa.total_policy_enrollments,  2) AS DECIMAL(10,2)) AS total_policy_enrollments
    , CAST(ROUND(pa.passed_policy_enrollments, 2) AS DECIMAL(10,2)) AS passed_policy_enrollments
    , CAST(ROUND(
          CASE 
              WHEN pa.total_policy_enrollments > 0
                  THEN 100.0 * pa.passed_policy_enrollments / pa.total_policy_enrollments
              ELSE NULL
          END
      , 2) AS DECIMAL(10,2)) AS passed_pct_policy
FROM base_units b
LEFT JOIN risk_agg   r  ON r.unit_key  = b.unit_key
LEFT JOIN phish_agg  p  ON p.unit_key  = b.unit_key
LEFT JOIN train_agg  t  ON t.unit_key  = b.unit_key
LEFT JOIN policy_agg pa ON pa.unit_key = b.unit_key;