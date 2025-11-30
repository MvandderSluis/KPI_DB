USE [KPI Database];
GO

-- Staging van master tables
CREATE OR ALTER PROCEDURE STG.usp_load_md_Units
AS
BEGIN
    SET NOCOUNT ON;

    -- Staging leegmaken (full refresh)
    TRUNCATE TABLE STG.stg_md_Units;

    INSERT INTO STG.stg_md_Units (Unit_code, Unit_name, hash_row)
		SELECT Unit AS Unit_code, [Unit name] AS Unit_name, HASHBYTES('SHA2_256', CONCAT(UPPER(LTRIM(RTRIM(Unit))) , '|' , ISNULL(UPPER(LTRIM(RTRIM([Unit name]))), '')))                                          AS hash_row
			FROM MST.MST_Units;
END;
GO

CREATE OR ALTER PROCEDURE STG.usp_load_md_Training
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE STG.stg_md_Training;

    INSERT INTO STG.stg_md_Training ([Name], Active, [Type], hash_row)
    SELECT [Name], Active, [Type], HASHBYTES('SHA2_256', CONCAT(UPPER(LTRIM(RTRIM([Name]))) , '|', IIF(Active = 1, '1', '0') , '|', ISNULL(UPPER(LTRIM(RTRIM([Type]))), ''))) AS hash_row
		FROM MST.MST_Training_Campaigns;
END;
GO

CREATE OR ALTER PROCEDURE STG.usp_load_md_Phishing
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE STG.stg_md_Phishing;

    INSERT INTO STG.stg_md_Phishing (Campaign_id, [Name], Active, hash_row)
		SELECT Campaign_id, [Name], Active, HASHBYTES('SHA2_256', CONCAT(CAST(Campaign_id AS VARCHAR(50)) , '|', ISNULL(UPPER(LTRIM(RTRIM([Name]))), '') , '|', IIF(Active = 1, '1', '0'))) AS hash_row
		FROM MST.MST_Phishing_Campaigns;
END;
GO

-- Dimensions

CREATE OR ALTER PROCEDURE DWH.usp_load_dim_user_from_stg
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LoadTs DATETIME2 = SYSDATETIME();

    ;WITH src AS (SELECT u.[user_id], u.eHash FROM STG.Stg_kb4_Users u)
		-- 1) Sluit gewijzigde current records af
		UPDATE d
		SET d.effective_to = @LoadTs, d.is_current   = 0
			FROM DWH.dim_user d
			JOIN src s ON s.[user_id] = d.[user_id]
			WHERE d.is_current = 1
				AND (ISNULL(d.eHash, 0x0) <> ISNULL(s.eHash, 0x0))
	;WITH src AS (SELECT u.[user_id], u.eHash FROM STG.Stg_kb4_Users u)
		-- 2) Voeg nieuwe of gewijzigde records toe (nieuwe SCD2-rand)
		INSERT INTO DWH.dim_user ([user_id], eHash, effective_from, effective_to, is_current)
			SELECT s.[user_id], s.eHash, @LoadTs, NULL, 1
				FROM src s
				LEFT JOIN DWH.dim_user d ON d.[user_id] = s.[user_id] AND d.is_current = 1
				WHERE d.[user_id] IS NULL								  -- nieuwe user
						OR ISNULL(d.eHash, 0x0) <> ISNULL(s.eHash, 0x0);  -- gewijzigde eHash
END;
GO

CREATE OR ALTER PROCEDURE DWH.usp_load_dim_unit_from_stg
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LoadTs DATETIME2 = SYSDATETIME();

    ;WITH src AS (SELECT Unit_code, Unit_name FROM STG.stg_md_Units)
		-- 1) Sluit gewijzigde current records af
		UPDATE d SET d.effective_to = @LoadTs, d.is_current   = 0
			FROM DWH.dim_unit d
			JOIN src s ON s.Unit_code = d.unit_code
			WHERE d.is_current = 1
				AND (  ISNULL(d.unit_naam,'') <> ISNULL(s.Unit_name,'') );
	;WITH src AS (SELECT Unit_code, Unit_name FROM STG.stg_md_Units)
		-- 2) Voeg nieuwe of gewijzigde records toe
		INSERT INTO DWH.dim_unit (unit_code, unit_naam, effective_from, effective_to, is_current)
		SELECT s.Unit_code, s.Unit_name, @LoadTs, NULL, 1
			FROM src s
			LEFT JOIN DWH.dim_unit d ON d.unit_code = s.Unit_code AND d.is_current = 1
			WHERE d.unit_code IS NULL OR ISNULL(d.unit_naam,'') <> ISNULL(s.Unit_name,'');
END;
GO

CREATE OR ALTER PROCEDURE DWH.usp_load_dim_campaign_from_stg
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LoadTs     DATETIME2 = SYSDATETIME();
    DECLARE @CutoffDate DATE	  = DATEADD(YEAR, -2, CAST(@LoadTs AS DATE));  -- alles ouder dan 2 jaar sluiten

    -- temp tabel met alle (actieve) campagnes uit staging
    IF OBJECT_ID('tempdb..#src') IS NOT NULL
        DROP TABLE #src;

    CREATE TABLE #src (
          campaign_id    BIGINT       NOT NULL
        , campaign_type  VARCHAR(20)  NOT NULL
        , [name]         VARCHAR(255) NULL
        , [status]       VARCHAR(50)  NULL
        , start_date_key INT          NULL
    );

    ------------------------------------------------------------------------
    -- BRON OPBOUWEN: phishing (gefilterd op Active=1 en niet ouder dan 2 jaar)
    --                + training/policy (zoals voorheen)
    ------------------------------------------------------------------------
    ;WITH phishing_base AS (
        -- 1 regel per phishing campaign_id
        SELECT p.campaign_id, MIN(p.[name]) AS [name], MAX(p.[status]) AS [status], MIN(CAST(p.started_at AS DATE)) AS start_date, MAX(DATEADD(DAY, ISNULL(p.duration_days,0),CAST(p.started_at AS DATE))) AS campaign_end_date
			FROM STG.Stg_kb4_Pst p
			GROUP BY p.campaign_id),
    phishing_filtered AS (
        -- alleen actieve phishing campagnes uit de master én niet ouder dan 2 jaar
        SELECT pb.campaign_id, pb.[name], pb.[status], pb.start_date
			FROM phishing_base pb
			JOIN STG.stg_md_Phishing mp ON mp.Campaign_id = pb.campaign_id AND mp.[Active] = 1
			WHERE pb.campaign_end_date >= @CutoffDate),
    
	training_base AS (
        -- training + policy (type uit stg_md_Training)
        SELECT tc.campaign_id, CASE UPPER(mt.[Type]) WHEN 'P' THEN 'policy' ELSE 'training' END AS campaign_type, MIN(tc.[name]) AS [name], MAX(tc.[status]) AS [status], MIN(tc.start_date) AS start_date
			FROM STG.Stg_kb4_Training_Campaign tc
			LEFT JOIN STG.stg_md_Training mt ON UPPER(mt.[Name]) = LEFT(UPPER(tc.[name]),LEN(mt.[Name]))
			GROUP BY tc.campaign_id, CASE UPPER(mt.[Type]) WHEN 'P' THEN 'policy' ELSE 'training' END),
    
	src_union AS (
        SELECT pf.campaign_id, CAST('phishing' AS VARCHAR(20)) AS campaign_type, pf.[name], pf.[status], pf.start_date
			FROM phishing_filtered pf

        UNION ALL

        SELECT tb.campaign_id, tb.campaign_type, tb.[name], tb.[status], tb.start_date
			FROM training_base tb),
    src_final AS (
        SELECT s.campaign_id, s.campaign_type, s.[name], s.[status], s.start_date, d.date_key AS start_date_key
			FROM src_union s
			LEFT JOIN DWH.dim_date d ON d.[date] = s.start_date)
    
	INSERT INTO #src (campaign_id, campaign_type, [name], [status], start_date_key)
		SELECT campaign_id, campaign_type, [name], [status], start_date_key
			FROM src_final;

    ------------------------------------------------------------------------
    -- STAP 1: sluit huidige PHISHING campagnes die NIET meer in #src staan
    --         (dus: niet meer active=1, of ouder dan 2 jaar)
    ------------------------------------------------------------------------
    UPDATE d
		SET  d.effective_to = @LoadTs,
			d.is_current   = 0
		FROM DWH.dim_campaign d
		LEFT JOIN #src s ON s.campaign_id   = d.campaign_id AND s.campaign_type = d.campaign_type
		WHERE d.campaign_type = 'phishing' AND d.is_current = 1 AND s.campaign_id   IS NULL;

    ------------------------------------------------------------------------
    -- STAP 2: sluit alle campagnes (phishing + training + policy) waarvan
    --         attribuutwijzigingen zijn t.o.v. #src (SCD2-change)
    ------------------------------------------------------------------------
    UPDATE d
		SET  d.effective_to = @LoadTs,
			 d.is_current   = 0
			FROM DWH.dim_campaign d
			JOIN #src s ON s.campaign_id = d.campaign_id AND s.campaign_type = d.campaign_type
			WHERE d.is_current = 1 AND (ISNULL(d.[name], '') <> ISNULL(s.[name], '') OR ISNULL(d.[status], '') <> ISNULL(s.[status], '') OR ISNULL(d.start_date_key, -1) <> ISNULL(s.start_date_key, -1));

    ------------------------------------------------------------------------
    -- STAP 3: voeg nieuwe/gewijzigde campagnes toe als nieuwe SCD2-rand
    ------------------------------------------------------------------------
    INSERT INTO DWH.dim_campaign (campaign_id, campaign_type, [name], [status], start_date_key, effective_from, effective_to, is_current)
		SELECT s.campaign_id, s.campaign_type, s.[name], s.[status], s.start_date_key, @LoadTs AS effective_from, NULL AS effective_to, 1 AS is_current
			FROM #src s
			LEFT JOIN DWH.dim_campaign d ON d.campaign_id = s.campaign_id AND d.campaign_type = s.campaign_type AND d.is_current = 1
			WHERE d.campaign_id IS NULL OR ISNULL(d.[name], '') <> ISNULL(s.[name], '') OR ISNULL(d.[status], '') <> ISNULL(s.[status], '') OR ISNULL(d.start_date_key, -1) <> ISNULL(s.start_date_key, -1);
END;
GO

CREATE OR ALTER PROCEDURE DWH.usp_load_dim_template_from_stg
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LoadTs DATETIME2 = SYSDATETIME();

    ;WITH src AS (SELECT DISTINCT p.template_id, p.template
					FROM STG.Stg_kb4_Pst_Recipient p
					WHERE p.[template_id] IS NOT NULL)
    INSERT INTO DWH.dim_template (template_id, template_name, effective_from, effective_to, is_current)
		SELECT DISTINCT s.template_id, s.template, @LoadTs, NULL, 1
			FROM src s
			LEFT JOIN DWH.dim_template d ON d.template_id = s.template_id AND d.is_current = 1
			WHERE d.template_key IS NULL;
END;
GO

CREATE OR ALTER PROCEDURE DWH.usp_init_dim_date (@StartDate DATE = '2015-01-01', @EndDate   DATE = '2035-12-31')
AS
BEGIN
    SET NOCOUNT ON;

    IF @EndDate < @StartDate
    BEGIN
        RAISERROR('EndDate mag niet vóór StartDate liggen.', 16, 1);
        RETURN;
    END;

    -- Optioneel: vaste weekstart aannemen (maandag)
    SET DATEFIRST 1;

    DELETE FROM DWH.dim_date;

    DECLARE @d DATE = @StartDate;

    WHILE @d <= @EndDate
    BEGIN
        INSERT INTO DWH.dim_date (date_key, [date], [year], [month], [day], [week], [quarter], is_weekend)
			VALUES (	YEAR(@d) * 10000+ MONTH(@d) * 100+ DAY(@d)                      -- date_key yyyymmdd
						, @d															-- date
						, YEAR(@d)														-- year
						, MONTH(@d)														-- month
						, DAY(@d)														-- day
						, DATEPART(ISO_WEEK, @d)										-- week (ISO-weeknummer)
						, DATEPART(QUARTER, @d)											-- quarter
						, CASE WHEN DATEPART(WEEKDAY, @d) IN (6,7) THEN 1  ELSE 0 END   -- is_weekend (za/zo)
					);
        SET @d = DATEADD(DAY, 1, @d);
    END;
END;
GO

-- Bridges
CREATE OR ALTER PROCEDURE DWH.usp_load_bridge_user_unit_from_stg
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Today      DATE      = CAST(SYSDATETIME() AS DATE);
    DECLARE @TodayKey   INT;

    SELECT @TodayKey = date_key
		FROM DWH.dim_date
		WHERE [date] = @Today;

    IF @TodayKey IS NULL
		BEGIN
			DECLARE @Msg NVARCHAR(200) =
				FORMATMESSAGE('dim_date bevat geen record voor vandaag (%s).', CONVERT(VARCHAR(10), @Today, 120));
			RAISERROR(@Msg, 16, 1);
			RETURN;
		END;

    ;WITH src AS (
        SELECT du.user_key, du.eHash, u.unit_key
			FROM STG.Stg_hitas_Users h
			JOIN DWH.dim_user du ON du.eHash = h.eHash AND du.is_current = 1
			JOIN DWH.dim_unit u ON u.unit_code  = h.[Unit] AND u.is_current = 1)
    -- 1) Sluit relaties af die niet meer voorkomen of naar andere unit zijn verschoven
		UPDATE bu SET bu.end_date_key = @TodayKey,bu.is_current   = 0
			FROM DWH.bridge_user_unit bu
			JOIN DWH.dim_user du ON du.user_key   = bu.user_key AND du.is_current = 1
			LEFT JOIN src s ON s.user_key  = bu.user_key AND s.unit_key  = bu.unit_key
		WHERE bu.is_current = 1  AND s.user_key IS NULL;  -- niet meer in huidige snapshot
    ;WITH src AS (
        SELECT du.user_key, du.eHash, u.unit_key
			FROM STG.Stg_hitas_Users h
			JOIN DWH.dim_user du ON du.eHash = h.eHash AND du.is_current = 1
			JOIN DWH.dim_unit u ON u.unit_code  = h.[Unit] AND u.is_current = 1)
    -- 2) Voeg nieuwe relaties toe die nog niet bestaan
		INSERT INTO DWH.bridge_user_unit (user_key, unit_key, start_date_key, end_date_key, is_current, allocation_wt)
			SELECT s.user_key, s.unit_key, @TodayKey, NULL, 1, 1.0
			FROM src s
			LEFT JOIN DWH.bridge_user_unit bu ON bu.user_key = s.user_key AND bu.unit_key = s.unit_key AND bu.is_current = 1
			WHERE bu.user_key IS NULL;					
END;
GO

-- FACTS
CREATE OR ALTER PROCEDURE DWH.usp_load_fact_pst_recipient_result
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH src AS (
        SELECT	r.pst_id, r.[user_id], r.template_id,
				CASE WHEN r.delivered_at           IS NOT NULL THEN 1 ELSE 0 END AS delivered_count,
				CASE WHEN r.opened_at              IS NOT NULL THEN 1 ELSE 0 END AS opens_count,
				CASE WHEN r.clicked_at             IS NOT NULL THEN 1 ELSE 0 END AS clicks_count,
				CASE WHEN r.replied_at             IS NOT NULL THEN 1 ELSE 0 END AS replies_count,
				CASE WHEN r.attachment_opened_at   IS NOT NULL THEN 1 ELSE 0 END AS attachments_opened,
				CASE WHEN r.data_entered_at        IS NOT NULL THEN 1 ELSE 0 END AS data_entered,
				CASE WHEN r.macro_enabled_at	   IS NOT NULL THEN 1 ELSE 0 END AS macro_enabled,
				CASE WHEN r.qr_code_scanned_at     IS NOT NULL THEN 1 ELSE 0 END AS qr_code_scanned,
				CASE WHEN r.reported_at            IS NOT NULL THEN 1 ELSE 0 END AS reported_count,
				r.load_ts  AS src_load_ts, p.campaign_id, p.started_at, p.duration_days, p.[name] AS template_name
			FROM STG.Stg_kb4_Pst_Recipient r
			JOIN STG.Stg_kb4_Pst p  ON p.pst_id = r.pst_id),
    src_mapped AS (
        SELECT	du.user_key, dc.campaign_key, dt.template_key,s.pst_id, s.[user_id], s.delivered_count, s.opens_count, s.clicks_count, s.replies_count, s.attachments_opened, s.data_entered, 
				s.macro_enabled, s.qr_code_scanned, s.reported_count, 
				dd.date_key AS started_date_key, CASE WHEN s.duration_days IS NOT NULL THEN s.duration_days * 86400 ELSE NULL END AS duration_seconds, s.src_load_ts
			FROM src s
			JOIN DWH.dim_user du ON du.[user_id] = s.[user_id] AND du.is_current = 1
			JOIN DWH.dim_campaign dc ON dc.campaign_id = s.campaign_id AND dc.campaign_type = 'phishing' AND dc.is_current = 1
			LEFT JOIN DWH.dim_template dt ON dt.template_id = s.template_id AND dt.is_current = 1
			LEFT JOIN DWH.dim_date dd ON dd.[date] = CAST(s.started_at AS date)
    )
    MERGE DWH.fact_pst_recipient_result AS tgt
    USING src_mapped AS src ON tgt.pst_id   = src.pst_id AND tgt.user_key = src.user_key
    WHEN MATCHED THEN
        UPDATE SET
            tgt.campaign_key       = src.campaign_key,
            tgt.template_key       = src.template_key,
            tgt.delivered_count    = src.delivered_count,
            tgt.opens_count        = src.opens_count,
            tgt.clicks_count       = src.clicks_count,
            tgt.replies_count      = src.replies_count,
            tgt.attachments_opened = src.attachments_opened,
            tgt.data_entered       = src.data_entered,
			tgt.macro_enabled      = src.macro_enabled,
			tgt.qr_code_scanned	   = src.qr_code_scanned,
            tgt.reported_count     = src.reported_count,
            tgt.started_date_key   = src.started_date_key,
            tgt.duration_seconds   = src.duration_seconds
            -- LET OP: load_ts níet updaten → eerste loaddatum blijft staan
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (user_key, campaign_key, template_key, pst_id, result_status, delivered_count, opens_count, clicks_count, replies_count, attachments_opened, data_entered, macro_enabled, qr_code_scanned, reported_count, started_date_key, 
				duration_seconds, load_ts)
			VALUES (src.user_key, src.campaign_key, src.template_key, src.pst_id, NULL, src.delivered_count, src.opens_count, src.clicks_count, src.replies_count, src.attachments_opened, src.data_entered, 
					src.macro_enabled, src.qr_code_scanned, src.reported_count, src.started_date_key, src.duration_seconds, src.src_load_ts);
END;
GO

CREATE OR ALTER PROCEDURE DWH.usp_load_fact_training_enrollment
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH src AS (
        SELECT
              du.user_key
            , dc.campaign_key
            , e.enrollment_id
            , e.[status]
            , ded.date_key AS enrollment_date_key
            , e.load_ts    AS src_load_ts   -- staging-load_ts
        FROM STG.Stg_kb4_Training_Enrollment e
        JOIN DWH.dim_user du
          ON du.user_id    = e.user_id
         AND du.is_current = 1
        JOIN DWH.dim_campaign dc
          ON dc.campaign_id   = e.campaign_id
         AND dc.campaign_type IN ('training','policy')
         AND dc.is_current    = 1
        LEFT JOIN DWH.dim_date ded
          ON ded.[date] = CAST(e.enrollment_date AS date)
    )
    MERGE DWH.fact_training_enrollment AS tgt
    USING src
       ON tgt.enrollment_id = src.enrollment_id
    WHEN MATCHED THEN
        UPDATE SET
              tgt.user_key            = src.user_key
            , tgt.campaign_key        = src.campaign_key
            , tgt.[status]            = src.[status]
            , tgt.enrollment_date_key = src.enrollment_date_key
            -- tgt.completion_date_key blijft zoals hij is
            -- tgt.load_ts NIET updaten: eerste loaddatum behouden
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
              user_key
            , campaign_key
            , enrollment_id
            , [status]
            , enrollment_date_key
            , completion_date_key
            , load_ts
        )
        VALUES (
              src.user_key
            , src.campaign_key
            , src.enrollment_id
            , src.[status]
            , src.enrollment_date_key
            , NULL           -- geen completion info in STG
            , src.src_load_ts
        );
END;
GO

CREATE OR ALTER PROCEDURE DWH.usp_load_fact_user_security_snapshot
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Today    DATE      = CAST(SYSDATETIME() AS DATE);
    DECLARE @TodayKey INT;
    DECLARE @LoadTs   DATETIME2 = SYSDATETIME();

    SELECT @TodayKey = date_key
    FROM DWH.dim_date
    WHERE [date] = @Today;

    IF @TodayKey IS NULL
		BEGIN
			DECLARE @Msg NVARCHAR(200) =
				FORMATMESSAGE('dim_date bevat geen record voor vandaag (%s).', CONVERT(VARCHAR(10), @Today, 120));
			RAISERROR(@Msg, 16, 1);
			RETURN;
		END;

    ;WITH risk AS (SELECT ud.user_id, ud.current_risk_score, ROW_NUMBER() OVER (PARTITION BY ud.user_id ORDER BY ud.load_ts DESC) AS rn
					FROM STG.Stg_kb4_User_Detail ud)
		INSERT INTO DWH.fact_user_security_snapshot (user_key, snapshot_date_key, phish_prone_pct, current_risk_score, load_ts)
			SELECT du.user_key, @TodayKey, u.phish_prone_pct, r.current_risk_score, @LoadTs
			FROM STG.Stg_kb4_Users u
			JOIN DWH.dim_user du ON du.user_id    = u.user_id AND du.is_current = 1
			LEFT JOIN risk r ON r.user_id = u.user_id AND r.rn = 1
			LEFT JOIN DWH.fact_user_security_snapshot f ON f.user_key = du.user_key AND f.snapshot_date_key = @TodayKey
			WHERE f.snapshot_key IS NULL;  -- geen dubbele snapshots per dag
END;
GO

