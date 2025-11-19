use [KPI Database];
GO


CREATE OR ALTER PROCEDURE DWH.usp_run_master_etl (@InitDimDate   BIT  = 0, @DimDateStart  DATE = '2015-01-01', @DimDateEnd    DATE = '2035-12-31', @Number_Of_Checks	INT = 10)
AS
BEGIN
    SET NOCOUNT ON;
	DECLARE @stg_knowBe4_ready		BIT;
	DECLARE @stg_Hitas_Ready		BIT;
	DECLARE @stg_Mst_Ready			BIT;
	DECLARE @Check_ready			INT;
	DECLARE @IsReady				BIT;
	------------------------------------------------------------
	-- Staging master tables
	------------------------------------------------------------
	INSERT INTO [MST].Logdata([Timestamp], [Source], [Severity], [Log regel]) VALUES(SYSDATETIME(), 'MASTER', 'INFO', 'Master staging gestart')
	WAITFOR DELAY '00:00:01'
	EXEC STG.usp_load_md_Units;
	EXEC STG.usp_load_md_Training;
	EXEC STG.usp_load_md_Phishing;
	UPDATE [MST].[Source_status]
		SET Run_date = SYSDATETIME(),
			Finished = 1
		WHERE [Source] = 'Master'
	INSERT INTO [MST].Logdata([Timestamp], [Source], [Severity], [Log regel]) VALUES(SYSDATETIME(), 'MASTER', 'INFO', 'Master staging afgerond')
	WAITFOR DELAY '00:00:01'
	--------------------------------------------------------------
	-- Check readyness
	--------------------------------------------------------------
	SET @Check_ready = 0;
	SET @IsReady = 0;
	WHILE @IsReady = 0 AND @Check_ready <= @Number_Of_Checks
	BEGIN	
		SELECT @stg_knowBe4_ready = Finished 
			FROM MST.Source_status
			WHERE Run_date = CAST(SYSDATETIME() AS DATE) AND [Source] = 'KnowBe4';
		SELECT @stg_Hitas_Ready = Finished 
			FROM MST.Source_status
			WHERE Run_date = CAST(SYSDATETIME() AS DATE) AND [Source] = 'Hitas';
		SELECT @stg_Mst_Ready = Finished 
			FROM MST.Source_status
			WHERE Run_date = CAST(SYSDATETIME() AS DATE) AND [Source] = 'Master';
		IF @stg_knowBe4_ready = 1 AND @stg_Hitas_Ready = 1 AND @stg_Mst_Ready = 1
			BEGIN
				SET @IsReady = 1
				------------------------------------------------------------
				-- 0) Optioneel: (her)opbouwen dim_date
				------------------------------------------------------------
				INSERT INTO [MST].Logdata([Timestamp], [Source], [Severity], [Log regel]) VALUES(SYSDATETIME(), 'ETL', 'INFO', 'ETL proces gestart')
				IF @InitDimDate = 1
				BEGIN
					EXEC DWH.usp_init_dim_date @StartDate = @DimDateStart
						, @EndDate   = @DimDateEnd;
				END;

				------------------------------------------------------------
				-- 1) Dimensions laden
				------------------------------------------------------------
				-- user SCD2
				EXEC DWH.usp_load_dim_user_from_stg;

				-- unit SCD2
				EXEC DWH.usp_load_dim_unit_from_stg;

				-- campaign SCD2 (phishing / training / policy)
				EXEC DWH.usp_load_dim_campaign_from_stg;

				-- templates
				EXEC DWH.usp_load_dim_template_from_stg;

				------------------------------------------------------------
				-- 2) Bridge-tabellen (relaties)
				------------------------------------------------------------
				EXEC DWH.usp_load_bridge_user_unit_from_stg;

				------------------------------------------------------------
				-- 3) Facts
				------------------------------------------------------------
				-- PST resultaten
				EXEC DWH.usp_load_fact_pst_recipient_result;

				-- Training enrollments
				EXEC DWH.usp_load_fact_training_enrollment;

				-- Dagelijkse user security snapshot
				EXEC DWH.usp_load_fact_user_security_snapshot;

				------------------------------------------------------------
				UPDATE [MST].[Source_status]
					SET Run_date = SYSDATETIME(),
						Finished = 1
					WHERE [Source] = 'DWH'
				INSERT INTO [MST].Logdata([Timestamp], [Source], [Severity], [Log regel]) VALUES(SYSDATETIME(), 'ETL', 'INFO', 'ETL proces afgerond')
			END 
		ELSE
			BEGIN
				INSERT INTO [MST].Logdata([Timestamp], [Source], [Severity], [Log regel]) VALUES(SYSDATETIME(), 'MASTER', 'WARN', 'Nog niet alle processen gereed voor ETL proces')
				SET @Check_ready = @Check_ready + 1
				WAITFOR DELAY '00:10:00'
			END;
		IF @Check_ready > @Number_Of_Checks AND @IsReady = 0
			INSERT INTO [MST].Logdata([Timestamp], [Source], [Severity], [Log regel]) VALUES(SYSDATETIME(), 'MASTER', 'ERR', 'ETL proces kan niet worden uitgevoerd')
	END
END
GO

