use [KPI Database];
GO


CREATE OR ALTER PROCEDURE DWH.usp_run_master_etl (@InitDimDate   BIT  = 0, @DimDateStart  DATE = '2015-01-01', @DimDateEnd    DATE = '2035-12-31')
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRAN;
		------------------------------------------------------------
		-- Staging master tables
		------------------------------------------------------------
		EXEC STG.usp_load_md_Units;
		EXEC STG.usp_load_md_Training;
		EXEC STG.usp_load_md_Phishing;
        ------------------------------------------------------------
        -- 0) Optioneel: (her)opbouwen dim_date
        ------------------------------------------------------------
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
        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN;

        DECLARE @ErrMsg  NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrSev  INT            = ERROR_SEVERITY();
        DECLARE @ErrState INT           = ERROR_STATE();

        RAISERROR(@ErrMsg, @ErrSev, @ErrState);
        RETURN;
    END CATCH;
END;
GO

