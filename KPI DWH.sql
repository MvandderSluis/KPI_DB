USE [KPI database];
GO
-- Deleting
BEGIN
	-- CONSTRAINTS
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'fact_pst_recipient_result' 
					AND rc.CONSTRAINT_NAME = 'FK_pst_user' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[fact_pst_recipient_result] DROP CONSTRAINT [FK_pst_user]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'fact_pst_recipient_result' 
					AND rc.CONSTRAINT_NAME = 'FK_pst_campaign' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[fact_pst_recipient_result] DROP CONSTRAINT [FK_pst_campaign]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'fact_pst_recipient_result' 
					AND rc.CONSTRAINT_NAME = 'FK_pst_template' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[fact_pst_recipient_result] DROP CONSTRAINT [FK_pst_template]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'fact_pst_recipient_result' 
					AND rc.CONSTRAINT_NAME = 'FK_pst_date' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[fact_pst_recipient_result] DROP CONSTRAINT [FK_pst_date]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'fact_training_enrollment' 
					AND rc.CONSTRAINT_NAME = 'FK_te_user' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[fact_training_enrollment] DROP CONSTRAINT [FK_te_user]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'fact_training_enrollment' 
					AND rc.CONSTRAINT_NAME = 'FK_te_campaign' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[fact_training_enrollment] DROP CONSTRAINT [FK_te_campaign]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'fact_training_enrollment' 
					AND rc.CONSTRAINT_NAME = 'FK_te_enrdate' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[fact_training_enrollment] DROP CONSTRAINT [FK_te_enrdate]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'fact_training_enrollment' 
					AND rc.CONSTRAINT_NAME = 'FK_te_compdate' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[fact_training_enrollment] DROP CONSTRAINT [FK_te_compdate]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'fact_user_security_snapshot' 
					AND rc.CONSTRAINT_NAME = 'FK_snap_user' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[fact_user_security_snapshot] DROP CONSTRAINT [FK_snap_user]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'fact_user_security_snapshot' 
					AND rc.CONSTRAINT_NAME = 'FK_snap_date' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[fact_user_security_snapshot] DROP CONSTRAINT [FK_snap_date]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'bridge_user_unit' 
					AND rc.CONSTRAINT_NAME = 'FK_buu_user' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[bridge_user_unit] DROP CONSTRAINT [FK_buu_user]
	IF EXISTS(SELECT rc.* 
					FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc 
					LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 
					ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG  
					AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA 
					AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
					WHERE KCU1.TABLE_NAME = 'bridge_user_unit' 
					AND rc.CONSTRAINT_NAME = 'FK_buu_unit' 
					AND rc.CONSTRAINT_SCHEMA = 'DWH') 
		ALTER TABLE [DWH].[bridge_user_unit] DROP CONSTRAINT [FK_buu_unit]
	-- STAGING
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[stg_kb4_users]') AND type in (N'U'))																																											
		DROP TABLE [STG].[stg_kb4_users];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[stg_kb4_user_detail]') AND type in (N'U'))
		DROP TABLE [STG].[stg_kb4_user_detail];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[stg_kb4_pst]') AND type in (N'U'))
		DROP TABLE [STG].[stg_kb4_pst];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[stg_kb4_pst_recipient]') AND type in (N'U'))
		DROP TABLE [STG].[stg_kb4_pst_recipient];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[Stg_kb4_Training_Campaign]') AND type in (N'U'))
		DROP TABLE [STG].[Stg_kb4_Training_Campaign];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[Stg_kb4_Training_Enrollment]') AND type in (N'U'))
		DROP TABLE [STG].[Stg_kb4_Training_Enrollment];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[Stg_hitas_Users]') AND type in (N'U'))
		DROP TABLE [STG].[Stg_hitas_Users];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[stg_md_Units]') AND type in (N'U'))
		DROP TABLE [STG].[stg_md_Units];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[stg_md_Training]') AND type in (N'U'))
		DROP TABLE [STG].stg_md_Training;
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[stg_md_Phishing]') AND type in (N'U'))
		DROP TABLE [STG].stg_md_Phishing;	
	-- Target
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[dim_user]') AND type in (N'U'))
		DROP TABLE [DWH].[dim_user];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[dim_campaign]') AND type in (N'U'))
		DROP TABLE [DWH].[dim_campaign];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[dim_template]') AND type in (N'U'))
		DROP TABLE [DWH].[dim_template];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[dim_date]') AND type in (N'U'))
		DROP TABLE [DWH].[dim_date];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[dim_unit]') AND type in (N'U'))
		DROP TABLE [DWH].[dim_unit];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[fact_pst_recipient_result]') AND type in (N'U'))
		DROP TABLE [DWH].[fact_pst_recipient_result];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[fact_training_enrollment]') AND type in (N'U'))
		DROP TABLE [DWH].[fact_training_enrollment];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[fact_user_security_snapshot]') AND type in (N'U'))
		DROP TABLE [DWH].[fact_user_security_snapshot];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[bridge_user_unit]') AND type in (N'U'))
		DROP TABLE [DWH].[bridge_user_unit];
	-- Master
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MST].[MST_Units]') AND type in (N'U'))
		DROP TABLE [MST].[MST_Units];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MST].[MST_Phishing_Campaigns]') AND type in (N'U'))
		DROP TABLE [MST].[MST_Phishing_Campaigns];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MST].[MST_Training_Campaigns]') AND type in (N'U'))
		DROP TABLE [MST].[MST_Training_Campaigns];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MST].[MST_Targets]') AND type in (N'U'))
		DROP TABLE [MST].[MST_Targets];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MST].[Logdata]') AND type in (N'U'))
		DROP TABLE [MST].[LogData];
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[MST].[Source_status]') AND type in (N'U'))
		DROP TABLE [MST].[Source_status];
	-- Procedures
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[usp_load_md_Units]') AND type in (N'P'))
		DROP PROCEDURE STG.usp_load_md_Units
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[usp_load_md_Training]') AND type in (N'P'))
		DROP PROCEDURE STG.usp_load_md_Training
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[STG].[usp_load_md_Phishing]') AND type in (N'P'))
		DROP PROCEDURE STG.usp_load_md_Phishing
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[usp_load_dim_user_from_stg]') AND type in (N'P'))
		DROP PROCEDURE DWH.usp_load_dim_user_from_stg
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[usp_load_dim_unit_from_stg]') AND type in (N'P'))
		DROP PROCEDURE DWH.usp_load_dim_unit_from_stg
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[usp_load_dim_campaign_from_stg]') AND type in (N'P'))
		DROP PROCEDURE DWH.usp_load_dim_campaign_from_stg
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[usp_load_dim_template_from_stg]') AND type in (N'P'))
		DROP PROCEDURE DWH.usp_load_dim_template_from_stg
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[usp_init_dim_date]') AND type in (N'P'))
		DROP PROCEDURE DWH.usp_init_dim_date
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[usp_load_bridge_user_unit_from_stg]') AND type in (N'P'))
		DROP PROCEDURE DWH.usp_load_bridge_user_unit_from_stg
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[usp_load_fact_pst_recipient_result]') AND type in (N'P'))
		DROP PROCEDURE DWH.usp_load_fact_pst_recipient_result
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[usp_load_fact_training_enrollment]') AND type in (N'P'))
		DROP PROCEDURE DWH.usp_load_fact_training_enrollment
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[usp_load_fact_user_security_snapshot]') AND type in (N'P'))
		DROP PROCEDURE DWH.usp_load_fact_user_security_snapshot
	IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[DWH].[usp_run_master_etl]') AND type in (N'P'))
		DROP PROCEDURE DWH.usp_run_master_etl
	-- SCHEMA's
	IF EXISTS (SELECT * FROM [KPI database].sys.schemas WHERE name = 'STG')
		DROP SCHEMA STG;
	IF EXISTS (SELECT * FROM [KPI database].sys.schemas WHERE name = 'MST')
		DROP SCHEMA MST;
	IF EXISTS (SELECT * FROM [KPI database].sys.schemas WHERE name = 'DWH')
		DROP SCHEMA DWH;
END
GO
----------------- STAGING
-- Staging layer tables

CREATE SCHEMA STG;
GO

-- Staging
BEGIN
	-- 1) Users (lijst)
	CREATE TABLE [STG].[Stg_kb4_Users] (
		[user_id]              BIGINT			NOT NULL,
		[eHash]                VARBINARY(32)	NULL,
		[phish_prone_pct]      DECIMAL(5,2)		NULL,   -- van "Get all users"
		[load_ts]              DATETIME2		NOT NULL DEFAULT SYSDATETIME(),
		[hash_row]             VARBINARY(32)	NULL,
	  CONSTRAINT PK_stg_kb4_users PRIMARY KEY ([user_id])
	);

	-- 2) User details (risk score per user)
	CREATE TABLE [STG].[Stg_kb4_User_Detail] (
		[user_id]              BIGINT			NOT NULL,
		[current_risk_score]   DECIMAL(9,4)		NULL,
		[load_ts]              DATETIME2		NOT NULL DEFAULT SYSDATETIME(),
		[hash_row]             VARBINARY(32)    NULL,
	  CONSTRAINT PK_stg_kb4_user_detail PRIMARY KEY (user_id, load_ts)
	);

	-- 3) Phishing Security Tests (PST's op campagneniveau)
	CREATE TABLE [STG].[Stg_kb4_Pst] (
		[campaign_id]          BIGINT			NOT NULL,
		[pst_id]               BIGINT			NOT NULL,
		[status]               VARCHAR(50)		NULL,
		[name]				   VARCHAR(255)		NULL,
		[started_at]           DATETIME2		NULL,
		[duration_days]		   INT				NULL,
		[load_ts]              DATETIME2		NOT NULL DEFAULT sysdatetime(),
		[hash_row]             VARBINARY(32)    NULL,
	  CONSTRAINT PK_stg_kb4_pst PRIMARY KEY (pst_id)
	);

	-- 4) Recipient results (per gebruiker per PST)
	CREATE TABLE [STG].[Stg_kb4_Pst_Recipient] (
		  [pst_id]               BIGINT				NOT NULL,
		  [user_id]              BIGINT				NOT NULL,
		  [template_id]			 BIGINT				NOT NULL,
		  [template]			 VARCHAR(255)		NULL,
		  [delivered_at]		 DATETIME2			NULL,
		  [opened_at]			 DATETIME2			NULL,
		  [clicked_at]			 DATETIME2			NULL,
		  [replied_at]           DATETIME2			NULL,
		  [attachment_opened_at] DATETIME2			NULL,
		  [macro_enabled_at]     DATETIME2			NULL,
		  [data_entered_at]      DATETIME2			NULL,
		  [qr_code_scanned_at]	 DATETIME2			NULL,
		  [reported_at]			 DATETIME2			NULL,
		  [load_ts]              DATETIME2			NOT NULL DEFAULT sysdatetime(),
		  [hash_row]			 VARBINARY(32)		NULL,
		  CONSTRAINT PK_stg_kb4_pst_recipient PRIMARY KEY (pst_id, user_id)
	);

	-- 5) Training campaigns
	CREATE TABLE [STG].[Stg_kb4_Training_Campaign] (
		  [campaign_id]          BIGINT			NOT NULL,
		  [name]                 VARCHAR(255)	NULL,
		  [status]               VARCHAR(50)	NULL,   -- active/completed/etc.
		  [start_date]           DATE			NULL,
		  [load_ts]              DATETIME2		NOT NULL DEFAULT sysdatetime(),
		  [hash_row]             VARBINARY(32)  NULL,
		  CONSTRAINT PK_stg_kb4_training_campaign PRIMARY KEY (campaign_id)
	);

	-- 6) Training enrollments (per user per campaign)
	CREATE TABLE [STG].[Stg_kb4_Training_Enrollment] (
		  [enrollment_id]        BIGINT				NOT NULL,
		  [campaign_id]          BIGINT				NOT NULL,
		  [user_id]              BIGINT				NOT NULL,
		  [enrollment_date]      DATETIME2			NULL,
		  [status]               VARCHAR(50)		NULL,   -- enrolled/completed/overdue
		  [load_ts]              DATETIME2			NOT NULL DEFAULT sysdatetime(),
		  [hash_row]             VARBINARY(32)		NULL,
		  CONSTRAINT PK_stg_kb4_training_enrollment PRIMARY KEY (enrollment_id)
		);

	-- 7) Hitas users 
	CREATE TABLE [STG].[Stg_hitas_Users] (
			[eHash]             VARBINARY(32)			NOT NULL,
			[Unit]				VARCHAR(50)				NULL,
			[Load_ts]			DATETIME2				NOT NULL DEFAULT sysdatetime(),
			[hash_row]          VARBINARY(32)			NULL,
			 CONSTRAINT PK_stg_Hitas_users PRIMARY KEY (eHash)
		);

	-- 8) Masters -- Unit
	
	CREATE TABLE STG.stg_md_Units(
		Unit_code		VARCHAR(50)		NOT NULL,
		Unit_name		VARCHAR(255)	NOT NULL,
		[load_ts]       DATETIME2		NOT NULL	DEFAULT SYSDATETIME(),
		hash_row		VARBINARY(32)	NULL,
		CONSTRAINT PK_stg_md_Units PRIMARY KEY (Unit_code)
	);

	-- 9) Masters -- Training Campaign
	CREATE TABLE STG.stg_md_Training(
		[Name]			VARCHAR(50)		NOT NULL,
		Active			BIT				NOT NULL,
		[Type]			VARCHAR(1)		NULL,
		[load_ts]       DATETIME2		NOT NULL	DEFAULT SYSDATETIME(),
		hash_row		VARBINARY(32)	NULL,
		CONSTRAINT PK_stg_md_Training PRIMARY KEY ([Name])
	);

	-- 10) Masters -- Phishing Campaign
	CREATE TABLE STG.stg_md_Phishing(
		Campaign_id		BIGINT			NOT NULL,
		[Name]			VARCHAR(50)		NULL,
		[Active]		BIT				NOT NULL,	
		[load_ts]       DATETIME2		NOT NULL	DEFAULT SYSDATETIME(),
		hash_row		VARBINARY(32)	NULL,
		CONSTRAINT PK_stg_md_Phish PRIMARY KEY (Campaign_id)
	);

END
GO

----------------- DIMENSIONS
CREATE SCHEMA DWH;
GO

-- Dimension tables
BEGIN

	CREATE TABLE DWH.dim_user (
	  [user_key]             INT IDENTITY(1,1)	NOT NULL PRIMARY KEY,
	  [user_id]              BIGINT				NOT NULL, -- NK
	  [eHash]                VARBINARY(32)		NULL,
	  [effective_from]		 DATETIME2			NOT NULL,
	  [effective_to]		 DATETIME2			NULL,
	  [is_current]           BIT				NOT NULL,
	  CONSTRAINT UQ_dim_user UNIQUE (user_id, effective_from)
	);
	CREATE INDEX IX_dim_user_nk ON DWH.dim_user (user_id, is_current);
	CREATE TABLE DWH.dim_campaign (
	  [campaign_key]         INT IDENTITY(1,1)	NOT NULL PRIMARY KEY,
	  [campaign_id]          BIGINT				NOT NULL, -- NK
	  [campaign_type]        VARCHAR(20)		NOT NULL, -- 'training'/'phishing'/Policy
	  [name]                 VARCHAR(255)		NULL,
	  [status]               VARCHAR(50)		NULL,
	  [start_date_key]       INT				NULL,    -- FK naar dim_date
	  [effective_from]       DATETIME2			NOT NULL,
	  [effective_to]         DATETIME2			NULL,
	  [is_current]           BIT				NOT NULL,
	  CONSTRAINT UQ_dim_campaign UNIQUE (campaign_id, campaign_type, effective_from)
	);
	ALTER TABLE DWH.dim_campaign
		ADD CONSTRAINT CK_dim_campaign_type
		CHECK (campaign_type IN ('training','phishing','policy'));

	CREATE TABLE DWH.dim_template (
      [template_key]         INT IDENTITY(1,1)	NOT NULL	PRIMARY KEY,
	  [template_id]			 BIGINT				NOT NULL,
	  [template_name]        VARCHAR(255)		NOT NULL,
	  [effective_from]       DATETIME2			NOT NULL,
	  [effective_to]         DATETIME2			NULL,
	  [is_current]           BIT		        NOT NULL,
	  CONSTRAINT UQ_dim_template UNIQUE (template_id, effective_from)
	);
	CREATE TABLE DWH.dim_date (
	  [date_key]             INT				NOT NULL	PRIMARY KEY,    -- yyyymmdd
	  [date]				 DATE				NOT NULL,
	  [year]                 SMALLINT			NOT NULL,
	  [month]                TINYINT			NOT NULL,
	  [day]                  TINYINT			NOT NULL,
	  [week]                 TINYINT			NULL,
	  [quarter]              TINYINT			NULL,
	  [is_weekend]           BIT				NULL
	);
	ALTER TABLE DWH.dim_date
		ADD CONSTRAINT UQ_dim_date UNIQUE ([date]);
	CREATE TABLE DWH.dim_unit (
	  [unit_key]       INT			NOT NULL	IDENTITY(1,1)	PRIMARY KEY,
	  [unit_code]      VARCHAR(50)  NOT NULL,   -- NK
	  [unit_naam]      VARCHAR(255) NOT NULL,
	  [effective_from] DATETIME2    NOT NULL,
	  [effective_to]   DATETIME2    NULL,
	  [is_current]     BIT          NOT NULL,
	CONSTRAINT UQ_dim_unit UNIQUE (unit_code, effective_from)
	);
	ALTER TABLE DWH.dim_campaign
		ADD CONSTRAINT FK_dim_campaign_startdate
		FOREIGN KEY (start_date_key) REFERENCES DWH.dim_date(date_key);
END
GO

------------------ FACTS
BEGIN
	CREATE TABLE DWH.fact_pst_recipient_result (
	  [pst_recipient_key]    BIGINT			NOT NULL	IDENTITY(1,1)	PRIMARY KEY,
	  [user_key]             INT			NOT NULL,
	  [campaign_key]         INT			NOT NULL,   -- type='phishing'
	  [template_key]         INT			NULL,
	  [pst_id]               BIGINT			NOT NULL,   -- degenerate NK voor detaildrill
	  [result_status]        VARCHAR(50)	NULL,
	  [delivered_count]		 INT			NULL,
	  [opens_count]          INT			NULL,
	  [clicks_count]         INT			NULL,
	  [replies_count]        INT			NULL,
	  [attachments_opened]   INT			NULL,
	  [data_entered]         INT			NULL,
	  [reported_count]		 INT			NULL,
	  [started_date_key]     INT			NULL,       -- uit stg_kb4_pst.started_at
	  [duration_seconds]     INT			NULL,
	  [load_ts]              DATETIME2		NOT NULL	DEFAULT SYSDATETIME(),
	  CONSTRAINT FK_pst_user     FOREIGN KEY (user_key)     REFERENCES DWH.dim_user(user_key),
	  CONSTRAINT FK_pst_campaign FOREIGN KEY (campaign_key) REFERENCES DWH.dim_campaign(campaign_key),
	  CONSTRAINT FK_pst_template FOREIGN KEY (template_key) REFERENCES DWH.dim_template(template_key),
	  CONSTRAINT FK_pst_date     FOREIGN KEY (started_date_key) REFERENCES DWH.dim_date(date_key)
	);
	CREATE INDEX IX_fact_pst_user      ON DWH.fact_pst_recipient_result(user_key);
	CREATE INDEX IX_fact_pst_campaign  ON DWH.fact_pst_recipient_result(campaign_key);
	CREATE INDEX IX_fact_pst_template  ON DWH.fact_pst_recipient_result(template_key);
	CREATE INDEX IX_fact_pst_date      ON DWH.fact_pst_recipient_result(started_date_key);
	CREATE TABLE DWH.fact_training_enrollment (
	  [enrollment_key]       BIGINT			NOT NULL	IDENTITY(1,1)	PRIMARY KEY,
	  [user_key]             INT			NOT NULL,
	  [campaign_key]         INT			NOT NULL,   -- type='training'
	  [enrollment_id]        BIGINT			NOT NULL,   -- degenerate NK
	  [status]               VARCHAR(50)	NULL,		-- enrolled/completed/overdue
	  [enrollment_date_key]  INT			NULL,
	  [completion_date_key]  INT			NULL,       -- te vullen als beschikbaar
	  [load_ts]              DATETIME2		NOT NULL DEFAULT sysdatetime(),
	  CONSTRAINT FK_te_user     FOREIGN KEY (user_key)     REFERENCES DWH.dim_user(user_key),
	  CONSTRAINT FK_te_campaign FOREIGN KEY (campaign_key) REFERENCES DWH.dim_campaign(campaign_key),
	  CONSTRAINT FK_te_enrdate  FOREIGN KEY (enrollment_date_key) REFERENCES DWH.dim_date(date_key),
	  CONSTRAINT FK_te_compdate FOREIGN KEY (completion_date_key) REFERENCES DWH.dim_date(date_key)
	);
	CREATE INDEX IX_fact_te_user       ON DWH.fact_training_enrollment(user_key);
	CREATE INDEX IX_fact_te_campaign   ON DWH.fact_training_enrollment(campaign_key);
	CREATE INDEX IX_fact_te_enrdate    ON DWH.fact_training_enrollment(enrollment_date_key);
	CREATE INDEX IX_fact_te_compdate   ON DWH.fact_training_enrollment(completion_date_key);
	CREATE TABLE DWH.fact_user_security_snapshot (
	  [snapshot_key]         BIGINT			NOT NULL	IDENTITY(1,1)	PRIMARY KEY,
	  [user_key]             INT			NOT NULL,
	  [snapshot_date_key]    INT			NOT NULL,
	  [phish_prone_pct]      DECIMAL(5,2)	NULL,
	  [current_risk_score]   DECIMAL(9,4)	NULL,
	  [load_ts]              DATETIME2		NOT NULL	DEFAULT SYSDATETIME(),
	  CONSTRAINT UQ_user_day UNIQUE (user_key, snapshot_date_key),
	  CONSTRAINT FK_snap_user FOREIGN KEY (user_key) REFERENCES DWH.dim_user(user_key),
	  CONSTRAINT FK_snap_date FOREIGN KEY (snapshot_date_key) REFERENCES DWH.dim_date(date_key)
	);
	CREATE INDEX IX_fact_snap_user     ON DWH.fact_user_security_snapshot(user_key);
	CREATE INDEX IX_fact_snap_date     ON DWH.fact_user_security_snapshot(snapshot_date_key);
	CREATE TABLE DWH.bridge_user_unit (
	  [user_key]			INT				NOT NULL,
	  [unit_key]			INT				NOT NULL,
	  [start_date_key]		INT				NOT NULL,	-- FK dim_date
	  [end_date_key]		INT				NULL,		-- FK dim_date (NULL = lopend)
	  [is_current]			BIT				NOT NULL,
	  [allocation_wt]		DECIMAL(9,4)	NULL,		-- optioneel (default 1)
	  CONSTRAINT PK_bridge_user_unit PRIMARY KEY (user_key, unit_key, start_date_key),
	  CONSTRAINT FK_buu_user FOREIGN KEY (user_key) REFERENCES DWH.dim_user(user_key),
	  CONSTRAINT FK_buu_unit FOREIGN KEY (unit_key) REFERENCES DWH.dim_unit(unit_key)
	);
	CREATE INDEX IX_buu_user           ON DWH.bridge_user_unit(user_key);
	CREATE INDEX IX_buu_unit           ON DWH.bridge_user_unit(unit_key);
	CREATE INDEX IX_buu_dates          ON DWH.bridge_user_unit(start_date_key, end_date_key);
END
GO

------------------ MASTER TABLES
CREATE SCHEMA MST;
GO

BEGIN
	-- Units
	CREATE TABLE [MST].[MST_Units](
		[Unit]				VARCHAR(50)			NOT NULL,
		[Unit Name]			VARCHAR(50)			NULL,
	 CONSTRAINT [PK_MST_Units] PRIMARY KEY (Unit)
	 );
	-- Phishing campaigns
	CREATE TABLE [MST].[MST_Phishing_Campaigns](
		[Campaign_id]		BIGINT				NOT NULL,
		[Name]				VARCHAR(50)			NULL,
		[Active]			BIT NOT NULL,
		CONSTRAINT [PK_Stam_Phishing_Campaigns] PRIMARY KEY  ([Campaign_id]) 
	);
	-- Training campaigns
	CREATE TABLE [MST].[MST_Training_Campaigns](
		[Name]				VARCHAR(50)			NOT NULL,
		[Active]			BIT					NOT NULL,
		[Type]				VARCHAR(1)			NULL,
		CONSTRAINT [PK_Stam_Training_Campaigns] PRIMARY KEY ([Name])
	);
	-- Targets
	CREATE TABLE [MST].[MST_Targets](
		[KPI]				VARCHAR(20)			NOT NULL,
		[Target]			DECIMAL(5,2)		NULL,
		[Active]			BIT					NOT NULL DEFAULT 1,
		[Active_From]		DATE				NULL,
		[Active_To]			DATE				NULL,
		CONSTRAINT [PK_MST_Targets] PRIMARY KEY CLUSTERED (KPI)
		);
	CREATE TABLE [MST].[Logdata](
		[Timestamp]		DATETIME2		NOT NULL,
		[Source]		VARCHAR(10)		NULL,
		[Severity]		VARCHAR(4)		NULL,
		[Log regel]		nVARCHAR(MAX)	NULL,
		CONSTRAINT [PK_Logdata] PRIMARY KEY CLUSTERED ([Timestamp])
	);
	CREATE TABLE [MST].[Source_status](
		[Source]				VARCHAR(10)		NOT NULL,
		[Run_date]				DATE			NULL,
		[Max_Fetches]			INT				NULL,
		[Fetched_today]			INT				NULL,
		[Warning_Percentage]	INT				NULL,
		[Error_Percentage]		INT				NULL,
		[Finished]				BIT				NOT NULL,
		CONSTRAINT [PK_MST_Source_status] PRIMARY KEY CLUSTERED (Source)
	);
END
GO

-- Filling master tables
BEGIN
	INSERT INTO [MST].[MST_Units](Unit, [Unit name]) VALUES('02.Business', 'Business')
	INSERT INTO [MST].[MST_Units](Unit, [Unit name]) VALUES('01.BI', 'Business Intelligence')
	INSERT INTO [MST].[MST_Units](Unit, [Unit name]) VALUES('03.Change', 'Change, Governance en Privacy')
	INSERT INTO [MST].[MST_Units](Unit, [Unit name]) VALUES('04.DataEng', 'Data Engineering')
	INSERT INTO [MST].[MST_Units](Unit, [Unit name]) VALUES('00.FF', 'Future Facts')
	INSERT INTO [MST].[MST_Units](Unit, [Unit name]) VALUES('Groep', 'HI Groep')
	INSERT INTO [MST].[MST_Units](Unit, [Unit name]) VALUES('08.HIBrid', 'Hibrid')
	INSERT INTO [MST].[MST_Units](Unit, [Unit name]) VALUES('06.INT', 'Intern')
	INSERT INTO [MST].[MST_Units](Unit, [Unit name]) VALUES('05.PlatEng', 'Platform Engineering')

	INSERT INTO [MST].[MST_Phishing_Campaigns](Campaign_id, [Name], Active) VALUES(243817, 'Baseline test', 0)
	INSERT INTO [MST].[MST_Phishing_Campaigns](Campaign_id, [Name], Active) VALUES(245938, 'Baseline', 0)
	INSERT INTO [MST].[MST_Phishing_Campaigns](Campaign_id, [Name], Active) VALUES(295408, 'Standaard campagne mei-oktober 2022', 0)
	INSERT INTO [MST].[MST_Phishing_Campaigns](Campaign_id, [Name], Active) VALUES(378282, 'Campagne November 22 - April 23', 0)
	INSERT INTO [MST].[MST_Phishing_Campaigns](Campaign_id, [Name], Active) VALUES(459178, 'Mei 2023 - Okt 2023', 0)
	INSERT INTO [MST].[MST_Phishing_Campaigns](Campaign_id, [Name], Active) VALUES(555830, 'Campagne nieuw', 1)
	INSERT INTO [MST].[MST_Phishing_Campaigns](Campaign_id, [Name], Active) VALUES(764860, 'Whitelist test', 0)
	INSERT INTO [MST].[MST_Phishing_Campaigns](Campaign_id, [Name], Active) VALUES(765406, 'Whitelist test Clone', 0)
	INSERT INTO [MST].[MST_Phishing_Campaigns](Campaign_id, [Name], Active) VALUES(905307, 'Callback Phishing', 1)

	INSERT INTO [MST].[MST_Training_Campaigns]([Name], Active, [type]) VALUES('The Inside Man', 1, 'T')
	INSERT INTO [MST].[MST_Training_Campaigns]([Name], Active, [type]) VALUES('Beleid', 1, 'P')

	INSERT INTO [MST].[MST_Targets](KPI, [Target], [Active_From]) VALUES('Risico score', 32.5, '07/01/2025')
	INSERT INTO [MST].[MST_Targets](KPI, [Target], [Active_From]) VALUES('Gerapporteerd', 80, '07/01/2025')
	INSERT INTO [MST].[MST_Targets](KPI, [Target], [Active_From]) VALUES('Phish Prone', 3.1, '07/01/2025')
	INSERT INTO [MST].[MST_Targets](KPI, [Target], [Active_From]) VALUES('Bekeken', 90, '07/01/2025')
	INSERT INTO [MST].[MST_Targets](KPI, [Target], [Active_From]) VALUES('Policy', 95, '07/01/2025')

	INSERT INTO [MST].[Source_status]([Source],Run_date, Max_fetches, Fetched_today, Warning_Percentage, Error_Percentage, finished) VALUES('KnowBe4',SYSDATETIME(),2000,0,75, 90, 0);
	INSERT INTO [MST].[Source_status]([Source], Finished) VALUES('Hitas', 0);
	INSERT INTO [MST].[Source_status]([Source], Finished) VALUES('DWH', 0);
	INSERT INTO [MST].[Source_status]([Source], Finished) VALUES('Master', 0);
END
GO