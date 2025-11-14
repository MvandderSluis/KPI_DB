USE [KPI DB]
GO

/****** Object:  Table [dbo].[Users]    Script Date: 7-11-2025 13:05:18 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Users]') AND type in (N'U'))
	DROP TABLE [dbo].[Users]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Phishing_tests]') AND type in (N'U'))
	DROP TABLE [dbo].[Phishing_tests]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Hitas_Users]') AND type in (N'U'))
	DROP TABLE [dbo].[Hitas_Users]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Training_Campaigns]') AND type in (N'U'))
	DROP TABLE [dbo].[Training_Campaigns]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Stam_Units]') AND type in (N'U'))
	DROP TABLE [dbo].[Stam_Units]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Stam_Phishing_Campaigns]') AND type in (N'U'))
	DROP TABLE [dbo].[Stam_Phishing_Campaigns]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Stam_Training_Campaigns]') AND type in (N'U'))
	DROP TABLE [dbo].[Stam_Training_Campaigns]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Stam_Targets]') AND type in (N'U'))
	DROP TABLE [dbo].[Stam_Targets]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Historie_Units]') AND type in (N'U'))
	DROP TABLE [dbo].[Historie_Units]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Historie_Total]') AND type in (N'U'))
	DROP TABLE [dbo].[Historie_Total]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Logdata]') AND type in (N'U'))
	DROP TABLE [dbo].[Logdata]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Vw_Active_Users]') AND type in (N'V'))
	DROP VIEW [dbo].[Vw_Active_Users]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Vw_Active_Phishing_Campaigns]') AND type in (N'V'))
	DROP VIEW [dbo].[Vw_Active_Phishing_Campaigns]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Vw_Active_Training_Campaigns]') AND type in (N'V'))
	DROP VIEW [dbo].[Vw_Active_Training_Campaigns]
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[set_history]') AND type in (N'P'))
	DROP PROCEDURE [dbo].[set_history]
GO

-- TABLES

CREATE TABLE [dbo].[Users](
	[Id] [int] NOT NULL,
	[Ehash] VARBINARY(32) NULL,
	[Phish_prone_percentage] [float] NULL,
	[current_risk_score] [float] NULL,
	[Percentage_Response] [float] NULL,
	[Percentage_Viewed] [float] NULL,
	[Percentage_Policies] [float] NULL,
	[Status] [nchar](10) NULL,
 CONSTRAINT [PK_Users] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Phishing_tests](
	[campaign_id] [int] NOT NULL,
	[pst_id] [int] NOT NULL,
	[status] [varchar](50) NULL,
	[name] [varchar](255) NULL,
	[started_at] [datetime] NULL,
	[duration] [int] NULL,
 CONSTRAINT [PK_Phishing_tests] PRIMARY KEY CLUSTERED 
(
	[campaign_id], [pst_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Hitas_Users](
	[Ehash] [varbinary](32) NOT NULL,
	[Groep] [varchar](50) NULL,
 CONSTRAINT [PK_Hitas_Users] PRIMARY KEY CLUSTERED 
(
	[Ehash] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Training_Campaigns](
	[campaign_id] [Int] NOT NULL,
	[name] [varchar](255) NULL,
	[status] [varchar](50) NULL,
	[start_date] [DateTime] NULL,
	CONSTRAINT [PK_Training_Campaigns] PRIMARY KEY CLUSTERED 
(
	[campaign_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Stam_Units](
	[Unit] [varchar](50) NOT NULL,
	[Unit naam] [varchar](50) NULL,
 CONSTRAINT [PK_Stam_Units] PRIMARY KEY CLUSTERED 
(
	[Unit] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Stam_Phishing_Campaigns](
	[Campaign_id] [int] NOT NULL,
	[Naam] VARCHAR(50) NULL,
	[Actief] [bit] NOT NULL,
	CONSTRAINT [PK_Stam_Phishing_Campaigns] PRIMARY KEY CLUSTERED 
(
	[Campaign_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Stam_Training_Campaigns](
	[Naam] VARCHAR(50) NOT NULL,
	[Actief] [bit] NOT NULL,
	[Type] VARCHAR(1) NULL,
	CONSTRAINT [PK_Stam_Training_Campaigns] PRIMARY KEY CLUSTERED 
(
	[Naam] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Stam_Targets](
	[KPI] [VARCHAR](20) NOT NULL,
	[Target] [FLOAT] NULL,
	CONSTRAINT [PK_Stam_Targets] PRIMARY KEY CLUSTERED 
(
	[KPI] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Historie_Units](
	[Date] [DATETIME] NOT NULL,
	[Unit] [VARCHAR](50) NOT NULL,
	[Phish_prone_percentage] [float] NULL,
	[current_risk_score] [float] NULL,
	[Percentage_Response] [float] NULL,
	[Percentage_Viewed] [float] NULL,
	[Percentage_Policies] [float] NULL,
 CONSTRAINT [PK_Historie_Units] PRIMARY KEY CLUSTERED 
(
	[Date],[Unit] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Historie_Total](
	[Date] [DATETIME] NOT NULL,
	[Phish_prone_percentage] [float] NULL,
	[current_risk_score] [float] NULL,
	[Percentage_Response] [float] NULL,
	[Percentage_Viewed] [float] NULL,
	[Percentage_Policies] [float] NULL,
 CONSTRAINT [PK_Historie_Total] PRIMARY KEY CLUSTERED 
(
	[Date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[Logdata](
	[Timestamp] [DATETIME] NOT NULL,
	[Log regel] [VARCHAR](255) NULL,
	CONSTRAINT [PK_Logdata] PRIMARY KEY CLUSTERED 
(
	[Timestamp] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

-- Views
CREATE VIEW [dbo].[Vw_Active_Users]
AS
SELECT id
	FROM [dbo].[Users] AS U
	JOIN [dbo].[Hitas_Users] AS H ON H.Ehash=U.Ehash
	JOIN [dbo].[Stam_Units] AS SU ON SU.Unit=H.Groep
GO

CREATE VIEW [dbo].[Vw_Active_Phishing_Campaigns]
AS
SELECT P.pst_id
	FROM [dbo].[Phishing_tests] AS P
	JOIN [dbo].[Stam_Phishing_Campaigns] AS S ON S.Campaign_id=P.campaign_id
	WHERE S.Actief=1 
	AND DATEADD(DAY,[duration],DATEADD(YEAR, 2, [started_at])) <= CONVERT(DATETIME,CAST(YEAR(SYSDATETIME()) AS VARCHAR(4)) + '-' + CAST(MONTH(SYSDATETIME()) AS VARCHAR(2)) + '-01', 102) - 1;
	-- ALs datum_actief + duration (dagen) meer dan twee jaar in het verleden ligt moet deze niet worden meegenomen
GO

CREATE VIEW [dbo].[Vw_Active_Training_Campaigns]
AS
SELECT T.[campaign_id], S.[Type]
	FROM [dbo].[Training_Campaigns] AS T
	JOIN [dbo].[Stam_Training_Campaigns] AS S ON S.[Naam] = LEFT(T.[Name], LEN(S.[Naam]))
	WHERE T.Status = 'In Progress' 
		AND T.start_date < CONVERT(DATETIME,CAST(YEAR(SYSDATETIME()) AS VARCHAR(4)) + '-' + CAST(MONTH(SYSDATETIME()) AS VARCHAR(2)) + '-01', 102) - 1
GO

-- Vullen van tabellen
INSERT INTO [dbo].[Stam_Units](Unit, [Unit naam]) VAlues('02.Business', 'Business')
INSERT INTO [dbo].[Stam_Units](Unit, [Unit naam]) VAlues('01.BI', 'Business Intelligence')
INSERT INTO [dbo].[Stam_Units](Unit, [Unit naam]) VAlues('03.Change', 'Change, Governance en Privacy')
INSERT INTO [dbo].[Stam_Units](Unit, [Unit naam]) VAlues('04.DataEng', 'Data Engineering')
INSERT INTO [dbo].[Stam_Units](Unit, [Unit naam]) VAlues('00.FF', 'Future Facts')
INSERT INTO [dbo].[Stam_Units](Unit, [Unit naam]) VAlues('Groep', 'HI Groep')
INSERT INTO [dbo].[Stam_Units](Unit, [Unit naam]) VAlues('08.HIBrid', 'Hibrid')
INSERT INTO [dbo].[Stam_Units](Unit, [Unit naam]) VAlues('06.INT', 'Intern')
INSERT INTO [dbo].[Stam_Units](Unit, [Unit naam]) VAlues('05.PlatEng', 'Platform Engineering')

INSERT INTO [dbo].[Stam_Phishing_Campaigns](Campaign_id, Naam, Actief) VALUES(243817, 'Baseline test', 0)
INSERT INTO [dbo].[Stam_Phishing_Campaigns](Campaign_id, Naam, Actief) VALUES(245938, 'Baseline', 0)
INSERT INTO [dbo].[Stam_Phishing_Campaigns](Campaign_id, Naam, Actief) VALUES(295408, 'Standaard campagne mei-oktober 2022', 0)
INSERT INTO [dbo].[Stam_Phishing_Campaigns](Campaign_id, Naam, Actief) VALUES(378282, 'Campagne November 22 - April 23', 0)
INSERT INTO [dbo].[Stam_Phishing_Campaigns](Campaign_id, Naam, Actief) VALUES(459178, 'Mei 2023 - Okt 2023', 0)
INSERT INTO [dbo].[Stam_Phishing_Campaigns](Campaign_id, Naam, Actief) VALUES(555830, 'Campagne nieuw', 1)
INSERT INTO [dbo].[Stam_Phishing_Campaigns](Campaign_id, Naam, Actief) VALUES(764860, 'Whitelist test', 0)
INSERT INTO [dbo].[Stam_Phishing_Campaigns](Campaign_id, Naam, Actief) VALUES(765406, 'Whitelist test Clone', 0)
INSERT INTO [dbo].[Stam_Phishing_Campaigns](Campaign_id, Naam, Actief) VALUES(905307, 'Callback Phishing', 1)

INSERT INTO [dbo].[Stam_Training_Campaigns](Naam, Actief, [type]) VALUES('The Inside Man', 1, 'T')
INSERT INTO [dbo].[Stam_Training_Campaigns](Naam, Actief, [type]) VALUES('Beleid', 1, 'P')

INSERT INTO [dbo].[Stam_Targets](KPI, [Target]) VALUES('Risico score', 32.5)
INSERT INTO [dbo].[Stam_Targets](KPI, [Target]) VALUES('Gerapporteerd', 80)
INSERT INTO [dbo].[Stam_Targets](KPI, [Target]) VALUES('Phish Prone', 3.1)
INSERT INTO [dbo].[Stam_Targets](KPI, [Target]) VALUES('Bekeken', 90)
INSERT INTO [dbo].[Stam_Targets](KPI, [Target]) VALUES('Policy', 95)

INSERT INTO [dbo].[Historie_Total]([Date], Phish_prone_percentage,current_risk_score,Percentage_Response,Percentage_Viewed) VALUES('07/01/2025', 3.3, 34, 58, 84)
INSERT INTO [dbo].[Historie_Total]([Date], Phish_prone_percentage,current_risk_score,Percentage_Response,Percentage_Viewed) VALUES('08/01/2025', 3.09, 33.27, 64, 82)
INSERT INTO [dbo].[Historie_Total]([Date], Phish_prone_percentage,current_risk_score,Percentage_Response,Percentage_Viewed) VALUES('09/01/2025', 2.8, 35.9, 62, 85)
INSERT INTO [dbo].[Historie_Total]([Date], Phish_prone_percentage,current_risk_score,Percentage_Response,Percentage_Viewed) VALUES('10/01/2025', 2.9, 34.48, 61, 85)
INSERT INTO [dbo].[Historie_Total]([Date], Phish_prone_percentage,current_risk_score,Percentage_Response,Percentage_Viewed) VALUES('11/01/2025', 3.36, 34.22, 61, 85)
GO

CREATE PROCEDURE [dbo].[set_history]
AS
BEGIN
	DECLARE @Nbr_Users_total AS INT;
	DECLARE @Nbr_Users_group AS INT;
	DECLARE @Nbr AS INT;
	DECLARE @Units AS CURSOR;
	DECLARE @USERS AS CURSOR;
	DECLARE @Unit_Code AS VARCHAR(50);
	DECLARE @PhishProne AS FLOAT;
	DECLARE @RiskScore AS FLOAT;
	DECLARE @PercResponse AS FLOAT;
	DECLARE @PercViewed AS FLOAT;
	DECLARE @PercPolicy AS FLOAT;
	DECLARE @PhishProne_Tot AS FLOAT;
	DECLARE @RiskScore_Tot AS FLOAT;
	DECLARE @PercResponse_Tot AS FLOAT;
	DECLARE @PercViewed_Tot AS FLOAT;
	DECLARE @PercPolicy_Tot AS FLOAT;
	DECLARE @PhishProne_Grp AS FLOAT;
	DECLARE @RiskScore_Grp AS FLOAT;
	DECLARE @PercResponse_Grp AS FLOAT;
	DECLARE @PercViewed_Grp AS FLOAT;
	DECLARE @PercPolicy_Grp AS FLOAT;
	DECLARE @Date AS DATE;
	SET @Nbr_Users_total = 0;
	SET @PhishProne_Tot = 0;
	SET @RiskScore_Tot = 0;
	SET @PercResponse_Tot = 0;
	SET @PercViewed_Tot = 0;
	SET @PercPolicy_Tot = 0;
	SET @Date = CONVERT(DATETIME,CAST(YEAR(SYSDATETIME()) AS VARCHAR(4)) + '-' + CAST(MONTH(SYSDATETIME()) AS VARCHAR(2)) + '-01', 102)
	SET @Units = CURSOR FOR SELECT [Unit] FROM [dbo].[Stam_Units];
	OPEN @Units;
	FETCH NEXT FROM @Units INTO @Unit_code;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @Nbr_Users_group = 0;
		SET @PhishProne_Grp = 0;
		SET @RiskScore_Grp = 0;
		SET @PercResponse_Grp = 0;
		SET @PercViewed_Grp = 0;
		SET @PercPolicy_Grp = 0;
		SET @USERS = CURSOR FOR 
			SELECT Phish_prone_percentage, current_risk_score, Percentage_Response, Percentage_Viewed, Percentage_Policies
				FROM [dbo].[Users] AS U
				JOIN [dbo].Hitas_Users AS H ON H.Ehash=U.Ehash
				WHERE Groep = @Unit_Code;
		OPEN @USERS;
		FETCH NEXT FROM @USERS INTO @PhishProne, @RiskScore, @PercResponse, @PercViewed, @PercPolicy;
		WHILE @@FETCH_STATUS = 0
			BEGIN
				SET @Nbr_Users_Group = @Nbr_Users_Group + 1;
				SET @PhishProne_Grp = @PhishProne_Grp + @PhishProne;
				SET @RiskScore_Grp = @RiskScore_Grp + @RiskScore;
				SET @PercResponse_Grp = @PercResponse_Grp + @PercResponse;
				SET @PercViewed_Grp = @PercViewed_Grp + @PercViewed;
				SET @PercPolicy_Grp = @PercPolicy_Grp + @PercPolicy;
				FETCH NEXT FROM @USERS INTO @PhishProne, @RiskScore, @PercResponse, @PercViewed, @PercPolicy;
			END
		IF @Nbr_Users_group > 0
			BEGIN
				SELECT @Nbr=COUNT(*) FROM [dbo].[Historie_Units]
					WHERE [Date] = @Date AND [Unit] = @Unit_Code;
				IF @Nbr > 0
					BEGIN
						UPDATE [dbo].[Historie_Units]
							SET Phish_prone_percentage = ROUND(@PhishProne_Grp/@Nbr_Users_group,2),
								current_risk_score = ROUND(@RiskScore_Grp/@Nbr_Users_group,2),
								Percentage_Response = ROUND(@PercResponse_Grp/@Nbr_Users_group,2),
								Percentage_Viewed = ROUND(@PercViewed_Grp/@Nbr_Users_group,2),
								Percentage_Policies = ROUND(@PercPolicy_Grp/@Nbr_Users_group,2)
							WHERE [Date] = @Date AND [Unit] = @Unit_Code;
					END
				ELSE
					BEGIN
						INSERT INTO [dbo].[Historie_Units]([Date], [Unit], Phish_prone_percentage, current_risk_score, Percentage_Viewed, Percentage_Response, Percentage_Policies)
							VALUES(@Date, @Unit_Code, ROUND(@PhishProne_Grp/@Nbr_Users_group,2),
									ROUND(@RiskScore_Grp/@Nbr_Users_group,2), ROUND(@PercResponse_Grp/@Nbr_Users_group,2), 
									ROUND(@PercViewed_Grp/@Nbr_Users_group,2), ROUND(@PercPolicy_Grp/@Nbr_Users_group,2));
					END
				SET @Nbr_Users_total = @Nbr_Users_total + @Nbr_Users_group;
				SET @PhishProne_Tot = @PhishProne_Tot + @PhishProne_Grp;
				SET @RiskScore_Tot = @RiskScore_Tot + @RiskScore_Grp;
				SET @PercResponse_Tot = @PercResponse_Tot + @PercResponse_Grp;
				SET @PercViewed_Tot = @PercViewed_Tot + @PercViewed_Grp;
				SET @PercPolicy_Tot = @PercPolicy_Tot + @PercPolicy_Grp;
			END;
		CLOSE @USERS;
		FETCH NEXT FROM @Units INTO @Unit_code;
	END
	SELECT @Nbr=COUNT(*) FROM [dbo].[Historie_Total]
				WHERE [Date] = @Date;
	IF @Nbr > 0
		UPDATE [dbo].[Historie_Total]
			SET Phish_prone_percentage = ROUND(@PhishProne_Tot/@Nbr_Users_total,2),
				current_risk_score = ROUND(@RiskScore_Tot/@Nbr_Users_total,2),
				Percentage_Response = ROUND(@PercResponse_Tot/@Nbr_Users_total,2),
				Percentage_Viewed = ROUND(@PercViewed_Tot/@Nbr_Users_total,2),
				Percentage_Policies = ROUND(@PercPolicy_Tot/@Nbr_Users_total,2)
			WHERE [Date] = @Date;
	ELSE
		INSERT INTO [dbo].[Historie_Total]([Date], Phish_prone_percentage, current_risk_score, Percentage_Viewed, Percentage_Response, Percentage_Policies)
					VALUES(@Date, ROUND(@PhishProne_Tot/@Nbr_Users_total,2), ROUND(@RiskScore_Tot/@Nbr_Users_total,2), 
					ROUND(@PercResponse_Tot/@Nbr_Users_total,2), ROUND(@PercViewed_Tot/@Nbr_Users_total,2), ROUND(@PercPolicy_Tot/@Nbr_Users_total,2));
	DEALLOCATE @USERS;
	CLOSE @Units;
	DEALLOCATE @Units;
END
GO