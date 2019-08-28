
/********************************
*********************************
**                             **
**       WhoUsesWhat.sql       **
**   2008 R2 Extended Events   **
**                             **
*********************************
********************************/

/* Create the event session
IF EXISTS
(
    SELECT *
    FROM sys.server_event_sessions
    WHERE name = 'WhoUsesWhat'
)
    DROP EVENT SESSION [WhoUsesWhat] ON SERVER;
CREATE EVENT SESSION [WhoUsesWhat]
ON SERVER
    ADD EVENT sqlserver.module_end
    (ACTION
     (
         sqlserver.client_hostname,
         sqlserver.database_id,
         sqlserver.username
     )
     WHERE (
           (
               [package0].[greater_than_uint64]([sqlserver].[database_id], (5))
               AND [package0].[equal_boolean]([sqlserver].[is_system], (0))
               AND [package0].[not_equal_i_ansi_string]( [sqlserver].[username], 'POWERBIZ\sqlservice3' )
			   AND [object_name] <> N'sp_procedure_params_100_managed'
			   AND [object_name] <> N'sp_reset_connection'
           )
           )
    )
    ADD TARGET package0.asynchronous_file_target
    (SET filename = 'E:\XE_Files\WhoUsesWhat.etl', metadatafile = 'E:\XE_Files\WhoUsesWhat.mta', max_file_size =(512), max_rollover_files=(20))
WITH
(
    MAX_MEMORY = 4096KB,
    EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
    MAX_DISPATCH_LATENCY = 30 SECONDS,
    MAX_EVENT_SIZE = 0KB,
    MEMORY_PARTITION_MODE = NONE,
    TRACK_CAUSALITY = OFF,
    STARTUP_STATE = OFF
);
--*/
GO

/* Start the event session
ALTER EVENT SESSION [WhoUsesWhat] ON SERVER STATE = START;
--*/
GO

/* Stop the event session
ALTER EVENT SESSION [WhoUsesWhat] ON SERVER STATE = STOP;
--*/
GO

/* Query the event file
IF OBJECT_ID('tempdb..#xml') IS NOT NULL
    DROP TABLE #xml

CREATE TABLE #xml ( [event_data] XML )

PRINT CONVERT(CHAR(19),GETDATE(),120) + ' - Inserting into #xml';
GO

INSERT INTO #xml
SELECT event_data = CONVERT(XML,event_data) FROM sys.fn_xe_file_target_read_file('E:\XE_Files\WhoUsesWhat_0_131366448173740000.etl', 'E:\XE_Files\WhoUsesWhat_0_131363005932340000.mta', NULL, NULL)

PRINT CONVERT(CHAR(19),GETDATE(),120) + ' - Inserting into mercury_archive.history.XE_Data';
GO

INSERT INTO mercury_archive.history.XE_Data
SELECT 
  [client_hostname] = event_data.value(N'(event/action[@name="client_hostname"])[1]', N'nvarchar(max)'),
  [username] = event_data.value(N'(event/action[@name="username"]/value)[1]', N'nvarchar(max)'),
  [database] = d.name,
  [object_type] = event_data.value(N'(event/data[@name="object_type"])[1]', N'nvarchar(max)'),
  [object_name] = event_data.value(N'(event/data[@name="object_name"])[1]', N'nvarchar(max)'),
  [timestamp] = DATEADD(HOUR, -4, event_data.value(N'(event/@timestamp)[1]', N'datetime'))
FROM #xml
INNER JOIN sys.databases d ON #xml.event_data.value(N'(event/action[@name="database_id"])[1]', N'nvarchar(max)') = d.database_id

PRINT CONVERT(CHAR(19),GETDATE(),120) + ' - Import complete';
GO

SELECT DISTINCT [database], object_type, [object_name] FROM mercury_archive.history.XE_Data ORDER BY [database], object_type, [object_name]

DROP TABLE #xml;
--*/
GO

