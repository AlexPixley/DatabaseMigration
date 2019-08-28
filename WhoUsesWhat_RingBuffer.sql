
/*******************************************
********************************************
**                                        **
**       WhoUsesWhat_RingBuffer.sql       **
**        2008 R2 Extended Events         **
**                                        **
********************************************
*******************************************/


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
               AND [object_name] <> N'sp_procedure_params_100_managed'
			   AND [object_name] <> N'sp_reset_connection'
           )
           )
    )
    ADD TARGET package0.ring_buffer
WITH ( MAX_MEMORY = 4096KB
     , EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS -- Switch to ALLOW_MULTIPLE_EVENT_LOSS if this is hurting the server
     , MAX_DISPATCH_LATENCY = 30 SECONDS
     , MAX_EVENT_SIZE = 0KB
     , MEMORY_PARTITION_MODE = NONE
     , TRACK_CAUSALITY = OFF
     , STARTUP_STATE = OFF
     )
;
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


/* Create the table for results
USE DBA;

CREATE TABLE [dbo].[XE_Data](
	[XE_Data_ID] [bigint] IDENTITY(1,1) NOT NULL,
	[server] [sysname] NOT NULL,
	[timestamp] [datetime] NULL,
	[client_hostname] [nvarchar](128) NULL,
	[username] [nvarchar](128) NULL,
	[database] [nvarchar](128) NULL,
	[object_type] [nvarchar](16) NULL,
	[object_name] [nvarchar](128) NULL,
CONSTRAINT [PK_XE_Data_ID] PRIMARY KEY CLUSTERED ([XE_Data_ID] ASC)) ON [PRIMARY]
GO

ALTER TABLE [dbo].[XE_Data] ADD  CONSTRAINT [DF_XE_Data_server]  DEFAULT (@@servername) FOR [server]
GO
--*/


/* Query the event ringbuffer
IF OBJECT_ID('tempdb..#capture_waits_data') IS NOT NULL
    DROP TABLE #capture_waits_data;

CREATE TABLE #capture_waits_data
    (
        [XML_ID] BIGINT NOT NULL IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
		[targetdata] XML NULL
    );

INSERT INTO #capture_waits_data ( [targetdata] )
            SELECT targetdata = CAST(xet.target_data AS XML)
            FROM   sys.dm_xe_session_targets xet
                   JOIN sys.dm_xe_sessions xes ON xes.address = xet.event_session_address
            WHERE  xes.name = 'WhoUsesWhat'
                   AND xet.target_name = 'ring_buffer';

INSERT INTO DBA.dbo.XE_Data ([timestamp], [client_hostname], [username], [database], [object_type], [object_name])
SELECT [timestamp] = DATEADD(HOUR, -4, xed.event_data.value('(@timestamp)[1]', 'datetime')) ,
       [client_hostname] = xed.event_data.value('(action[@name="client_hostname"])[1]', 'nvarchar(max)') ,
       [username] = xed.event_data.value('(action[@name="username"]/value)[1]', 'nvarchar(max)') ,
       [database] = d.name,
       [object_type] = event_data.value(N'(data[@name="object_type"])[1]', N'nvarchar(max)'),
       [object_name] = event_data.value(N'(data[@name="object_name"])[1]', N'nvarchar(max)')
FROM   #capture_waits_data cwd
       CROSS APPLY targetdata.nodes('//RingBufferTarget/event') xed(event_data)
       INNER JOIN sys.databases d ON cwd.targetdata.value(N'(//RingBufferTarget/event/action[@name="database_id"])[1]', N'nvarchar(max)') = d.database_id
--*/
GO


/* Create job to query the event ringbuffer
USE [msdb]
GO

/****** Object:  Job [XE_Data] ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]] ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'XE_Data', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'CONTOSO\SQLAgentService', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Gather XE_Data] ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Gather XE_Data', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET QUOTED_IDENTIFIER ON

IF OBJECT_ID(''tempdb..#capture_waits_data'') IS NOT NULL
    DROP TABLE #capture_waits_data;

CREATE TABLE #capture_waits_data
    (
        [XML_ID] BIGINT NOT NULL IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
		[targetdata] XML NULL
    );

INSERT INTO #capture_waits_data ( [targetdata] )
            SELECT targetdata = CAST(xet.target_data AS XML)
            FROM   sys.dm_xe_session_targets xet
                   JOIN sys.dm_xe_sessions xes ON xes.address = xet.event_session_address
            WHERE  xes.name = ''WhoUsesWhat''
                   AND xet.target_name = ''ring_buffer'';

INSERT INTO DBA.dbo.XE_Data ([timestamp], [client_hostname], [username], [database], [object_type], [object_name])
SELECT [timestamp] = DATEADD(HOUR, -4, xed.event_data.value(''(@timestamp)[1]'', ''datetime'')) ,
       [client_hostname] = xed.event_data.value(''(action[@name="client_hostname"])[1]'', ''nvarchar(max)'') ,
       [username] = xed.event_data.value(''(action[@name="username"]/value)[1]'', ''nvarchar(max)'') ,
       [database] = d.name,
       [object_type] = event_data.value(N''(data[@name="object_type"])[1]'', N''nvarchar(max)''),
       [object_name] = event_data.value(N''(data[@name="object_name"])[1]'', N''nvarchar(max)'')
FROM   #capture_waits_data cwd
       CROSS APPLY targetdata.nodes(''//RingBufferTarget/event'') xed(event_data)
	   INNER JOIN sys.databases d ON cwd.targetdata.value(N''(//RingBufferTarget/event/action[@name="database_id"])[1]'', N''nvarchar(max)'') = d.database_id', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Constant', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=1, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20190821, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'85673e36-4974-4c9f-bfe7-e8a99461b137'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO
*/


-- SELECT * FROM DBA.dbo.XE_Data
