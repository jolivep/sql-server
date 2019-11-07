use master
go


create or alter procedure usp_DBA_active_getWaitingTasks
	@getCommand_Plan as bit = 1 --Default = 1
	,@sohDTC as bit = 0			--Default = 0
	,@comUOW as bit = 0			--Default = 0
	,@comWaitfor as bit = 0     --Default = 0
	,@comMsqlxp as bit = 0      --Default = 0
as

	-----------------------------------------------------------------------------------------------
	--Readme Proc
	-----------------------------------------------------------------------------------------------
	/*
		Desenvolvimento: https://github.com/jolivep
		Objetivo: Recuperar as informações das tasks em espera no servidor.
		Versão: 1.0
			1.0 -> 11/04/2018 - Criação da Procedure 
		Referências utilizadas no desenvolvimento:

		Uso:
			exec usp_DBA_active_getWaitingTasks
			exec usp_DBA_active_getWaitingTasks @getCommand_Plan = 1, @sohDTC = 0, @comUOW = 1, @comWaitfor = 1, @comMsqlxp = 1
			exec usp_DBA_active_getWaitingTasks @getCommand_Plan = 1, @sohDTC = 1, @comUOW = 1, @comWaitfor = 1, @comMsqlxp = 1
	*/


	-----------------------------------------------------------------------------------------------
	--Propriedades da conexão
	-----------------------------------------------------------------------------------------------
	set nocount on
	set xact_abort on 


	-----------------------------------------------------------------------------------------------
	--Parametros 
	-----------------------------------------------------------------------------------------------
	/*
		declare @getCommand_Plan as bit = 1 --Default = 1
		declare @sohDTC as bit = 0			--Default = 0
		declare @comWaitfor as bit = 0      --Default = 0
		declare @comMsqlxp as bit = 0       --Default = 0
	--*/
	

	-----------------------------------------------------------------------------------------------
	--Só sysadmin's
	-----------------------------------------------------------------------------------------------
	if not isnull(is_srvrolemember('sysadmin'), 0) = 1 
		begin 
			raiserror('Você não tem permissão para executar procedure!', 16, 0)
			return
		end

	
	-----------------------------------------------------------------------------------------------
	--Waiting Tasks sem o Text e Plan, somente seus handle. Assim, é mais rápido para servidores lentos.
	-----------------------------------------------------------------------------------------------
	declare @uows as table (spid int, uow uniqueidentifier)

	if isnull(@comUOW, 0) = 1
		begin
			insert into @uows
			select 
				distinct 
				locks.request_session_id,
				locks.request_owner_guid
			from 
				sys.dm_tran_locks as locks 
			where 
				locks.request_owner_guid is not null and
				locks.request_owner_guid <> '00000000-0000-0000-0000-000000000000'
		end

	
	-----------------------------------------------------------------------------------------------
	--Waiting Tasks sem o Text e Plan, somente seus handle. Assim, é mais rápido para servidores lentos.
	-----------------------------------------------------------------------------------------------
	if (@getCommand_Plan = 0) 
		begin
			select
				case when c.start_time is not null and e.login_time is not null then 
					right('00' + cast(datediff(second, coalesce(c.start_time, e.login_time), getdate()) / 86400 as varchar), 2) + ' ' + 
					right('00' + cast((datediff(second, coalesce(c.start_time, e.login_time), getdate()) / 3600) % 24 as varchar), 2) + ':' + 
					right('00' + cast((datediff(second, coalesce(c.start_time, e.login_time), getdate()) / 60) % 60 as varchar), 2) + ':' + 
					right('00' + cast(datediff(second, coalesce(c.start_time, e.login_time), getdate()) % 60 as varchar), 2) + '.' + 
					right('000' + cast(datediff(second, coalesce(c.start_time, e.login_time), getdate()) as varchar), 3) 
				else null end as 'Session Duration',
				a.session_id as 'SPID',
				f.uow as 'UOW',
				a.wait_type as 'Wait Type',
				convert(decimal(18,4), (a.wait_duration_ms/1000.0)) as 'Wait Duration Sec',
				db_name(c.database_id) as 'Database',
				e.host_name as 'Host',
				b.scheduler_id as 'Scheduler ID',
				d.dop as 'Dop',
				d.query_cost as 'Query Cost',
				a.exec_context_id as 'Exec Context ID',
				a.blocking_session_id as 'Blck SPID',
				a.blocking_exec_context_id as 'Blck Exec Conext ID',
				e.login_name as 'Login',
				e.program_name as 'Program Name',
				'select * from sys.dm_exec_sql_text(0x' + convert(varchar(128), c.sql_handle, 2) + ')'  as 'Text',
				'select * from sys.dm_exec_query_plan(0x' + convert(varchar(128), c.plan_handle, 2) + ')' as 'Plan',
				a.waiting_task_address as 'Waiting Task Address',
				a.blocking_task_address as 'Blocking Task Address',
				a.resource_address as 'Resource Address',
				a.resource_description as 'Resource Desc',
				e.open_transaction_count as 'Open Tran Count',
				e.is_user_process as 'Is User Process'
			from
				sys.dm_os_waiting_tasks as a
			inner join
				sys.dm_os_tasks as b on a.waiting_task_address = b.task_address 
			inner join
				sys.dm_exec_requests as c on a.session_id = c.session_id
			left join
				sys.dm_exec_query_memory_grants as d on a.session_id = d.session_id
			left join
				sys.dm_exec_sessions as e on a.session_id = e.session_id
			left join
				@uows as f on a.session_id = f.spid
			where
				e.is_user_process = 1
				and a.wait_type <> 'SP_SERVER_DIAGNOSTICS_SLEEP'
				and a.wait_type <> 'BROKER_RECEIVE_WAITFOR'
				and ( @comWaitfor = 1 or (@comWaitfor = 0 and a.wait_type <> 'WAITFOR') )
				and ( @comMsqlxp  = 1 or (@comMsqlxp  = 0 and a.wait_type <> 'MSQL_XP') )
				and ( @sohDTC     = 0 or (@sohDTC	  = 1 and a.wait_type in (
							'PREEMPTIVE_COM_QUERYINTERFACE','PREEMPTIVE_TRANSIMPORT','DTC','DTC_STATE','DTCPNTSYNC','DTC_WAITFOR_OUTCOME',
							'DTC_RESOLVE','DTC_ABORT_REQUEST','DTC_TMDOWN_REQUEST','PREEMPTIVE_OS_DTCOPS','PREEMPTIVE_DTC_ABORT',
							'PREEMPTIVE_DTC_ABORTREQUESTDONE','PREEMPTIVE_DTC_BEGINTRANSACTION','PREEMPTIVE_DTC_COMMITREQUESTDONE',
							'PREEMPTIVE_DTC_ENLIST','PREEMPTIVE_DTC_PREPAREREQUESTDONE'
						)) )
			order by
				a.wait_duration_ms desc,
				a.session_id asc,
				a.exec_context_id asc
			option (recompile)
		end

	
	-----------------------------------------------------------------------------------------------
	--Waiting Tasks com o Text e Plan.
	-----------------------------------------------------------------------------------------------
	if (@getCommand_Plan = 1) 
		begin
			select 
				case when c.start_time is not null and e.login_time is not null then 
					right('00' + cast(datediff(second, coalesce(c.start_time, e.login_time), getdate()) / 86400 as varchar), 2) + ' ' + 
					right('00' + cast((datediff(second, coalesce(c.start_time, e.login_time), getdate()) / 3600) % 24 as varchar), 2) + ':' + 
					right('00' + cast((datediff(second, coalesce(c.start_time, e.login_time), getdate()) / 60) % 60 as varchar), 2) + ':' + 
					right('00' + cast(datediff(second, coalesce(c.start_time, e.login_time), getdate()) % 60 as varchar), 2) + '.' + 
					right('000' + cast(datediff(second, coalesce(c.start_time, e.login_time), getdate()) as varchar), 3) 
				else null end as 'Session Duration',
				a.session_id as 'SPID',
				f.uow as 'UOW',
				a.wait_type as 'Wait Type',
				convert(decimal(18,4), (a.wait_duration_ms/1000.0)) as 'Wait Duration Sec',
				db_name(c.database_id) as 'Database',
				e.host_name as 'Host',
				b.scheduler_id as 'Scheduler ID',
				d.dop as 'Dop',
				d.query_cost as 'Query Cost',
				a.exec_context_id as 'Exec Context ID',
				a.blocking_session_id as 'Blck SPID',
				a.blocking_exec_context_id as 'Blck Exec Conext ID',
				e.login_name as 'Login',
				e.program_name as 'Program Name',
				case when x.text is null then null else convert(xml, 
					'<?consulta /* ' + char(13) + left(x.text, 2000) + char(10) + ' */ ?>'
				) end as 'Text',
				y.query_plan as 'Plan',
				a.waiting_task_address as 'Waiting Task Address',
				a.blocking_task_address as 'Blocking Task Address',
				a.resource_address as 'Resource Address',
				a.resource_description as 'Resource Desc',
				e.open_transaction_count as 'Open Tran Count',
				e.is_user_process as 'Is User Process'
			from
				sys.dm_os_waiting_tasks as a
			inner join
				sys.dm_os_tasks as b on a.waiting_task_address = b.task_address 
			inner join
				sys.dm_exec_requests as c on a.session_id = c.session_id
			left join
				sys.dm_exec_query_memory_grants as d on a.session_id = d.session_id
			left join
				sys.dm_exec_sessions as e on a.session_id = e.session_id
			outer apply 
				sys.dm_exec_sql_text(c.sql_handle) as x
			outer apply
				sys.dm_exec_query_plan(c.plan_handle) as y
			left join
				@uows as f on a.session_id = f.spid
			where
				e.is_user_process = 1
				and a.wait_type <> 'SP_SERVER_DIAGNOSTICS_SLEEP'
				and a.wait_type <> 'BROKER_RECEIVE_WAITFOR'
				and ( @comWaitfor = 1 or (@comWaitfor = 0 and a.wait_type <> 'WAITFOR') )
				and ( @comMsqlxp  = 1 or (@comMsqlxp  = 0 and a.wait_type <> 'MSQL_XP') )
				and ( @sohDTC     = 0 or (@sohDTC	  = 1 and a.wait_type in (
							'PREEMPTIVE_COM_QUERYINTERFACE','PREEMPTIVE_TRANSIMPORT','DTC','DTC_STATE','DTCPNTSYNC','DTC_WAITFOR_OUTCOME',
							'DTC_RESOLVE','DTC_ABORT_REQUEST','DTC_TMDOWN_REQUEST','PREEMPTIVE_OS_DTCOPS','PREEMPTIVE_DTC_ABORT',
							'PREEMPTIVE_DTC_ABORTREQUESTDONE','PREEMPTIVE_DTC_BEGINTRANSACTION','PREEMPTIVE_DTC_COMMITREQUESTDONE',
							'PREEMPTIVE_DTC_ENLIST','PREEMPTIVE_DTC_PREPAREREQUESTDONE'
						)) )
			order by
				a.wait_duration_ms desc,
				a.session_id asc,
				a.exec_context_id asc
			option (recompile)
		end