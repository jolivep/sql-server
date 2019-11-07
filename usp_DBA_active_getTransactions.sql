use master
go


create or alter procedure dbo.usp_DBA_active_getTransactions
	@onlyWithSessions as bit = 0
as

	-----------------------------------------------------------------------------------------------
	--Readme Proc
	-----------------------------------------------------------------------------------------------
	/*
		Desenvolvimento: https://github.com/jolivep
		Objetivo: Recuperar as informações das transações e seus estados rodando no servidor.
		Versão: 1.0
			1.0 -> 11/04/2018 - Criação da Procedure 
		Referências utilizadas no desenvolvimento:
			https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-tran-active-transactions-transact-sql
			https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-tran-database-transactions-transact-sql
			https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-task-space-usage-transact-sql
			https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-session-space-usage-transact-sql

		Uso:
			exec usp_DBA_active_getTransactions
			exec usp_DBA_active_getTransactions @onlyWithSessions = 1
	*/
	

	-----------------------------------------------------------------------------------------------
	--Definição das configuração da conexão
	-----------------------------------------------------------------------------------------------
	set nocount on
	set xact_abort on
	set transaction isolation level read uncommitted 
	

	-----------------------------------------------------------------------------------------------
	--Definição das configuração da conexão
	-----------------------------------------------------------------------------------------------
	/*
		declare @onlyWithSessions as bit = 0
	--*/


	-----------------------------------------------------------------------------------------------
	--Definição das CTEs
	-----------------------------------------------------------------------------------------------
	;with ActiveTransactions as (
		-----------------------------------------------------------------------------------------------
		--Recupera informação das Transações Ativas
		-----------------------------------------------------------------------------------------------
		select 
			a.transaction_id as xId,
			a.name as xName,
			a.transaction_begin_time as xBeginTime,
			case a.transaction_type 
				when 1 then '1 = Read/Write Tran'
				when 2 then '2 = Read-Only Tran'
				when 3 then '3 = System Tran'
				when 4 then '4 = Distributed Tran'
				else 'unknown'
			end as xType,
			a.transaction_uow as xUOW,
			case a.transaction_state
				when 0 then '0 = Initializing'
				when 1 then '1 = Initialized (Not Started)'
				when 2 then '2 = Active'
				when 3 then '3 = Ended (Read-Only)'
				when 4 then '4 = Commiting (Distributed)'
				when 5 then '5 = Prepared (Waiting Resolution)'
				when 6 then '6 = Committed'
				when 7 then '7 = Rolling Back'
				when 8 then '8 = Rolled Back'
				else 'unknown'
			end as xState,
			case a.dtc_state
				when 1 then '1 = Active'
				when 2 then '2 = Prepared'
				when 3 then '3 = Committed'
				when 4 then '4 = Aborted'
				when 5 then '5 = Recovered'
			end as dtcState
		from 
			sys.dm_tran_active_transactions as a 
	), DatabaseTransactions as (
		-----------------------------------------------------------------------------------------------
		--Recupera informações das transações ativas nas bases
		-----------------------------------------------------------------------------------------------
		select 
			a.transaction_id as xId,
			case when a.database_id = 32767 then 'MSSQL Resource' else db_name(a.database_id) end as base,
			a.database_transaction_begin_time as xBeginTimeDB,
			case a.database_transaction_type 
				when 1 then '1 = Read/Write Tran'
				when 2 then '2 = Read-Only Tran'
				when 3 then '3 = System Tran'
			end as xTypeDB,
			case a.database_transaction_state
				when 1 then '1 = Not Initialized'
				when 3 then '3 = Initialized (No Log Records)'
				when 4 then '4 = Generated Log Records'
				when 5 then '5 = Prepared'
				when 10 then '10 = Committed'
				when 11 then '11 = Rolled Back'
				when 12 then '12 = Commiting (Log Record generated but has not been materialized or persisted)'
			end as xStateDB,
			a.database_transaction_log_record_count as 'logRecordCountDB',
			a.database_transaction_replicate_record_count as 'logRecordReplicateCountDB',
			convert(money, ((a.database_transaction_log_bytes_used/1024.)/1024.)) as 'logUsedMB',
			convert(money, ((a.database_transaction_log_bytes_reserved/1024.)/1024.)) as 'logReservedMB',
			convert(money, ((a.database_transaction_log_bytes_used_system/1024.)/1024.)) as 'logUsedSystemMB',
			convert(money, ((a.database_transaction_log_bytes_reserved_system/1024.)/1024.)) as 'logReservedSystemMB'
		from 
			sys.dm_tran_database_transactions as a
	)

	-----------------------------------------------------------------------------------------------
	--Retorna informação das transações
	-----------------------------------------------------------------------------------------------
	select 
		case when a.xBeginTime is not null then 
			right('00' + cast(datediff(second, a.xBeginTime, getdate()) / 86400 as varchar), 2) + ' ' + 
			right('00' + cast((datediff(second, a.xBeginTime, getdate()) / 3600) % 24 as varchar), 2) + ':' + 
			right('00' + cast((datediff(second, a.xBeginTime, getdate()) / 60) % 60 as varchar), 2) + ':' + 
			right('00' + cast(datediff(second, a.xBeginTime, getdate()) % 60 as varchar), 2) + '.' + 
			right('000' + cast(datediff(second, a.xBeginTime, getdate()) as varchar), 3) 
		else null end as 'Duração (Tran)', 
		c.session_id as 'SPID',
		a.xBeginTime as 'Begin Time (X)',
		b.base as 'Base',
		f.status as 'Session Status',
		a.xState as 'State (X)',
		a.dtcState as 'DTC State',
		e.alocadoMbTempDb as 'TempDB Alloc (MB)',
		(b.logUsedMB + b.logReservedMB + b.logUsedSystemMB + b.logReservedSystemMB) as 'Log Used+Reserved (MB)',
		f.host_name as 'Host',
		f.program_name as 'Programa',
		f.login_name as 'Login',
		d.cmd as 'Wait Type',
		d.lastwaittype as 'Last Wait Type',
		case when y.text is null or d.spid is null then null else try_convert(xml, 
			'<?Statement /*' + char(13) + 
			left(
				substring(
					y.text, 
					((d.stmt_start/2)+1),   
					( (( (case d.stmt_end when -1 then datalength(y.text) else d.stmt_end end) - d.stmt_start) /2) + 1)
				)
			, 600)
			+ char(10) + ' */ ?>'
		) end as 'Statement',
		case when y.text is null then null else try_convert(xml, 
			'<?Consulta /* ' + char(13) + left(y.text, 600) + char(10) + ' */ ?>'
		) end as 'Consulta',
		a.xId as 'ID (X)',
		a.xName as 'Name (X)',
		a.xType as 'Type (X)',
		a.xUOW as 'UOW (X)',
		b.xTypeDB as 'Type DB (X)',
		b.xStateDB as 'State DB (X)',
		b.logRecordCountDB as 'Log Record Count',
		b.logRecordReplicateCountDB as 'Log Record REPL Count',
		b.logUsedMB as 'Log Used MB',
		b.logReservedMB as 'Log Reserved MB',
		b.logUsedSystemMB as 'Log Used System MB',
		b.logReservedSystemMB as 'Log Reserved System MB'
	from 
		ActiveTransactions as a
	left join
		DatabaseTransactions as b on a.xId = b.xId
	left join
		sys.dm_tran_session_transactions as c on a.xId = c.transaction_id
	left join 
		sys.sysprocesses as d on c.session_id = d.spid
	left join
		(
			select
				a.spid,
				sum(convert(money, a.alocadoMB)) as alocadoMbTempDb
			from
				(
					select
						a.session_id as spid,
						sum(
							(user_objects_alloc_page_count + internal_objects_alloc_page_count) - 
							(user_objects_dealloc_page_count + internal_objects_dealloc_page_count)
						)/128. as alocadoMB
					from 
						sys.dm_db_session_space_usage as a 
					group by
						a.session_id
					union all
					select 
						a.session_id as spid,
						sum(
							(user_objects_alloc_page_count + internal_objects_alloc_page_count) - 
							(user_objects_dealloc_page_count + internal_objects_dealloc_page_count)
						)/128. as alocadoMB
					from 
						sys.dm_db_task_space_usage as a
					group by
						a.session_id
				) as a
			group by
				a.spid		
		) as e on c.session_id = e.spid
	left join
		sys.dm_exec_sessions as f on c.session_id = f.session_id
	outer apply
		sys.dm_exec_sql_text(d.sql_handle) as y
	where
		((@onlyWithSessions = 0) or (@onlyWithSessions = 1 and c.session_id is not null))
	order by 
		(case when c.session_id is null then 2 else 1 end),
		a.xBeginTime
	option (recompile)