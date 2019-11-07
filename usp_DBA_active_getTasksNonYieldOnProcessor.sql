use master
go


create or alter procedure usp_DBA_active_getTasksNonYieldOnProcessor
	@retirarWaitFor as bit = 1
as

	-----------------------------------------------------------------------------------------------
	--Readme Proc
	-----------------------------------------------------------------------------------------------
	/*
		Desenvolvimento: https://github.com/jolivep
		Objetivo: Recuperar as informações das tasks a mais de 7 segundos, essas tasks são candidatas para gerar dump de non yield scheduler
		Versão: 1.0
			1.0 -> 11/04/2018 - Criação da Procedure 
		Referências utilizadas no desenvolvimento:
			https://mssqlwiki.com/tag/non-yielding-scheduler/
		Uso:
			exec usp_DBA_active_getTasksNonYieldOnProcessor
			exec usp_DBA_active_getTasksNonYieldOnProcessor @retirarWaitFor = 0
	*/

	set nocount on
	set xact_abort on


	declare @tassk_a as table (
		scheduler_address varbinary(8),
		scheduler_id int,
		yield_count int,
		active_worker_address varbinary(8),
		task_state nvarchar(60),
		session_id smallint,
		command nvarchar(32),
		stmt xml,
		consulta xml
	)

	declare @tassk_b as table (
		scheduler_address varbinary(8),
		scheduler_id int,
		yield_count int,
		active_worker_address varbinary(8),
		task_state nvarchar(60),
		session_id smallint,
		command nvarchar(32),
		stmt xml,
		consulta xml
	)

	insert into @tassk_a
	select
		a.scheduler_address,
		a.scheduler_id,
		a.yield_count,
		a.active_worker_address,
		b.task_state,
		b.session_id,
		c.command,
		case when y.text is null then null else try_convert(xml, 
			'<?Statement /*' + char(13) + left(
			substring(
				y.text, 
				((c.statement_start_offset/2)+1),   
				( (( (case c.statement_end_offset when -1 then datalength(y.text) else c.statement_end_offset end) - c.statement_start_offset) /2) + 1)
			), 2000) 
			+ char(10) + ' */ ?>'
		) end as 'Statement',
		case when y.text is null then null else try_convert(xml, 
			'<?Consulta /* ' + char(13) + left(y.text, 2000) + char(10) + ' */ ?>'
		) end as 'Consulta'
	from
		sys.dm_os_schedulers as a
	inner join
		sys.dm_os_tasks as b on a.active_worker_address = b.worker_address and b.session_id > 50
	left join
		sys.dm_exec_requests as c on b.session_id = c.session_id
	outer apply
		sys.dm_exec_sql_text(c.sql_handle) as y
	where 
		a.scheduler_id < 1024

	waitfor delay '00:00:07'

	insert into @tassk_b
	select
		a.scheduler_address,
		a.scheduler_id,
		a.yield_count,
		a.active_worker_address,
		b.task_state,
		b.session_id,
		c.command,
		case when y.text is null then null else try_convert(xml, 
			'<?Statement /*' + char(13) + left(
			substring(
				y.text, 
				((c.statement_start_offset/2)+1),   
				( (( (case c.statement_end_offset when -1 then datalength(y.text) else c.statement_end_offset end) - c.statement_start_offset) /2) + 1)
			), 2000) 
			+ char(10) + ' */ ?>'
		) end as 'Statement',
		case when y.text is null then null else try_convert(xml, 
			'<?Consulta /* ' + char(13) + left(y.text, 2000) + char(10) + ' */ ?>'
		) end as 'Consulta'
	from
		sys.dm_os_schedulers as a
	inner join
		sys.dm_os_tasks as b on a.active_worker_address = b.worker_address and b.session_id > 50
	left join
		sys.dm_exec_requests as c on b.session_id = c.session_id
	outer apply
		sys.dm_exec_sql_text(c.sql_handle) as y
	where 
		a.scheduler_id < 1024

	select
		b.scheduler_address  as 'Scheduler Address',
		b.scheduler_id  as 'Scheduler ID',
		b.yield_count  as 'Yield Count',
		b.active_worker_address  as 'Active Worker Address',
		b.task_state  as 'Task State',
		b.session_id  as 'SPID',
		b.command  as 'Command',
		b.stmt  as 'Statement',
		b.consulta as 'Consulta'
	from 
		@tassk_a as a
	inner join
		@tassk_b as b on a.scheduler_id = b.scheduler_id and a.yield_count = b.yield_count
	where
		(@retirarWaitFor = 0) or
		(@retirarWaitFor = 1 and a.command <> 'WAITFOR')

		
	