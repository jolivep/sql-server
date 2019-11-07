use master
go


create or alter procedure usp_DBA_active_getTasksInProcessor
	@top as int = 30,
	@soPesadas as bit = 1,
	@detalhado as bit = 0
as

	-----------------------------------------------------------------------------------------------
	--Readme Proc
	-----------------------------------------------------------------------------------------------
	/*
		Desenvolvimento: https://github.com/jolivep
		Objetivo: Recuperar as informações das consultas que estão consumindo CPU no momento.
		Versão: 1.2
			1.0 -> Criação da Procedure
			1.1 -> Definição dos parametros @soPesadas e @detalhado
			1.2 -> 26/03/2018 - Alteração do filtro de @soPesadas: > 70% de uso paralelo para > 20%, e alteração do tempo de CPU mínima de 8 para 4 Segundos.
		Referências utilizadas no desenvolvimento:
			https://blogs.msdn.microsoft.com/ialonso/2015/02/06/sys-dm_exec_requests-showing-negative-values-for-total_elapsed_time-wait_time-or-any-other-column-it-exposes-as-an-integer-int-data-type;
			https://www.dirceuresende.com/blog/sql-server-utilizando-a-sp-whoisactive-para-identificar-locks-blocks-queries-lentas-queries-em-execucao-e-muito-mais/

		Uso:
			exec usp_DBA_active_getTasksInProcessor @top = 5, @soPesadas = 1, @detalhado = 0
			exec usp_DBA_active_getTasksInProcessor @top = 30, @soPesadas = 0, @detalhado = 1
	*/


	-----------------------------------------------------------------------------------------------
	--Propriedades de conexão
	-----------------------------------------------------------------------------------------------
	set nocount on
	set xact_abort on


	-----------------------------------------------------------------------------------------------
	--Parametros da procedure
	--	@top -> Essa SP consome bastante CPU, o TOP ajuda a trazer resultados mais rápidos
	--	@soPesadas -> Filtro pra gepro, trazer somente quem está moendo CPU, deixando análise mais focada.
	--	@detalhado -> Trazer mais detalhes?
	-----------------------------------------------------------------------------------------------
	/*
		declare @top as int = 30
		declare @soPesadas as bit = 1
		declare @detalhado as bit = 0
	--*/
	

	-----------------------------------------------------------------------------------------------
	--Modo Detalhado Desabilitado - Roda mais rápido porque consome menos CPU
	-----------------------------------------------------------------------------------------------
	if @detalhado = 0
		begin

			-----------------------------------------------------------------------------------------------
			--Recupera informação das tasks em execução. Ordenadas pelo uso de CPU.
			-----------------------------------------------------------------------------------------------
			;with cte_tasks as (
				select 
					top (@top)
					case when e.start_time is not null and f.login_time is not null then 
						right('00' + cast(datediff(second, coalesce(e.start_time, f.login_time), getdate()) / 86400 as varchar), 2) + ' ' + 
						right('00' + cast((datediff(second, coalesce(e.start_time, f.login_time), getdate()) / 3600) % 24 as varchar), 2) + ':' + 
						right('00' + cast((datediff(second, coalesce(e.start_time, f.login_time), getdate()) / 60) % 60 as varchar), 2) + ':' + 
						right('00' + cast(datediff(second, coalesce(e.start_time, f.login_time), getdate()) % 60 as varchar), 2) + '.' + 
						right('000' + cast(datediff(second, coalesce(e.start_time, f.login_time), getdate()) as varchar), 3) 
					else null end as 'Duração (Sessão)', --[Ref: Dirceu Resende]
					a.session_id as 'SPID',
					f.host_name as 'Host',
					db_name(e.database_id) as 'Base',
					f.login_name as 'Login',
					convert(money, 
						convert(float, e.cpu_time)/
						convert(float, 
							case when e.total_elapsed_time = 0 then 1 
							when e.total_elapsed_time < 0 then 2147483647 + (2147483649 + e.total_elapsed_time) --[Ref: Nacho Alonso Portillo]
							else e.total_elapsed_time end
						)
					) as 'Média CPU',
					convert(money, (
							convert(float, e.cpu_time)/
							convert(float, 
								case when e.total_elapsed_time = 0 then 1 
								when e.total_elapsed_time < 0 then 2147483647 + (2147483649 + e.total_elapsed_time) --[Ref: Nacho Alonso Portillo]
								else e.total_elapsed_time end
							)
						) / 
						(case when z.subTasks = 0 then 1 else z.subTasks end)
					) as 'Média CPU Paralela',
					case when y.text is null then null else try_convert(xml, 
						'<?Statement /*' + char(13) + 
						substring(
							y.text, 
							((e.statement_start_offset/2)+1),   
							( (( (case e.statement_end_offset when -1 then datalength(y.text) else e.statement_end_offset end) - e.statement_start_offset) /2) + 1)
						) 
						+ char(10) + ' */ ?>'
					) end as 'Statement',
					case when y.text is null then null else try_convert(xml, 
						'<?Consulta /* ' + char(13) + left(y.text, 2000) + char(10) + ' */ ?>'
					) end as 'Consulta',
					e.cpu_time as 'Tempo de CPU',
					a.context_switches_count as 'Context Switches Count',
					a.task_state as 'Task State',
					e.wait_type as 'Wait Type',
					e.last_wait_type as 'Last Wait Type',
					e.blocking_session_id as 'Blk SPID',
					a.exec_context_id as 'Exec Conext ID',
					a.task_address as 'Task Address',
					a.worker_address as 'Worker Address',
					e.percent_complete as '% Complete',
					z.subTasks as 'Qtd Sub Tasks SPID'
				from 
					sys.dm_os_tasks as a
				inner join
					sys.dm_exec_requests as e on a.session_id = e.session_id
				left join
					sys.dm_exec_sessions as f on a.session_id = f.session_id
				outer apply
					sys.dm_exec_sql_text(e.sql_handle) as y
				outer apply
					(select count(*) as subTasks from sys.dm_os_tasks as tasks where tasks.session_id = a.session_id and tasks.parent_task_address is not null) as z
				where
					a.session_id <> @@spid and						/*Retirar a execução desta.*/
					a.session_id > 50 and							/*Evitar sessões de sistema*/
					a.task_state not in ('DONE') and				/*Tarefas já finalizadas não importa*/
					a.task_state in ('RUNNABLE', 'RUNNING') and		/*Trazer somente tasks rodando ou pronta pra rodar*/
					(
						(@soPesadas = 0) or
						(@soPesadas = 1 and e.cpu_time > 4000)		/*Filtro incial, pra trazer somente quem está moendo CPU (CPU > 4 Segs).*/
					) 
			)
			select
				*
			from
				cte_tasks as a
			where
				(
					(@soPesadas = 0) or
					(@soPesadas = 1 and a.[Média CPU Paralela] >= 0.20) /*Trazer somente quem está moendo CPU (Média Paralela > 20%).*/
				)
			order by
				row_number () over ( order by 
					a.[Média CPU Paralela] desc,
					case a.[Task State] when 'RUNNING' then 1 when 'RUNNABLE' then 2 when 'DONE' then 4 else 3 end, 
					a.[Tempo de CPU] desc,
					a.[Context Switches Count] desc 
				)
			option (recompile)

		end


	-----------------------------------------------------------------------------------------------
	--Modo Detalhado Habilitado - Mais demorado porque consome mais CPU
	-----------------------------------------------------------------------------------------------	
	if @detalhado = 1
		begin

			-----------------------------------------------------------------------------------------------
			--Recupera informação das tasks em execução. Ordenadas pelo uso de CPU.
			-----------------------------------------------------------------------------------------------
			;with cte_tasks as (
				select 
					top (@top)
					case when e.start_time is not null and f.login_time is not null then 
						right('00' + cast(datediff(second, coalesce(e.start_time, f.login_time), getdate()) / 86400 as varchar), 2) + ' ' + 
						right('00' + cast((datediff(second, coalesce(e.start_time, f.login_time), getdate()) / 3600) % 24 as varchar), 2) + ':' + 
						right('00' + cast((datediff(second, coalesce(e.start_time, f.login_time), getdate()) / 60) % 60 as varchar), 2) + ':' + 
						right('00' + cast(datediff(second, coalesce(e.start_time, f.login_time), getdate()) % 60 as varchar), 2) + '.' + 
						right('000' + cast(datediff(second, coalesce(e.start_time, f.login_time), getdate()) as varchar), 3) 
					else null end as 'Duração (Sessão)', --[Ref: Dirceu Resende]
					a.session_id as 'SPID',
					f.host_name as 'Host',
					db_name(e.database_id) as 'Base',
					f.login_name as 'Login',
					convert(money, 
						convert(float, e.cpu_time)/
						convert(float, 
							case when e.total_elapsed_time = 0 then 1 
							when e.total_elapsed_time < 0 then 2147483647 + (2147483649 + e.total_elapsed_time) --[Ref: Nacho Alonso Portillo]
							else e.total_elapsed_time end
						)
					) as 'Média CPU',
					convert(money, (
							convert(float, e.cpu_time)/
							convert(float, 
								case when e.total_elapsed_time = 0 then 1 
								when e.total_elapsed_time < 0 then 2147483647 + (2147483649 + e.total_elapsed_time) --[Ref: Nacho Alonso Portillo]
								else e.total_elapsed_time end
							)
						) / 
						(case when z.subTasks = 0 then 1 else z.subTasks end)
					) as 'Média CPU Paralela',
					case when y.text is null then null else try_convert(xml, 
						'<?Statement /*' + char(13) + 
						substring(
							y.text, 
							((e.statement_start_offset/2)+1),   
							( (( (case e.statement_end_offset when -1 then datalength(y.text) else e.statement_end_offset end) - e.statement_start_offset) /2) + 1)
						) 
						+ char(10) + ' */ ?>'
					) end as 'Statement',
					case when y.text is null then null else try_convert(xml, 
						'<?consulta /* ' + char(13) + left(y.text, 2000) + char(10) + ' */ ?>'
					) end as 'Consulta',
					x.query_plan as 'Plano',
					e.cpu_time as 'Tempo de CPU',
					a.context_switches_count as 'Context Switches Count',
					a.task_state as 'Task State',
					b.state as 'Worker State',
					case b.return_code 
						when 0 then 'SUCCESS' 
						when 3 then 'DEADLOCK' 
						when 4 then 'PREMATURE_WAKEUP' 
						when 258 then 'TIMEOUT' 
						else 'UNKNOWN'
					end as 'Worker Return Code Desc',
					b.last_wait_type as 'Worker Last Wait Type',
					e.blocking_session_id as 'Blk SPID',
					a.exec_context_id as 'Exec Conext ID',
					d.status as 'Scheduler Status',
					d.scheduler_id as 'Scheduler ID',
					d.cpu_id as 'CPU ID', 
					d.load_factor as 'Load Factor',
					d.current_workers_count as 'Current Workers Count',
					d.current_tasks_count as 'Current Tasks Count',
					c.affinity as 'Thread Affinity',
					c.affinity as 'Worker Affinity',
					a.task_address as 'Task Address',
					a.worker_address as 'Worker Address',
					d.scheduler_address as 'Scheduler Address',
					c.started_by_sqlservr as 'Started by SQL',
					c.os_thread_id as 'OS Thread ID',
					e.percent_complete as '% Complete',
					z.subTasks as 'Qtd Sub Tasks SPID'
				from 
					sys.dm_os_tasks as a
				left join
					sys.dm_os_workers as b on a.worker_address = b.worker_address
				left join
					sys.dm_os_threads as c on b.worker_address = c.worker_address
				left join
					sys.dm_os_schedulers as d on c.scheduler_address = d.scheduler_address
				inner join
					sys.dm_exec_requests as e on a.session_id = e.session_id
				left join
					sys.dm_exec_sessions as f on a.session_id = f.session_id
				outer apply
					sys.dm_exec_query_plan(e.plan_handle) as x
				outer apply
					sys.dm_exec_sql_text(e.sql_handle) as y
				outer apply
					(select count(*) as subTasks from sys.dm_os_tasks as tasks where tasks.session_id = a.session_id and tasks.parent_task_address is not null) as z
				where
					a.session_id <> @@spid and						/*Retirar a execução desta.*/
					a.session_id > 50 and							/*Evitar sessões de sistema*/
					a.task_state not in ('DONE') and				/*Tarefas já finalizadas não importa*/
					a.task_state in ('RUNNABLE', 'RUNNING') and		/*Trazer somente tasks rodando ou pronta pra rodar*/
					(
						(@soPesadas = 0) or
						(@soPesadas = 1 and e.cpu_time > 4000)		/*Filtro incial, pra trazer somente quem está moendo CPU (CPU > 4 Segs).*/
					) 
			)
			select
				*
			from
				cte_tasks as a
			where
				(
					(@soPesadas = 0) or
					(@soPesadas = 1 and a.[Média CPU Paralela] >= 0.20) /*Trazer somente quem está moendo CPU (Média Paralela > 20%).*/
				)
			order by
				row_number () over ( order by 
					case when a.[Worker Last Wait Type] like 'DBMIRROR%' then 1 else 0 end, 
					case when a.[Worker Last Wait Type] like 'WAITFOR%' then 1 else 0 end, 
					a.[Média CPU Paralela] desc,
					case a.[Task State] when 'RUNNING' then 1 when 'RUNNABLE' then 2 when 'DONE' then 4 else 3 end, 
					a.[Tempo de CPU] desc,
					a.[Context Switches Count] desc 
				)
			option (recompile)

	end