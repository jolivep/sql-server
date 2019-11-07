use master
go

create or alter procedure usp_DBA_active_getRequestsRemotas

as
	
	-----------------------------------------------------------------------------------------------
	--Readme Proc
	-----------------------------------------------------------------------------------------------
	/*
		Desenvolvimento: https://github.com/jolivep
		Objetivo: Recuperar as informações sessões executando comandos remotos
		Versão: 1.0
			1.0 -> 11/04/2018 - Criação da Procedure 
		Referências utilizadas no desenvolvimento:
			https://blogs.msdn.microsoft.com/ialonso/2015/02/06/sys-dm_exec_requests-showing-negative-values-for-total_elapsed_time-wait_time-or-any-other-column-it-exposes-as-an-integer-int-data-type/
			https://www.dirceuresende.com/blog/sql-server-utilizando-a-sp-whoisactive-para-identificar-locks-blocks-queries-lentas-queries-em-execucao-e-muito-mais/

		Uso:
			exec usp_DBA_active_getRequestsRemotas
	*/

	select
		sessoes.[Servidor Local],
		sessoes.[Servidor Remoto],
		sessoes.UOW,
		b.*
	from
		(
			select 
				distinct 
				@@servername as 'Servidor Local',
				a.host_name as 'Servidor Remoto',
				a.session_id as 'SPID',
				c.request_owner_guid as 'UOW'
			from 
				sys.dm_exec_sessions as a 
			inner join 
				sys.dm_exec_requests as b on a.session_id = b.session_id 
			left join 
				sys.dm_tran_locks as c on a.session_id = c.request_session_id and c.request_owner_guid <> '00000000-0000-0000-0000-000000000000'
			where
				a.host_name is not null and
				a.host_name <> left(@@servername, 9)
		) as sessoes
	outer apply
		(
			select 
				case when e.start_time is not null and f.login_time is not null then 
					right('00' + cast(datediff(second, coalesce(e.start_time, f.login_time), getdate()) / 86400 as varchar), 2) + ' ' + 
					right('00' + cast((datediff(second, coalesce(e.start_time, f.login_time), getdate()) / 3600) % 24 as varchar), 2) + ':' + 
					right('00' + cast((datediff(second, coalesce(e.start_time, f.login_time), getdate()) / 60) % 60 as varchar), 2) + ':' + 
					right('00' + cast(datediff(second, coalesce(e.start_time, f.login_time), getdate()) % 60 as varchar), 2) + '.' + 
					right('000' + cast(datediff(second, coalesce(e.start_time, f.login_time), getdate()) as varchar), 3) 
				else null end as 'Duração (Sessão)', --[Ref: Dirceu Resende]
				e.session_id as 'SPID',
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
					left(substring(
						y.text, 
						((e.statement_start_offset/2)+1),   
						( (( (case e.statement_end_offset when -1 then datalength(y.text) else e.statement_end_offset end) - e.statement_start_offset) /2) + 1)
					), 500)
					+ char(10) + ' */ ?>'
				) end as 'Statement',
				case when y.text is null then null else try_convert(xml, 
					'<?Consulta /* ' + char(13) + left(y.text, 500) + char(10) + ' */ ?>'
				) end as 'Consulta',
				e.cpu_time as 'Tempo de CPU',
				e.wait_type as 'Wait Type',
				e.last_wait_type as 'Last Wait Type',
				e.blocking_session_id as 'Blk SPID',
				e.wait_resource as 'Wait Resource',
				e.percent_complete as '% Complete',
				e.reads as 'Reads',
				e.writes as 'Writes',
				e.logical_reads as 'Logical Reads'
			from 
				sys.dm_exec_requests as e
			left join
				sys.dm_exec_sessions as f on e.session_id = f.session_id
			outer apply
				sys.dm_exec_sql_text(e.sql_handle) as y
			outer apply
				(select count(*) as subTasks from sys.dm_os_tasks as tasks where tasks.session_id = e.session_id and tasks.parent_task_address is not null) as z
			where
				e.session_id = sessoes.SPID
		) as b


			