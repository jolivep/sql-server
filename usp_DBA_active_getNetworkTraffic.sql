use master
go


create or alter procedure usp_DBA_active_getNetworkTraffic 
	@sohRequestAtiva as bit = 1
as

	-----------------------------------------------------------------------------------------------
	--Readme Proc
	-----------------------------------------------------------------------------------------------
	/*
		Desenvolvimento: https://github.com/jolivep
		Objetivo: Identificar conexões consumindo mais rede
		Versão: 1.0
			1.0 -> 11/04/2018 - Criação da Procedure 
		Referências utilizadas no desenvolvimento:

		Uso:
			exec usp_DBA_active_getNetworkTraffic
			exec usp_DBA_active_getNetworkTraffic @sohRequestAtiva = 1
	*/

	-----------------------------------------------------------------------------------------------
	--Propriedades de conexão
	-----------------------------------------------------------------------------------------------
	set nocount on
	set xact_abort on


	-----------------------------------------------------------------------------------------------
	--Parametros da procedure
	-----------------------------------------------------------------------------------------------
	/*
		declare @sohRequestAtiva as bit = 0
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
	--Tabelas temporárias que irá armazenar as conexões do momento e os totais em bytes
	-----------------------------------------------------------------------------------------------
	declare @connBytesA as table (
		connection_id uniqueidentifier,
		totalBytes bigint
	)

	declare @connBytesB as table (
		connection_id uniqueidentifier,
		totalBytes bigint
	)
	
	
	-----------------------------------------------------------------------------------------------
	--Recuperamos as conexoes atuais, e o total em bytes, momento 1.
	-----------------------------------------------------------------------------------------------
	insert into @connBytesA
	select 
		a.connection_id,
		a.num_reads+a.num_writes as totalBytes
	from
		sys.dm_exec_connections as a
	
	
	-----------------------------------------------------------------------------------------------
	--Esperamos 1 segundo, para reproduzir o Total KBytes"/Sec"
	-----------------------------------------------------------------------------------------------
	waitfor delay '00:00:00.990'
	
	
	-----------------------------------------------------------------------------------------------
	--Recuperamos as conexoes atuais, e o total em bytes, momento 2.
	-----------------------------------------------------------------------------------------------
	insert into @connBytesB
	select 
		a.connection_id,
		a.num_reads+a.num_writes as totalBytes
	from
		sys.dm_exec_connections as a
	
	
	-----------------------------------------------------------------------------------------------
	--Recupera as informações das conexões
	-----------------------------------------------------------------------------------------------
	select 
		a.session_id as 'SPID',
		case when a.last_read > a.last_write then a.last_read else a.last_write end as 'Last Communication',
		right('00' + cast(datediff(second, coalesce(a.connect_time, getdate()), getdate()) / 86400 as varchar), 2) + ' ' + 
		right('00' + cast((datediff(second, coalesce(a.connect_time, getdate()), getdate()) / 3600) % 24 as varchar), 2) + ':' + 
		right('00' + cast((datediff(second, coalesce(a.connect_time, getdate()), getdate()) / 60) % 60 as varchar), 2) + ':' + 
		right('00' + cast(datediff(second, coalesce(a.connect_time, getdate()), getdate()) % 60 as varchar), 2) + '.' + 
		right('000' + cast(datediff(second, coalesce(a.connect_time, getdate()), getdate()) as varchar), 3) as 'Connection Duration',
		case when c.start_time is not null and b.login_time is not null then 
			right('00' + cast(datediff(second, coalesce(c.start_time, b.login_time), getdate()) / 86400 as varchar), 2) + ' ' + 
			right('00' + cast((datediff(second, coalesce(c.start_time, b.login_time), getdate()) / 3600) % 24 as varchar), 2) + ':' + 
			right('00' + cast((datediff(second, coalesce(c.start_time, b.login_time), getdate()) / 60) % 60 as varchar), 2) + ':' + 
			right('00' + cast(datediff(second, coalesce(c.start_time, b.login_time), getdate()) % 60 as varchar), 2) + '.' + 
			right('000' + cast(datediff(second, coalesce(c.start_time, b.login_time), getdate()) as varchar), 3) 
		else null end as 'Session Duration',
		a.client_net_address + ':' + convert(varchar(10), a.client_tcp_port) as 'RemoteIP',
		convert(money, (a.num_reads+a.num_writes)/1024.) as 'Total KB',
		isnull(e.totalKb1Sec, 0) as 'KB/Sec',
		db_name(b.database_id) as 'Database',
		c.wait_type as 'Wait Type',
		c.last_wait_type as 'Last Wait Type',
		convert(decimal(18,4), (c.wait_time/1000.0)) as 'Wait Duration Sec',
		b.host_name as 'Host',
		b.login_name as 'Login',
		d.dop as 'Dop',
		d.query_cost as 'Query Cost',
		b.program_name as 'Program Name',
		case when x.text is null then null else convert(xml, 
			'<?consulta /* ' + char(13) + left(x.text, 2000) + char(10) + ' */ ?>'
		) end as 'Text',
		y.query_plan as 'Plan',
		b.open_transaction_count as 'Open Tran Count',
		b.is_user_process as 'Is User Process',
		a.local_net_address + ':' + convert(varchar(10), a.local_tcp_port) as 'Local IP',
		convert(money, a.num_reads/1024.) as 'Reads KB',
		convert(money, a.num_writes/1024.) as 'Writes KB',
		a.auth_scheme as 'Auth Scheme'
	from
		sys.dm_exec_connections as a
	left join
		sys.dm_exec_sessions as b on a.session_id = b.session_id
	left join
		sys.dm_exec_requests as c on a.session_id = c.session_id 
			and (c.wait_type not in ('SP_SERVER_DIAGNOSTICS_SLEEP', 'BROKER_RECEIVE_WAITFOR', 'WAITFOR') or c.wait_type is null)
	left join
		sys.dm_exec_query_memory_grants as d on a.session_id = d.session_id
	left join
		(
			select
				a.connection_id,
				convert(money, (b.totalBytes - a.totalBytes)/1024.) as totalKb1Sec
			from
				@connBytesA as a
			inner join
				@connBytesB as b on a.connection_id = b.connection_id
		) as e on a.connection_id = e.connection_id
	outer apply 
		sys.dm_exec_sql_text(c.sql_handle) as x
	outer apply
		sys.dm_exec_query_plan(c.plan_handle) as y
	where
		(@sohRequestAtiva = 0) or
		(@sohRequestAtiva = 1 and c.session_id is not null) 
	order by
		case when c.session_id is null then 1 else 0 end, /*Priorizar requests ativas*/
		case when a.last_read > a.last_write then a.last_read else a.last_write end desc /*Priorizar conexões com tráfego de rede*/,
		e.totalKb1Sec desc /*Priorizar quem trafegou mais no intervalo de tempo coletado*/,
		a.num_reads+a.num_writes desc /*Priorizar mais trafego*/

