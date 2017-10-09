/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
Make sure we are not locking a database
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */
use master
go

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
Cleanup
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */
/*

use master
go

drop database star_play
drop database src_sim1

*/

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
Setup DB
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */
declare @timer nvarchar(max)
select @timer = 'log : ' + convert(nvarchar, current_timestamp, 126)
print @timer

declare @scratch_database_name sysname = 'star_play'

if (select count(*) from sys.databases where name = @scratch_database_name) = 0
begin
	begin try
		exec ('create database star_play')
	end try
	begin catch 
		print ('create database star_play - FAILED !!')
	end catch
end 
go

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
create table for  descriptive metadata of the source for our star
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */
use star_play
go

-- declare @cur_db sysname = db_name()
if (select count(*) from information_schema.tables where table_schema = 'dbo' and table_name = 'meta_generator') = 1
begin
	drop table star_play.dbo.meta_generator
end
go

create table star_play.dbo.meta_generator
(meta_generator_id bigint identity not null primary key,
database_name sysname not null,
owner_name sysname not null,
table_name sysname not null,
column_name sysname not null,
column_data_type sysname not null,
min1 numeric null,
max1 numeric null,
min2 numeric null,
max2 numeric null,
min3 numeric null,
max3 numeric null)


-- truncate table meta_generator
set nocount on
insert into star_play.dbo.meta_generator(database_name, owner_name, table_name, column_name, column_data_type, min1, max1, min2, max2, min3, max3) select 'src_sim1', 'xxx', 'customer', 'customer_id', 'int identity not null', null, null, null, null, null, null
insert into star_play.dbo.meta_generator(database_name, owner_name, table_name, column_name, column_data_type, min1, max1, min2, max2, min3, max3) select 'src_sim1', 'xxx', 'customer', 'customer_name', 'nvarchar(100)', 10, 30, unicode('A'), unicode('Z'), null, null
insert into star_play.dbo.meta_generator(database_name, owner_name, table_name, column_name, column_data_type, min1, max1, min2, max2, min3, max3) select 'src_sim1', 'xxx', 'customer', 'country_id', 'int', 1, 100, null, null, null, null
insert into star_play.dbo.meta_generator(database_name, owner_name, table_name, column_name, column_data_type, min1, max1, min2, max2, min3, max3) select 'src_sim1', 'xxx', 'customer', 'region_id', 'int', 10, 10000, null, null, null, null

insert into star_play.dbo.meta_generator(database_name, owner_name, table_name, column_name, column_data_type, min1, max1, min2, max2, min3, max3) select 'src_sim1', 'yyy', 'product', 'product_id', 'int identity not null', null, null, null, null, null, null
insert into star_play.dbo.meta_generator(database_name, owner_name, table_name, column_name, column_data_type, min1, max1, min2, max2, min3, max3) select 'src_sim1', 'yyy', 'product', 'product_name', 'nvarchar(100)', 20, 30, unicode('A'), unicode('Z'), null, null
insert into star_play.dbo.meta_generator(database_name, owner_name, table_name, column_name, column_data_type, min1, max1, min2, max2, min3, max3) select 'src_sim1', 'yyy', 'product', 'price', 'int', 100, 20000, null, null, null, null
insert into star_play.dbo.meta_generator(database_name, owner_name, table_name, column_name, column_data_type, min1, max1, min2, max2, min3, max3) select 'src_sim1', 'yyy', 'product', 'product_classification_id', 'int', 1, 1000, null, null, null, null


/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
make sure database mentioned exists
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */
declare @database_name sysname
declare @sql nvarchar(max)

declare cur1 cursor for
	select 
		distinct database_name
	from star_play.dbo.meta_generator as t1

open cur1
fetch next from cur1 into @database_name
while (@@fetch_status = 0)
begin
	if (select count(*) from sys.databases where name = @database_name) = 0
	begin
		begin try
			set @sql = 'create database ' + quotename(@database_name)
			exec (@sql)
			print (@sql)
		end try
		begin catch 
			print (@sql + ' - FAILED !!')
		end catch
	end 
	fetch next from cur1 into @database_name
end

close cur1
deallocate cur1
go

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
make sure schemas mentioned exists
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */
declare @database_name sysname
declare @q_database_name sysname
declare @schema_name sysname
declare @sp_executesql sysname
declare @parm nvarchar(max)
declare @sql nvarchar(max)
declare @tst bit

declare cur1 cursor for
	select distinct 
		database_name,
		owner_name
	from star_play.dbo.meta_generator as t1

open cur1
fetch next from cur1 into @database_name, @schema_name
while (@@fetch_status = 0)
begin
	set @tst = 0
	set @q_database_name = quotename(@database_name)
	set @sp_executesql = @q_database_name + '..sp_executesql'
	set @sql = 'select @in_tst = count(*) from sys.schemas where name = @in_schemaname'  
	set @parm = '@in_schemaname sysname, @in_tst bit output'
	exec @sp_executesql @sql, @parm, @in_schemaname = @schema_name, @in_tst = @tst output  -- injection possibility ??

	-- create schemas that don't exist
	if (@tst = 0) 
	begin
		set @sql = 'create schema ' + quotename(@schema_name)
		print @sql 
		exec @sp_executesql @sql
	end
	fetch next from cur1 into @database_name, @schema_name
end

close cur1
deallocate cur1
go

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
make sure tables mentioned exists
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */
-- temp table with indicators for "min" and "max"
if (object_id('tempdb..#meta_generator') is not null)
	drop table #meta_generator
go

select
	*,
	row_number() over(partition by database_name, owner_name, table_name order by meta_generator_id) as column_id, -- 1 = first column 
	row_number() over(partition by database_name, owner_name, table_name order by meta_generator_id desc) as rev_column_id, -- 1 = last column,
	rank() over(order by database_name, owner_name, table_name) as table_id
into #meta_generator
from meta_generator

-- find all existing tables int the databases used
if (object_id('tempdb..#existing_tabs') is not null)
	drop table #existing_tabs
go

create table #existing_tabs
(table_catalog sysname, 
table_schema sysname, 
table_name sysname)


declare @database_name sysname
declare @sp_executesql sysname
declare @sql nvarchar(max)

declare cur1 cursor for
	select distinct
		database_name
	from #meta_generator
open cur1
fetch next from cur1 into @database_name
while (@@fetch_status = 0)
begin

	set @sp_executesql = quotename(@database_name) + '..sp_executesql'

	set @sql = 'select table_catalog, table_schema, table_name from information_schema.tables where table_type = ''BASE TABLE'''
	-- print @sql 
	insert into #existing_tabs(table_catalog, table_schema, table_name)
	exec @sp_executesql @sql
	fetch next from cur1 into @database_name
end

close cur1
deallocate cur1
go

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
create any table that outright does not exist (GL with handling alter tables - todo :-)
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

declare @database_name sysname 
declare @schema_name sysname 
declare @table_name sysname
declare @sql nvarchar(max)

declare cur1 cursor for
	select 
		database_name, 
		owner_name, 
		table_name 
	from #meta_generator
	except -- N.B.: implicit distinct
	select 
		table_catalog, 
		table_schema, 
		table_name 
	from #existing_tabs

open cur1
fetch next from cur1 into @database_name, @schema_name, @table_name
while (@@fetch_status = 0)
begin
	set @sql = ''

	select 
		@sql = 
			@sql +
			-- create table part if first column
			case
				when column_id = 1 then 'create table ' + quotename(database_name) + '.' +  quotename(owner_name) + '.' + quotename(table_name) + nchar(10) + '(' + nchar(10) + nchar(9)
				else nchar(9)
			end + 
			-- column stuff
			column_name + ' ' +
			column_data_type +
			-- komma if its not last and end parenthisis when it is last
			case when rev_column_id = 1 then nchar(10) + ')' + nchar(10) else ',' end +
			-- done
			nchar(10)
	from #meta_generator
	where database_name = @database_name
	and owner_name = @schema_name
	and table_name = @table_name
	order by column_id

	exec (@sql)
	print @sql
	fetch next from cur1 into @database_name, @schema_name, @table_name
end

close cur1
deallocate cur1
go


-- ADHOC verification:
/*

declare @sql nvarchar(max)
set @sql = 'select * from information_schema.tables'
exec src_sim1..sp_executesql @sql

*/



/* ------------------------------------------------------------------------------------------------------- */
/*                                                                                                         */
/* EVERY TABLE SHOULD NOW EXISTS !!!!!!!!!!!!!!!                                                           */
/*                                                                                                         */
/* ------------------------------------------------------------------------------------------------------- */

/*
select 
	'select * from ' + database_name + '.' + owner_name + '.' + table_name 
from 
	(select distinct database_name, owner_name, table_name from star_play.dbo.meta_generator) as x
*/





select * from src_sim1.xxx.customer
select * from src_sim1.yyy.product


-- ABS(CHECKSUM(NewId()))











