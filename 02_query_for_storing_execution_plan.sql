USE AdventureWorks;
GO

/*

 - Storing Execution plan in table
 - running R dataframes and stats using sp_execute_External_Script
 
Author: Tomaz Kastrun
Date: 1.1.2019
Blog: tomaztsql.wordpress.com


*/


DECLARE @queryplan TABLE ([ShowPlanXML] XML);

-- bulk insert XML document into my table variable
INSERT INTO @queryplan([ShowPlanXML])

SELECT CONVERT(XML, BulkColumn) AS QueryPlan
FROM OPENROWSET
	--(BULK 'C:\DataTK\git\SQL_Server_Query_Execution_plans_with_R\plan1.sqlplan', SINGLE_BLOB) as x;
	(BULK 'C:\DataTK\git\SQL_Server_Query_Execution_plans_with_R\plan2_actual.sqlplan', SINGLE_BLOB) as x;



;WITH XMLNAMESPACES(DEFAULT N'http://schemas.microsoft.com/sqlserver/2004/07/showplan')

SELECT DISTINCT

  RO.p.value(N'@AvgRowSize',N'int') AS AvgRowSize 
 ,RO.p.value(N'@EstimateCPU', N'FLOAT') AS EstimateCPU
 ,RO.p.value(N'@EstimateIO',N'FLOAT') AS EstimateIO
 ,RO.p.value(N'@EstimateRebinds', N'FLOAT') AS EstimateRebinds
 ,RO.p.value(N'@EstimateRewinds',N'FLOAT') AS EstimateRewinds
 ,RO.p.value(N'@EstimatedExecutionMode', N'VARCHAR(100)') AS EstimatedExecutionMode
 ,RO.p.value(N'@GroupExecuted', N'VARCHAR(50)') AS GroupExecuted
 ,RO.p.value(N'@EstimateRows', N'FLOAT') AS EstimateRows
 ,RO.p.value(N'@EstimatedRowsRead', N'FLOAT') AS EstimateRowsRead
 ,RO.p.value(N'@LogicalOp', N'VARCHAR(50)') AS LogicalOperator
 ,RO.p.value(N'@NodeId', N'INT') AS NodeID
 ,RO.p.value(N'@Parallel',N'VARCHAR(50)') AS Parallel
 ,RO.p.value(N'@RemoteDataAccess', N'VARCHAR(50)') AS RemoteDataAccess
 ,RO.p.value(N'@Partitioned', N'VARCHAR(50)') AS Partitioned
 ,RO.p.value(N'@PhysicalOp',  N'varchar(50)') AS PhysicalOperator
 ,RO.p.value(N'@IsAdaptive',  N'INT') AS IsAdaptive
 ,RO.p.value(N'@AdaptiveThresholdRows',  N'INT') AS AdaptiveThresholdRows
 ,RO.p.value(N'@EstimatedTotalSubtreeCost',  N'FLOAT') AS EstimatedTotalSubtreeCost
 ,RO.p.value(N'@TableCardinality',  N'FLOAT') AS TableCardinality
 ,RO.p.value(N'@StatsCollectionId',  N'VARCHAR(100)') AS StatisticsCollectionID
 ,RO.p.value(N'@EstimatedJoinType',  N'varchar(50)') AS EstimatedJoinType
 ,NL.p.value(N'@Thread', N'INT') AS Thread
 ,NL.p.value(N'@BrickId', N'INT') AS BrickID
 ,NL.p.value(N'@ActualRows', N'int') as ActualRows
 ,NL.p.value(N'@ActualRowsRead', N'int') AS ActualRowsRead
 ,NL.p.value(N'@ActualElapsedms', N'int') AS ActualElapsedms
 ,NL.p.value(N'@ActualCPUms', N'int') AS ActualCPUms
 ,NL.p.value(N'@ActualScans', N'int') AS ActualScans
 ,NL.p.value(N'@ActualLogicalReads', N'int') AS ActualLogicalReads
 ,NL.p.value(N'@ActualPhysicalReads', N'int') AS ActualPhysicalReads
 ,NL.p.value(N'@ActualReadAheads', N'int') AS ActualReadAheads
 ,NL.p.value(N'@ActualLobLogicalReads', N'int') AS ActualLobLogicalReads
 ,NL.p.value(N'@ActualLobPhysicalReads', N'int') AS ActualLobPhysicalReads
 ,NL.p.value(N'@ActualLobReadAheads', N'int') AS ActualLobReadAheads
 ,NLOC.p.value(N'@Database', N'VARCHAR(200)') AS DatabaseName
 ,NLOC.p.value(N'@Schema', N'VARCHAR(200)') AS SchemaName
 ,NLOC.p.value(N'@Table', N'VARCHAR(200)') AS TableName
 ,NLOC.p.value(N'@Alias', N'VARCHAR(200)') AS AliasName
 ,NLOC.p.value(N'@Column', N'VARCHAR(200)') AS ColumnName
FROM 
    @queryplan AS qp 
CROSS APPLY  qp.ShowPlanXML.nodes(N'//RelOp') AS RO(p)
CROSS APPLY  RO.p.nodes(N'RunTimeInformation/RunTimeCountersPerThread') AS NL(p)
CROSS APPLY  RO.p.nodes(N'OutputList/ColumnReference') AS NLOC(p)

-- WHERE RO.p.value(N'@NodeId', N'INT') = 17
	

--<NestedLoops Optimized="false">
--                        <OuterReferences>
--                          <ColumnReference Database="[AdventureWorks]" Schema="[Person]" Table="[Person]" Alias="[p]" Column="BusinessEntityID" />
--                        </OuterReferences>

	/*

	Thread="0" 
	ActualRows="17" 
	Batches="0" 
	ActualEndOfScans="1" 
	ActualExecutions="1" 
	ActualExecutionMode="Row" 
	ActualElapsedms="0" 
	ActualCPUms="0" 
	*/
	/*
	<xsd:attribute name="Thread" type="xsd:int" use="required"/>
	<xsd:attribute name="BrickId" type="xsd:int" use="optional"/>
	<xsd:attribute name="ActualRebinds" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualRewinds" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualRows" type="xsd:unsignedLong" use="required"/>
	<xsd:attribute name="ActualRowsRead" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="Batches" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualEndOfScans" type="xsd:unsignedLong" use="required"/>
	<xsd:attribute name="ActualExecutions" type="xsd:unsignedLong" use="required"/>
	<xsd:attribute name="ActualExecutionMode" type="shp:ExecutionModeType" use="optional"/>
	<!-- more optional counters -->
	<xsd:attribute name="TaskAddr" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="SchedulerId" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="FirstActiveTime" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="LastActiveTime" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="OpenTime" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="FirstRowTime" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="LastRowTime" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="CloseTime" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualElapsedms" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualCPUms" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualScans" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualLogicalReads" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualPhysicalReads" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualReadAheads" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualLobLogicalReads" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualLobPhysicalReads" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="ActualLobReadAheads" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="SegmentReads" type="xsd:int" use="optional"/>
	<xsd:attribute name="SegmentSkips" type="xsd:int" use="optional"/>
	<xsd:attribute name="ActualLocallyAggregatedRows" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="InputMemoryGrant" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="OutputMemoryGrant" type="xsd:unsignedLong" use="optional"/>
	<xsd:attribute name="UsedMemoryGrant" type="xsd:unsignedLong" use="optional"/>

	*/