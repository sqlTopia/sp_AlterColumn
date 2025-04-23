IF SCHEMA_ID(N'tools') IS NULL
        EXEC(N'CREATE SCHEMA tools;');
GO
IF OBJECT_ID(N'tools.sp_AlterColumn', N'P') IS NULL
        EXEC(N'CREATE PROCEDURE tools.sp_AlterColumn AS');
GO
SET ANSI_NULLS, QUOTED_IDENTIFIER ON;
GO
ALTER PROCEDURE tools.sp_AlterColumn
(
        @use_sql_agent BIT = 0,
        @database_collation_name VARCHAR(128) = NULL,
        @execute_index_conflict_check BIT = 0,
        @execute_inline_conversion_check BIT = 0,
        @execute_cleantable_check BIT = 0,
        @execute_refreshmodule_check BIT = 0,
        @number_of_processes TINYINT = 4,
        @process_statements INT = 2147483647,
        @maximum_retry_count TINYINT = 100,
        @wait_time TIME(3) = '00:00:00.250',
        @verbose BIT = 1
)
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Local helper variables
DECLARE @item INT,
        @items INT,
        @progress VARCHAR(5),
        @curr_id INT,
        @stop_id INT,
        @sql VARCHAR(MAX),
        @helper VARCHAR(MAX),
        @table_name VARCHAR(257),
        @index_name VARCHAR(128);

-- This is the proper metadata for the columns that need to alter
CREATE TABLE    #configurations
                (
                        id INT IDENTITY(1, 1) NOT NULL PRIMARY KEY CLUSTERED,
                        table_id INT NOT NULL,
                        table_name VARCHAR(257) COLLATE DATABASE_DEFAULT NOT NULL,
                        column_id INT NOT NULL,
                        column_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        new_column_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        is_nullable VARCHAR(5) COLLATE DATABASE_DEFAULT NOT NULL,
                        datatype_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        system_datatype_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        is_computed BIT NOT NULL,
                        is_user_defined BIT NOT NULL,
                        max_length VARCHAR(4) COLLATE DATABASE_DEFAULT NULL,
                        precision TINYINT NULL,
                        scale TINYINT NULL,
                        collation_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        xml_collection_name VARCHAR(257) COLLATE DATABASE_DEFAULT NULL,
                        datatype_default_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        datatype_rule_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        is_inline_conversion_possible BIT NOT NULL,
                        INDEX uix_configurations UNIQUE NONCLUSTERED (table_name, column_name)
                );

-- This is the current metadata
CREATE TABLE    #current
                (
                        table_id INT NOT NULL,
                        table_name VARCHAR(257) COLLATE DATABASE_DEFAULT NOT NULL,
                        column_id INT NOT NULL,
                        column_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        is_computed BIT NOT NULL,
                        is_user_defined BIT NOT NULL,
                        is_nullable BIT NOT NULL,
                        datatype_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        system_datatype_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        max_length SMALLINT NOT NULL,
                        precision TINYINT NOT NULL,
                        scale TINYINT NOT NULL,
                        collation_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        xml_collection_name VARCHAR(257) COLLATE DATABASE_DEFAULT NULL,
                        datatype_default_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        datatype_rule_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        graph_id INT NOT NULL,
                        node_count INT NOT NULL
                        INDEX ucx_current UNIQUE CLUSTERED (table_id, column_id)
                );

-- How columns are connected
CREATE TABLE    #graphs
                (
                        source_table_id INT NOT NULL,
                        source_column_id INT NOT NULL,
                        target_table_id INT NOT NULL,
                        target_column_id INT NOT NULL,
                        INDEX cx_graphs CLUSTERED (source_table_id, source_column_id)
                );

-- This is the wanted metadata
CREATE TABLE    #future
                (
                        tag VARCHAR(36) COLLATE DATABASE_DEFAULT NOT NULL,
                        table_id INT NULL,
                        table_name VARCHAR(257) COLLATE DATABASE_DEFAULT NOT NULL,
                        column_id INT NULL,
                        column_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        new_column_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        is_computed BIT NULL,
                        is_user_defined BIT NULL,
                        is_nullable BIT NULL,
                        datatype_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        system_datatype_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        max_length SMALLINT NULL,
                        precision TINYINT NULL,
                        scale TINYINT NULL,
                        collation_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        xml_collection_name VARCHAR(257) COLLATE DATABASE_DEFAULT NULL,
                        datatype_default_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        datatype_rule_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        log_text VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        graph_id INT NULL,
                        node_count INT NULL,
                        is_replenished BIT NOT NULL,
                        INDEX ix_future NONCLUSTERED (table_id, column_id) INCLUDE(graph_id),
                        PRIMARY KEY CLUSTERED (table_name, column_name, tag)
                );

-- These are the foreign keys involved in column changes
CREATE TABLE    #foreign_keys
                (
                        id INT NOT NULL,
                        name VARCHAR (128) COLLATE DATABASE_DEFAULT NOT NULL,
                        delete_action VARCHAR(MAX) COLLATE DATABASE_DEFAULT NULL,
                        update_action VARCHAR(MAX) COLLATE DATABASE_DEFAULT NULL,
                        referenced_table_name VARCHAR(261) COLLATE DATABASE_DEFAULT NOT NULL,
                        referenced_columns VARCHAR(MAX) COLLATE DATABASE_DEFAULT NULL,
                        parent_table_name VARCHAR(261) COLLATE DATABASE_DEFAULT NOT NULL,
                        entity VARCHAR(257) COLLATE DATABASE_DEFAULT NOT NULL,
                        parent_columns VARCHAR(MAX) COLLATE DATABASE_DEFAULT NULL,
                        page_count BIGINT NOT NULL,
                        PRIMARY KEY CLUSTERED (id)
                );

-- These are the indexes involved in  column changes
CREATE TABLE    #index_columns
                (
                        is_unique BIT NOT NULL,
                        table_id INT NOT NULL,
                        table_name VARCHAR(257) COLLATE DATABASE_DEFAULT NOT NULL,
                        entity VARCHAR(257) COLLATE DATABASE_DEFAULT NOT NULL,
                        index_id INT NOT NULL,
                        index_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        column_id INT NOT NULL,
                        column_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        new_column_name VARCHAR(128) COLLATE DATABASE_DEFAULT NULL,
                        key_ordinal INT NOT NULL,
                        is_included_column INT NOT NULL,
                        partition_ordinal INT NOT NULL,
                        column_store_order_ordinal INT NOT NULL,
                        is_descending_key BIT NOT NULL,
                        index_column_id INT NOT NULL,
                        PRIMARY KEY CLUSTERED (table_id, index_id, column_id)
                );

-- These are the page counts
CREATE TABLE    #page_counts
                (
                        object_id INT NOT NULL,
                        page_count DECIMAL(19, 0) NOT NULL
                );

-- These are the indexes involved with possible collation conflict
CREATE TABLE    #conflict_indexes
                (
                        id INT NOT NULL PRIMARY KEY CLUSTERED,
                        table_id INT NOT NULL,
                        table_name VARCHAR(257) COLLATE DATABASE_DEFAULT NOT NULL,
                        index_id INT NOT NULL,
                        index_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        filter_definition VARCHAR(MAX) COLLATE DATABASE_DEFAULT NOT NULL,
                        column_names VARCHAR(MAX) COLLATE DATABASE_DEFAULT NOT NULL,
                        select_names VARCHAR(MAX) COLLATE DATABASE_DEFAULT NULL,
                        group_by_names VARCHAR(MAX) COLLATE DATABASE_DEFAULT NULL,
                        UNIQUE (table_name, index_name),
                        INDEX uix_conflict_indexes_table_id_index_id UNIQUE NONCLUSTERED (table_id, index_id)
                );

-- These are the actual index conflicts values
CREATE TABLE    #conflict_payload
                (
                        id INT NOT NULL,
                        table_name VARCHAR(257) COLLATE DATABASE_DEFAULT NOT NULL,
                        index_name VARCHAR(128) COLLATE DATABASE_DEFAULT NOT NULL,
                        content VARCHAR(MAX) COLLATE DATABASE_DEFAULT NOT NULL,
                        INDEX cx_conflict_payload CLUSTERED (table_name, index_name)
                );

-- These are the dependencies
CREATE TABLE    #dependencies
                (
                        level INT NOT NULL,
                        object_id INT NOT NULL,
                        column_id INT NOT NULL,
                        PRIMARY KEY CLUSTERED (object_id, column_id)
                );

BEGIN TRY
        /*
                Validate environment
        */

        RAISERROR('Processing is starting...', 10, 1) WITH NOWAIT;

        -- Transaction count
        IF @@TRANCOUNT >= 1
                BEGIN
                        RAISERROR('sp_AlterColumn is not allowed to run inside a transaction.', 18, 1);
                END;

        -- Processing has started
        IF EXISTS(SELECT * FROM tools.atac_queue AS taq WHERE taq.status_code != 'L')
                BEGIN
                        SELECT  @item = MAX(CASE WHEN taq.status_code = 'F' THEN taq.statement_id ELSE 0 END),
                                @items = COUNT(*)
                        FROM    tools.atac_queue AS taq;

                        SET     @progress = FORMAT(100E * @item / @items, '#,0.00');

                        RAISERROR('Current processing has completed %s%%. Please try again later.', 16, 1, @progress);
                END;

        -- Configurations exist
        SET     @database_collation_name = PARSENAME(@database_collation_name, 1);

        IF NOT EXISTS(SELECT * FROM tools.atac_configurations AS cfg) AND (@database_collation_name IS NULL OR @database_collation_name = CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS VARCHAR(128)))
                BEGIN
                        RAISERROR('There are no configurations to process.', 16, 1, @progress);
                END;

        -- SQL Agent
        IF @use_sql_agent = 1 AND NOT EXISTS(SELECT * FROM sys.dm_server_services AS dss WHERE dss.servicename = CONCAT('SQL Server Agent (', @@SERVICENAME, ')') AND dss.status = 4)
                BEGIN
                        SET     @use_sql_agent = 0;

                        RAISERROR('Manual processing is required as SQL Agent is not running on instance %s.', 10, 1, @@SERVICENAME) WITH NOWAIT;
                END;

        /*
                Validate user supplied parameter values
        */

        RAISERROR('Validating user supplied parameter values...', 10, 1) WITH NOWAIT;

        -- Current database collation name
        IF @database_collation_name IS NULL OR @database_collation_name = CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS VARCHAR(128))
                BEGIN
                        SET     @database_collation_name = NULL;
                END;
        ELSE IF NOT EXISTS(SELECT * FROM sys.fn_helpcollations() AS hc WHERE hc.name COLLATE DATABASE_DEFAULT = @database_collation_name)
                BEGIN
                        RAISERROR('Collation %s is not valid.', 16, 1, @database_collation_name);
                END;

        -- Execute index conflict check
        IF @execute_index_conflict_check IS NULL
                BEGIN
                        SET     @execute_index_conflict_check = 0;
                END;

        -- Execute inline conversion check
        IF @execute_inline_conversion_check IS NULL
                BEGIN
                        SET     @execute_inline_conversion_check = 0;
                END;

        -- Execute cleantable check
        IF @execute_cleantable_check IS NULL
                BEGIN
                        SET     @execute_cleantable_check = 0;
                END;

        -- Execute refreshmodule check
        IF @execute_refreshmodule_check IS NULL
                BEGIN
                        SET     @execute_refreshmodule_check = 0;
                END;

        -- Number of processes
        IF @number_of_processes = 0
                BEGIN
                        RAISERROR('Number of processes must be between 1 and 255.', 10, 1);

                        RETURN;
                END;
        ELSE IF @number_of_processes IS NULL
                BEGIN
                        SET     @number_of_processes = 4;
                END;

        -- Process statements
        IF @process_statements IS NULL OR @process_statements < 0
                BEGIN
                        SET     @process_statements = 2147483647;
                END;

        -- Maximum retry count
        IF @maximum_retry_count IS NULL OR @maximum_retry_count > 100
                BEGIN
                        SET     @maximum_retry_count = 100;
                END;

        -- Wait time
        IF @wait_time IS NULL
                BEGIN
                        SET     @wait_time = '00:00:00.250';
                END;

        -- Verbose
        IF @verbose IS NULL
                BEGIN
                        SET     @verbose = 1;
                END;

        /*
                Validate configurations
        */
        RAISERROR('Validating configurations...', 10, 1) WITH NOWAIT;

        INSERT          #page_counts
                        (
                                object_id,
                                page_count
                        )
        SELECT          ps.object_id,
                        SUM(ps.reserved_page_count) AS page_count
        FROM            sys.dm_db_partition_stats AS ps
        GROUP BY        ps.object_id
        HAVING          SUM(ps.reserved_page_count) >= 1;

        RAISERROR('  Calculating graphs...', 10, 1) WITH NOWAIT;

        -- Fetch current payload
        INSERT          #current
                        (
                                table_id,
                                table_name,
                                column_id,
                                column_name,
                                is_computed,
                                is_user_defined,
                                is_nullable,
                                datatype_name,
                                system_datatype_name,
                                max_length,
                                precision,
                                scale,
                                collation_name,
                                xml_collection_name,
                                datatype_default_name,
                                datatype_rule_name,
                                graph_id,
                                node_count
                        )
        SELECT          tbl.object_id AS table_id,
                        CONCAT(sch.name COLLATE DATABASE_DEFAULT, '.', tbl.name COLLATE DATABASE_DEFAULT) AS table_name,
                        col.column_id AS column_id,
                        col.name COLLATE DATABASE_DEFAULT,
                        col.is_computed,
                        usr.is_user_defined,
                        col.is_nullable,
                        usr.name COLLATE DATABASE_DEFAULT AS datatype_name,
                        typ.name COLLATE DATABASE_DEFAULT AS system_datatype_name,
                        col.max_length,
                        col.precision,
                        col.scale,
                        col.collation_name,
                        xsc.name COLLATE DATABASE_DEFAULT AS xml_collection_name,
                        def.name COLLATE DATABASE_DEFAULT AS datatype_default_name,
                        rul.name COLLATE DATABASE_DEFAULT AS datatype_rule_name,
                        ROW_NUMBER() OVER (ORDER BY col.object_id, col.column_id) AS graph_id,
                        1 AS node_count
        FROM            sys.columns AS col
        INNER JOIN      sys.tables AS tbl ON tbl.object_id = col.object_id
        INNER JOIN      sys.schemas AS sch ON sch.schema_id = tbl.schema_id
        INNER JOIN      sys.types AS usr ON usr.user_type_id = col.user_type_id
        INNER JOIN      sys.types AS typ ON typ.user_type_id = col.system_type_id
        LEFT JOIN       sys.xml_schema_collections AS xsc ON xsc.xml_collection_id = col.xml_collection_id
        LEFT JOIN       sys.objects AS def ON def.object_id = col.default_object_id
                                AND def.type = N'D'
        LEFT JOIN       sys.objects AS rul ON rul.object_id = col.rule_object_id
                                AND def.type = N'R'
        ORDER BY        col.object_id,
                        col.column_id;

        -- Get graphs
        WITH cte_graphs(source_table_id, source_column_id, target_table_id, target_column_id)
        AS (
                SELECT  fkc.parent_object_id AS source_table_id,
                        fkc.parent_column_id AS source_column_id,
                        fkc.referenced_object_id AS target_table_id,
                        fkc.referenced_column_id AS target_column_id
                FROM    sys.foreign_key_columns AS fkc

                UNION

                SELECT  fkc.referenced_object_id AS source_table_id,
                        fkc.referenced_column_id AS source_column_id,
                        fkc.parent_object_id AS target_table_id,
                        fkc.parent_column_id AS target_column_id
                FROM    sys.foreign_key_columns AS fkc
        )
        INSERT          #graphs
                        (
                                source_table_id,
                                source_column_id,
                                target_table_id,
                                target_column_id
                        )
        SELECT          cte.source_table_id,
                        cte.source_column_id,
                        cte.target_table_id,
                        cte.target_column_id
        FROM            cte_graphs AS cte
        ORDER BY        cte.source_table_id,
                        cte.source_column_id,
                        cte.target_table_id,
                        cte.target_column_id;

        -- Connect related columns into graphs
        WHILE @@ROWCOUNT >= 1
                BEGIN
                        MERGE   #current AS tgt
                        USING   (
                                        SELECT          grp.target_table_id AS table_id,
                                                        grp.target_column_id AS column_id,
                                                        MIN(cur.graph_id) AS graph_id
                                        FROM            #current AS cur
                                        INNER JOIN      #graphs AS grp ON grp.source_table_id = cur.table_id
                                                                AND grp.source_column_id = cur.column_id
                                        GROUP BY        grp.target_table_id,
                                                        grp.target_column_id
                                ) AS src ON src.table_id = tgt.table_id
                                        AND src.column_id = tgt.column_id
                        WHEN    MATCHED AND tgt.graph_id > src.graph_id
                                THEN    UPDATE 
                                        SET     tgt.graph_id = src.graph_id;
                END;

        -- Calculate node count
        WITH cte_graphs(node_count, items)
        AS (
                SELECT  cur.node_count,
                        COUNT(*) OVER (PARTITION BY cur.graph_id) AS items
                FROM    #current AS cur
        )
        UPDATE  cte
        SET     cte.node_count = cte.items
        FROM    cte_graphs AS cte
        WHERE   cte.items >= 2;

        -- Fetch future payload
        RAISERROR('  Checking configurations...', 10, 1) WITH NOWAIT;

        INSERT          #future
                        (
                                tag,
                                table_id,
                                table_name,
                                column_id,
                                column_name,
                                new_column_name,
                                is_computed,
                                is_user_defined,
                                is_nullable,
                                datatype_name,
                                system_datatype_name,
                                max_length,
                                precision,
                                scale,
                                collation_name,
                                xml_collection_name,
                                datatype_default_name,
                                datatype_rule_name,
                                graph_id,
                                node_count,
                                is_replenished,
                                log_text
                        )
        SELECT          cfg.tag,
                        tbl.object_id AS table_id,
                        cfg.table_name,
                        col.column_id,
                        cfg.column_name,
                        cfg.new_column_name,
                        col.is_computed,
                        usr.is_user_defined,
                        CASE
                                WHEN cfg.is_nullable IN ('true', 'yes') THEN 1
                                WHEN cfg.is_nullable IN ('false', 'no') THEN 0
                                ELSE col.is_nullable
                        END AS is_nullable,
                        cfg.datatype_name,
                        typ.name AS system_datatype_name,
                        CASE
                                WHEN typ.name COLLATE DATABASE_DEFAULT IN ('nvarchar', 'varbinary', 'varchar') AND cfg.max_length = 'MAX' THEN -1
                                WHEN typ.name COLLATE DATABASE_DEFAULT IN ('binary', 'char', 'varbinary', 'varchar') AND cfg.max_length IS NOT NULL THEN CAST(cfg.max_length AS SMALLINT)
                                WHEN typ.name COLLATE DATABASE_DEFAULT IN ('nchar', 'nvarchar') AND cfg.max_length IS NOT NULL THEN CAST(2 * cfg.max_length AS SMALLINT)
                                ELSE COALESCE(typ.max_length, cur.max_length)
                        END AS max_length,
                        CASE
                                WHEN typ.name COLLATE DATABASE_DEFAULT IN ('decimal', 'numeric') AND cfg.precision IS NOT NULL THEN cfg.precision
                                ELSE COALESCE(typ.precision, cur.precision)
                        END AS precision,
                        CASE
                                WHEN typ.name COLLATE DATABASE_DEFAULT IN ('datetime2', 'datetimeoffet', 'decimal', 'numeric', 'time') AND cfg.scale IS NOT NULL THEN cfg.scale
                                ELSE COALESCE(typ.scale, cur.scale)
                        END AS scale,
                        CASE
                                WHEN typ.name COLLATE DATABASE_DEFAULT IN ('char', 'nchar', 'nvarchar', 'varchar') AND cfg.collation_name > '' THEN cfg.collation_name
                                WHEN typ.name COLLATE DATABASE_DEFAULT IN ('char', 'nchar', 'nvarchar', 'varchar') AND cfg.collation_name = '' THEN COALESCE(@database_collation_name, CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS SYSNAME))
                                WHEN typ.name COLLATE DATABASE_DEFAULT IN ('char', 'nchar', 'nvarchar', 'varchar') AND cfg.collation_name IS NULL THEN COALESCE(@database_collation_name, cur.collation_name)
                                ELSE NULL
                        END AS collation_name,
                        CASE
                                WHEN typ.name COLLATE DATABASE_DEFAULT = 'xml' AND cfg.xml_collection_name >= '' THEN cfg.xml_collection_name
                                ELSE cur.xml_collection_name
                        END AS xml_collection_name,
                        CASE
                                WHEN cfg.datatype_default_name >= '' THEN cfg.datatype_default_name
                                ELSE cur.datatype_default_name
                        END AS datatype_default_name,
                        CASE
                                WHEN cfg.datatype_rule_name >= '' THEN cfg.datatype_rule_name
                                ELSE cur.datatype_rule_name
                        END AS datatype_rule_name,
                        cur.graph_id,
                        cur.node_count,
                        0 AS is_replenished,
                        CASE
                                WHEN tbl.object_id IS NULL THEN CONCAT('Table ', cfg.table_name, ' is not valid.')
                                WHEN col.column_id IS NULL THEN CONCAT('Column ', cfg.column_name, ' is not valid.')
                                WHEN usr.user_type_id IS NULL THEN CONCAT('Datatype ', cfg.datatype_name, ' is not valid.')
                                WHEN cfg.collation_name > '' AND hc.name IS NULL THEN CONCAT('Collation ', cfg.collation_name, ' is not valid.')
                                WHEN cfg.xml_collection_name > '' AND xsc.name IS NULL THEN CONCAT('XML collection ', cfg.xml_collection_name, ' is not valid.')
                                WHEN cfg.datatype_default_name > '' AND def.name IS NULL THEN CONCAT('Default ', cfg.datatype_default_name, ' is not valid.')
                                WHEN cfg.datatype_rule_name > '' AND rul.name IS NULL THEN CONCAT('Rule ', cfg.datatype_rule_name, ' is not valid.')
                                ELSE NULL
                        END AS log_text
        FROM            tools.atac_configurations AS cfg
        LEFT JOIN       sys.schemas AS sch ON sch.name COLLATE DATABASE_DEFAULT = PARSENAME(cfg.table_name, 2)
        LEFT JOIN       sys.tables AS tbl ON tbl.schema_id = sch.schema_id
                                AND tbl.name COLLATE DATABASE_DEFAULT = PARSENAME(cfg.table_name, 1)
        LEFT JOIN       sys.columns AS col ON col.object_id = tbl.object_id
                                AND col.name COLLATE DATABASE_DEFAULT = cfg.column_name
        LEFT JOIN       sys.types AS usr ON usr.name COLLATE DATABASE_DEFAULT = cfg.datatype_name
        LEFT JOIN       sys.types AS typ ON typ.user_type_id = usr.system_type_id
        LEFT JOIN       sys.fn_helpcollations() AS hc ON hc.name COLLATE DATABASE_DEFAULT = cfg.collation_name
        LEFT JOIN       sys.xml_schema_collections AS xsc ON CONCAT(SCHEMA_NAME(xsc.schema_id) COLLATE DATABASE_DEFAULT, '.', xsc.name COLLATE DATABASE_DEFAULT) = cfg.xml_collection_name
        LEFT JOIN       sys.objects AS def ON def.name COLLATE DATABASE_DEFAULT = cfg.datatype_default_name
                                AND def.type COLLATE DATABASE_DEFAULT = 'D'
        LEFT JOIN       sys.objects AS rul ON rul.name COLLATE DATABASE_DEFAULT = cfg.datatype_rule_name
                                AND rul.type COLLATE DATABASE_DEFAULT = 'R'
        LEFT JOIN       #current AS cur ON cur.table_id = col.object_id
                                AND cur.column_id = col.column_id
        ORDER BY        cfg.table_name,
                        cfg.column_name,
                        cfg.tag;

        UPDATE          cfg
        SET             cfg.log_text = fut.log_text
        FROM            tools.atac_configurations AS cfg
        INNER JOIN      #future AS fut ON fut.table_name = cfg.table_name
                                AND fut.column_name = cfg.column_name
        WHERE           fut.log_text IS NOT NULL;

        IF EXISTS (SELECT * FROM tools.atac_configurations AS cfg WHERE cfg.log_text IS NOT NULL)
                BEGIN
                        RAISERROR('Configuration error. For more information see column log_text.', 16, 1);
                END;

        -- New column name already exist in table
        UPDATE          fut
        SET             fut.log_text = CONCAT('Column ', fut.new_column_name, ' already exist in table ', fut.table_name, '.')
        FROM            #future AS fut
        INNER JOIN      sys.columns AS col ON col.object_id = fut.table_id
                                AND col.name COLLATE DATABASE_DEFAULT = fut.new_column_name;

        UPDATE          cfg
        SET             cfg.log_text = fut.log_text
        FROM            tools.atac_configurations AS cfg
        INNER JOIN      #future AS fut ON fut.table_name = cfg.table_name
                                AND fut.column_name = cfg.column_name
        WHERE           fut.log_text IS NOT NULL;

        IF EXISTS (SELECT * FROM tools.atac_configurations AS cfg WHERE cfg.log_text IS NOT NULL)
                BEGIN
                        RAISERROR('Configuration error. For more information see column log_text.', 16, 1);
                END;

        -- Same column is using different new column name
        WITH cte_new_column_name(log_text, mi, mx, table_name, column_name, is_replenished)
        AS (
                SELECT  fut.log_text,
                        MIN(COALESCE(fut.new_column_name, fut.column_name)) OVER (PARTITION BY fut.table_id, fut.column_id) AS mi,
                        MAX(COALESCE(fut.new_column_name, fut.column_name)) OVER (PARTITION BY fut.table_id, fut.column_id) AS mx,
                        fut.table_name,
                        fut.column_name,
                        fut.is_replenished
                FROM    #future AS fut
        )
        UPDATE          cte
        SET             cte.log_text = CONCAT('Different new column name is used in tables {', wrk.column_names, '}.')
        FROM            cte_new_column_name AS cte
        CROSS APPLY     (
                                SELECT  STRING_AGG(CAST(x.column_name AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY x.column_name) AS column_names
                                FROM    (
                                                SELECT  CONCAT(QUOTENAME(PARSENAME(cur.table_name, 2)), '.', QUOTENAME(PARSENAME(cur.table_name, 1)), '.', QUOTENAME(cur.column_name)) AS column_name,
                                                        ROW_NUMBER() OVER (PARTITION BY cte.table_name, cte.column_name ORDER BY cte.table_name, cte.column_name) AS rnk
                                                FROM    #current AS cur
                                                WHERE   cur.table_name = cte.table_name
                                                        AND cur.column_name = cte.column_name
                                        ) AS x
                                WHERE   x.rnk = 1
                        ) AS wrk
        WHERE           cte.mi < cte.mx
                        AND cte.is_replenished = 0;

        UPDATE          cfg
        SET             cfg.log_text = fut.log_text
        FROM            tools.atac_configurations AS cfg
        INNER JOIN      #future AS fut ON fut.table_name = cfg.table_name
                                AND cfg.column_name = cfg.column_name
        WHERE           fut.log_text IS NOT NULL;

        IF EXISTS (SELECT * FROM tools.atac_configurations AS cfg WHERE cfg.log_text IS NOT NULL)
                BEGIN
                        RAISERROR('Configuration error. For more information see column log_text.', 16, 1);
                END;

        -- Check indeterministic max_length
        WITH cte_max_length(log_text, mi, mx, graph_id, is_replenished)
        AS (
                SELECT  fut.log_text,
                        MIN(COALESCE(fut.max_length, '')) OVER (PARTITION BY fut.graph_id) AS mi,
                        MAX(COALESCE(fut.max_length, '')) OVER (PARTITION BY fut.graph_id) AS mx,
                        fut.graph_id,
                        fut.is_replenished
                FROM    #future AS fut
                WHERE   fut.node_count >= 2
        )
        UPDATE          cte
        SET             cte.log_text = CONCAT('Different max_length in connected columns {', wrk.column_names, '}.')
        FROM            cte_max_length AS cte
        CROSS APPLY     (
                                SELECT  STRING_AGG(CAST(x.column_name AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY x.column_name) AS column_names
                                FROM    (
                                                SELECT  CONCAT(QUOTENAME(PARSENAME(cur.table_name, 2)), '.', QUOTENAME(PARSENAME(cur.table_name, 1)), '.', QUOTENAME(cur.column_name)) AS column_name,
                                                        ROW_NUMBER() OVER (PARTITION BY cur.max_length ORDER BY cur.table_name, cur.column_name) AS rnk
                                                FROM    #current AS cur
                                                WHERE   cur.graph_id = cte.graph_id
                                        ) AS x
                                WHERE   x.rnk = 1
                        ) AS wrk
        WHERE           cte.mi < cte.mx
                        AND cte.is_replenished = 0
                        AND cte.log_text IS NULL;

        IF EXISTS (SELECT * FROM tools.atac_configurations AS cfg WHERE cfg.log_text IS NOT NULL)
                BEGIN
                        RAISERROR('Configuration error. For more information see column log_text.', 16, 1);
                END;

        -- Check indeterministic precision
        WITH cte_precision( log_text, mi, mx, graph_id, is_replenished)
        AS (
                SELECT  fut.log_text,
                        MIN(COALESCE(fut.precision, -1)) OVER (PARTITION BY fut.graph_id) AS mi,
                        MAX(COALESCE(fut.precision, -1)) OVER (PARTITION BY fut.graph_id) AS mx,
                        fut.graph_id,
                        fut.is_replenished
                FROM    #future AS fut
                WHERE   fut.node_count >= 2
        )
        UPDATE          cte
        SET             cte.log_text = CONCAT('Different precision in connected columns {', wrk.column_names, '}.')
        FROM            cte_precision AS cte
        CROSS APPLY     (
                                SELECT  STRING_AGG(CAST(x.column_name AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY x.column_name) AS column_names
                                FROM    (
                                                SELECT  CONCAT(QUOTENAME(PARSENAME(cur.table_name, 2)), '.', QUOTENAME(PARSENAME(cur.table_name, 1)), '.', QUOTENAME(cur.column_name)) AS column_name,
                                                        ROW_NUMBER() OVER (PARTITION BY cur.precision ORDER BY cur.table_name, cur.column_name) AS rnk
                                                FROM    #current AS cur
                                                WHERE   cur.graph_id = cte.graph_id
                                        ) AS x
                                WHERE   x.rnk = 1
                        ) AS wrk
        WHERE           cte.mi < cte.mx
                        AND cte.is_replenished = 0
                        AND cte.log_text IS NULL;

        IF EXISTS (SELECT * FROM tools.atac_configurations AS cfg WHERE cfg.log_text IS NOT NULL)
                BEGIN
                        RAISERROR('Configuration error. For more information see column log_text.', 16, 1);
                END;

        -- Check indeterministic scale
        WITH cte_scale(log_text, mi, mx, graph_id, is_replenished)
        AS (
                SELECT  fut.log_text,
                        MIN(COALESCE(fut.scale, -1)) OVER (PARTITION BY fut.graph_id) AS mi,
                        MAX(COALESCE(fut.scale, -1)) OVER (PARTITION BY fut.graph_id) AS mx,
                        fut.graph_id,
                        fut.is_replenished
                FROM    #future AS fut
                WHERE   fut.node_count >= 2
        )
        UPDATE          cte
        SET             cte.log_text = CONCAT('Different scale in connected columns {', wrk.column_names, '}.')
        FROM            cte_scale AS cte
        CROSS APPLY     (
                                SELECT  STRING_AGG(CAST(x.column_name AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY x.column_name) AS column_names
                                FROM    (
                                                SELECT  CONCAT(QUOTENAME(PARSENAME(cur.table_name, 2)), '.', QUOTENAME(PARSENAME(cur.table_name, 1)), '.', QUOTENAME(cur.column_name)) AS column_name,
                                                        ROW_NUMBER() OVER (PARTITION BY cur.scale ORDER BY cur.table_name, cur.column_name) AS rnk
                                                FROM    #current AS cur
                                                WHERE   cur.graph_id = cte.graph_id
                                        ) AS x
                                WHERE   x.rnk = 1
                        ) AS wrk
        WHERE           cte.mi < cte.mx
                        AND cte.is_replenished = 0
                        AND cte.log_text IS NULL;

        IF EXISTS (SELECT * FROM tools.atac_configurations AS cfg WHERE cfg.log_text IS NOT NULL)
                BEGIN
                        RAISERROR('Configuration error. For more information see column log_text.', 16, 1);
                END;

        -- Check indeterministic collation name
        WITH cte_collation_name(log_text, mi, mx, graph_id, is_replenished)
        AS (
                SELECT  fut.log_text,
                        MIN(COALESCE(fut.collation_name, '')) OVER (PARTITION BY fut.graph_id) AS mi,
                        MAX(COALESCE(fut.collation_name, '')) OVER (PARTITION BY fut.graph_id) AS mx,
                        fut.graph_id,
                        fut.is_replenished
                FROM    #future AS fut
                WHERE   fut.node_count >= 2
        )
        UPDATE          cte
        SET             cte.log_text = CONCAT('Different collation name in connected columns {', wrk.column_names, '}.')
        FROM            cte_collation_name AS cte
        CROSS APPLY     (
                                SELECT  STRING_AGG(CAST(x.column_name AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY x.column_name) AS column_names
                                FROM    (
                                                SELECT  CONCAT(QUOTENAME(PARSENAME(cur.table_name, 2)), '.', QUOTENAME(PARSENAME(cur.table_name, 1)), '.', QUOTENAME(cur.column_name)) AS column_name,
                                                        ROW_NUMBER() OVER (PARTITION BY cur.collation_name ORDER BY cur.table_name, cur.column_name) AS rnk
                                                FROM    #current AS cur
                                                WHERE   cur.graph_id = cte.graph_id
                                        ) AS x
                                WHERE   x.rnk = 1
                        ) AS wrk
        WHERE           cte.mi < cte.mx
                        AND cte.is_replenished = 0
                        AND cte.log_text IS NULL;

        UPDATE          cfg
        SET             cfg.log_text = fut.log_text
        FROM            tools.atac_configurations AS cfg
        INNER JOIN      #future AS fut ON fut.table_name = cfg.table_name
                                AND fut.column_name = cfg.column_name
        WHERE           fut.log_text IS NOT NULL;

        IF EXISTS (SELECT * FROM tools.atac_configurations AS cfg WHERE cfg.log_text IS NOT NULL)
                BEGIN
                        RAISERROR('Configuration error. For more information see column log_text.', 16, 1);
                END;

        -- Fetch future payload
        RAISERROR('  Replenishing configurations...', 10, 1) WITH NOWAIT;

        -- Get all connected columns to validate properly
        WITH cte_graphs(graph_id, tag, collation_name)
        AS (
                SELECT  fut.graph_id,
                        fut.tag,
                        fut.collation_name
                FROM    #future AS fut
                WHERE   fut.node_count >= 2

                UNION

                SELECT          cur.graph_id,
                                '' AS tag,
                                @database_collation_name AS collation_name
                FROM            #current AS cur
                INNER JOIN      sys.computed_columns AS cc ON cc.object_id = cur.table_id
                                        AND cc.column_id = cur.column_id
                WHERE           @database_collation_name IS NOT NULL
                                AND cur.node_count >= 2
        )
        MERGE   #future AS tgt
        USING   (
                        SELECT          cte.tag,
                                        cur.table_id,
                                        cur.table_name,
                                        cur.column_id,
                                        cur.column_name,
                                        cur.is_user_defined,
                                        cur.is_computed,
                                        cur.is_nullable,
                                        fut.datatype_name,
                                        fut.system_datatype_name,
                                        fut.max_length,
                                        fut.precision,
                                        fut.scale,
                                        fut.collation_name,
                                        cur.xml_collection_name,
                                        cur.datatype_default_name,
                                        cur.datatype_rule_name,
                                        1 AS is_replenished,
                                        cur.graph_id,
                                        cur.node_count
                        FROM            #current AS cur
                        INNER JOIN      cte_graphs AS cte ON cte.graph_id = cur.graph_id
                        INNER JOIN      #future AS fut ON fut.graph_id = cte.graph_id
                ) AS src ON src.table_name = tgt.table_name
                        AND src.column_name = tgt.column_name
                        AND src.tag = tgt.tag
        WHEN    NOT MATCHED BY TARGET
                THEN    INSERT  (
                                        tag,
                                        table_id,
                                        table_name,
                                        column_id,
                                        column_name,
                                        is_user_defined,
                                        is_computed,
                                        is_nullable,
                                        datatype_name,
                                        system_datatype_name,
                                        max_length,
                                        precision,
                                        scale,
                                        collation_name,
                                        xml_collection_name,
                                        datatype_default_name,
                                        datatype_rule_name,
                                        is_replenished,
                                        graph_id,
                                        node_count
                                )
                        VALUES  (
                                        src.tag,
                                        src.table_id,
                                        src.table_name,
                                        src.column_id,
                                        src.column_name,
                                        src.is_user_defined,
                                        src.is_computed,
                                        src.is_nullable,
                                        src.datatype_name,
                                        src.system_datatype_name,
                                        src.max_length,
                                        src.precision,
                                        src.scale,
                                        src.collation_name,
                                        src.xml_collection_name,
                                        src.datatype_default_name,
                                        src.datatype_rule_name,
                                        src.is_replenished,
                                        src.graph_id,
                                        src.node_count
                                );

        IF EXISTS (SELECT * FROM #future AS fut WHERE fut.is_replenished = 1)
                BEGIN
                        INSERT  tools.atac_configurations
                                (
                                        tag,
                                        table_name,
                                        column_name,
                                        new_column_name,
                                        is_nullable,
                                        datatype_name,
                                        max_length,
                                        precision,
                                        scale,
                                        collation_name,
                                        xml_collection_name,
                                        datatype_default_name,
                                        datatype_rule_name,
                                        log_text
                                )
                        SELECT  fut.tag,
                                fut.table_name,
                                fut.column_name,
                                fut.new_column_name,
                                CASE
                                        WHEN fut.is_nullable = 1 THEN 'true'
                                        ELSE 'false'
                                END AS is_nullable,
                                fut.datatype_name,
                                CASE
                                        WHEN fut.datatype_name COLLATE DATABASE_DEFAULT IN ('nvarchar', 'varbinary', 'varchar') AND fut.max_length = -1 THEN 'MAX'
                                        WHEN fut.datatype_name COLLATE DATABASE_DEFAULT IN ('binary', 'char', 'varbinary', 'varchar') THEN fut.max_length
                                        WHEN fut.datatype_name COLLATE DATABASE_DEFAULT IN ('nchar', 'nvarchar') THEN fut.max_length
                                        ELSE NULL
                                END AS max_length,
                                CASE
                                        WHEN fut.datatype_name COLLATE DATABASE_DEFAULT IN ('decimal', 'numeric') THEN fut.precision
                                        ELSE NULL
                                END AS precision,
                                CASE
                                        WHEN fut.datatype_name COLLATE DATABASE_DEFAULT IN ('datetime2', 'datetimeoffet', 'decimal', 'numeric', 'time') THEN fut.scale
                                        ELSE NULL
                                END AS scale,
                                fut.collation_name,
                                fut.xml_collection_name,
                                fut.datatype_default_name,
                                fut.datatype_rule_name,
                                fut.log_text
                        FROM    #future AS fut
                        WHERE   fut.is_replenished = 1;

                        RAISERROR('There are replenished rows. Please validate and start sp_AlterColumn again.', 16, 1);
                END;

        ALTER INDEX ALL ON tools.atac_configurations REBUILD WITH (FILLFACTOR = 100, DATA_COMPRESSION = NONE);

        -- Build statement parts
        WITH cte_configurations(table_id, table_name, column_id, column_name, new_column_name, is_computed, is_user_defined, is_nullable, datatype_name, system_datatype_name, max_length, precision, scale, collation_name, xml_collection_name, datatype_default_name, datatype_rule_name)
        AS (
                SELECT  fut.table_id,
                        fut.table_name,
                        fut.column_id,
                        fut.column_name,
                        fut.new_column_name,
                        fut.is_computed,
                        fut.is_user_defined,
                        fut.is_nullable,
                        fut.datatype_name,
                        fut.system_datatype_name,
                        fut.max_length,
                        fut.precision,
                        fut.scale,
                        fut.collation_name,
                        fut.xml_collection_name,
                        fut.datatype_default_name,
                        fut.datatype_rule_name
                FROM    #future AS fut
                
                EXCEPT

                SELECT  cur.table_id,
                        cur.table_name,
                        cur.column_id,
                        cur.column_name,
                        NULL AS new_column_name,
                        cur.is_computed,
                        cur.is_user_defined,
                        cur.is_nullable,
                        cur.datatype_name,
                        cur.system_datatype_name,
                        cur.max_length,
                        cur.precision,
                        cur.scale,
                        cur.collation_name,
                        cur.xml_collection_name,
                        cur.datatype_default_name,
                        cur.datatype_rule_name
                FROM    #current AS cur
        )
        INSERT          #configurations
                        (
                                table_id,
                                table_name,
                                column_id,
                                column_name,
                                new_column_name,
                                is_computed,
                                is_user_defined,
                                is_nullable,
                                datatype_name,
                                system_datatype_name,
                                max_length,
                                precision,
                                scale,
                                collation_name,
                                xml_collection_name,
                                datatype_default_name,
                                datatype_rule_name,
                                is_inline_conversion_possible
                        )
        SELECT          cte.table_id,
                        cte.table_name,
                        cte.column_id,
                        cte.column_name,
                        cte.new_column_name,
                        cte.is_computed,
                        cte.is_user_defined,
                        CASE
                                WHEN cte.is_nullable = 1 THEN 'true'
                                ELSE 'false'
                        END AS is_nullable,
                        cte.datatype_name,
                        cte.system_datatype_name,
                        CASE
                                WHEN cte.is_user_defined = 1 THEN NULL
                                WHEN cte.datatype_name IN ('nvarchar', 'varbinary', 'varchar') AND cte.max_length = -1 THEN 'MAX'
                                WHEN cte.datatype_name IN ('binary', 'char', 'varbinary', 'varchar') THEN CAST(cte.max_length AS VARCHAR(4))
                                WHEN cte.datatype_name IN ('nchar', 'nvarchar') THEN CAST(cte.max_length / 2 AS VARCHAR(4))
                                ELSE NULL
                        END AS max_length,
                        CASE
                                WHEN cte.is_user_defined = 1 THEN NULL
                                WHEN cte.datatype_name IN ('decimal', 'numeric') THEN cte.precision
                                ELSE NULL
                        END AS precision,
                        CASE
                                WHEN cte.is_user_defined = 1 THEN NULL
                                WHEN cte.datatype_name IN ('datetime2', 'datetimeoffet', 'decimal', 'numeric', 'time') THEN cte.scale
                                ELSE NULL
                        END AS scale,
                        CASE
                                WHEN cte.is_user_defined = 1 THEN NULL
                                ELSE cte.collation_name
                        END AS collation_name,
                        CASE
                                WHEN cte.is_user_defined = 1 THEN NULL
                                WHEN cte.datatype_name = 'xml' THEN cte.xml_collection_name
                                ELSE NULL
                        END AS xml_collection_name,
                        cte.datatype_default_name,
                        cte.datatype_rule_name,
                        1 AS is_inline_conversion_possible
        FROM            cte_configurations AS cte
        ORDER BY        cte.table_name,
                        cte.column_name;

        DELETE  cfg
        FROM    #configurations AS cfg
        WHERE   cfg.system_datatype_name IN ('ntext', 'text')
                AND cfg.collation_name IS NOT NULL;

        IF NOT EXISTS(SELECT * FROM #configurations AS cfg) AND @database_collation_name IS NULL
                BEGIN
                        RAISERROR('There is nothing to change.', 10, 1);

                        RETURN;
                END;

        -- Calculating dependencies
        RAISERROR('Calculating dependencies...', 10, 1) WITH NOWAIT;

        INSERT          #dependencies
                        (
                                level,
                                object_id,
                                column_id
                        )
        SELECT DISTINCT 1 AS level,
                        sed.referencing_id AS object_id,
                        sed.referenced_minor_id AS column_id
        FROM            sys.sql_expression_dependencies AS sed
        INNER JOIN      #configurations AS cfg ON cfg.table_id = sed.referenced_id
                                AND cfg.column_id = sed.referenced_minor_id
        WHERE           sed.is_schema_bound_reference = 1

        /*
                Investigate unique indexes against new collation
        */

        IF @execute_index_conflict_check = 1 AND @database_collation_name IS NOT NULL
                BEGIN
                        RAISERROR('Investigating collation conflicts at index level...', 10, 1, @curr_id, @stop_id) WITH NOWAIT;

                        -- Get all valid indexes
                        WITH cte_indexes(table_id, table_name, index_id, index_name, filter_definition, column_names)
                        AS (
                                SELECT          ind.object_id AS table_id,
                                                CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(ind.object_id)), '.', QUOTENAME(OBJECT_NAME(ind.object_id))) AS table_name,
                                                ind.index_id,
                                                QUOTENAME(MAX(ind.name COLLATE DATABASE_DEFAULT)) AS index_name,
                                                CASE
                                                        WHEN MAX(COALESCE(ind.filter_definition COLLATE DATABASE_DEFAULT, '')) = '' THEN ''
                                                        ELSE CONCAT(' WHERE ', MAX(COALESCE(ind.filter_definition COLLATE DATABASE_DEFAULT, '')))
                                                END AS filter_definition,
                                                STRING_AGG(CAST(QUOTENAME(col.name) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY col.name) AS column_names
                                FROM            sys.indexes AS ind
                                INNER JOIN      sys.index_columns AS icl ON icl.object_id = ind.object_id
                                                        AND icl.index_id = ind.index_id
                                                        AND icl.key_ordinal >= 1
                                INNER JOIN      sys.columns AS col ON col.object_id = icl.object_id
                                                        AND col.column_id = icl.column_id
                                INNER JOIN      sys.objects AS obj ON obj.object_id = col.object_id
                                                        AND obj.type COLLATE DATABASE_DEFAULT IN ('U', 'V')
                                WHERE           ind.is_unique = 1
                                                AND col.collation_name COLLATE DATABASE_DEFAULT <> @database_collation_name
                                                AND     (
                                                                @database_collation_name IS NOT NULL
                                                                OR EXISTS(SELECT * FROM #configurations AS cfg WHERE cfg.table_id = ind.object_id)
                                                        )
                                GROUP BY        ind.object_id,
                                                ind.index_id
                        )
                        INSERT  #conflict_indexes
                                (
                                        id,
                                        table_id,
                                        table_name,
                                        index_id,
                                        index_name,
                                        filter_definition,
                                        column_names
                                )
                        SELECT  ROW_NUMBER() OVER (ORDER BY cte.table_name, cte.index_name) AS id,
                                cte.table_id,
                                cte.table_name,
                                cte.index_id,
                                cte.index_name,
                                cte.filter_definition,
                                cte.column_names
                        FROM    cte_indexes AS cte;

                        -- Delete indexes with same columns but in different order
                        WITH cte_indexes(rnk)
                        AS (
                                SELECT  ROW_NUMBER() OVER (PARTITION BY wrk.table_name, wrk.column_names ORDER BY wrk.index_name) AS rnk
                                FROM    #conflict_indexes AS wrk
                        )
                        DELETE  cte
                        FROM    cte_indexes AS cte
                        WHERE   cte.rnk >= 2;

                        -- Get columns in proper order
                        WITH cte_columns(id, table_id, index_id, column_names, select_names, group_by_names)
                        AS (
                                SELECT          ROW_NUMBER() OVER (ORDER BY wrk.table_id, wrk.index_id) AS id,
                                                wrk.table_id,
                                                wrk.index_id,
                                                STRING_AGG(CAST(QUOTENAME(col.name) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY icl.key_ordinal) AS column_names,
                                                STRING_AGG(CAST(CONCAT(QUOTENAME(col.name), CASE WHEN typ.name IN ('char', 'nchar', 'nvarchar', 'sysname', 'varchar') THEN CONCAT(' COLLATE ', @database_collation_name, ' AS ', QUOTENAME(col.name)) ELSE '' END) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY icl.key_ordinal) AS select_names,
                                                STRING_AGG(CAST(CONCAT(QUOTENAME(col.name), CASE WHEN typ.name IN ('char', 'nchar', 'nvarchar', 'sysname', 'varchar') THEN CONCAT(' COLLATE ', @database_collation_name) ELSE '' END) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY icl.key_ordinal) AS group_by_names
                                FROM            #conflict_indexes AS wrk
                                INNER JOIN      sys.index_columns AS icl ON icl.object_id = wrk.table_id
                                                        AND icl.index_id = wrk.index_id
                                                        AND icl.key_ordinal >= 1
                                INNER JOIN      sys.columns AS col ON col.object_id = icl.object_id
                                                        AND col.column_id = icl.column_id
                                INNER JOIN      sys.types AS typ ON typ.user_type_id = col.system_type_id
                                GROUP BY        wrk.table_id,
                                                wrk.index_id
                        )
                        UPDATE          wrk
                        SET             wrk.id = cte.id,
                                        wrk.column_names = cte.column_names,
                                        wrk.select_names = cte.select_names,
                                        wrk.group_by_names = cte.group_by_names
                        FROM            #conflict_indexes AS wrk
                        INNER JOIN      cte_columns AS cte ON cte.table_id = wrk.table_id
                                                AND cte.index_id = wrk.index_id;

                        -- How many indexes to test
                        SELECT  @curr_id = 1,
                                @stop_id = MAX(wrk.id)
                        FROM    #conflict_indexes AS wrk;

                        -- Iterate all indexes
                        WHILE @curr_id <= @stop_id
                                BEGIN
                                        -- Log intention
                                        IF @curr_id IN (1, @stop_id) OR @curr_id % 100 = 0
                                                BEGIN
                                                        RAISERROR('  Working with index %d of %d...', 10, 1, @curr_id, @stop_id) WITH NOWAIT;
                                                END;

                                        -- Build statement
                                        SELECT  @table_name = wrk.table_name,
                                                @index_name = wrk.index_name,
                                                @sql = CONCAT('INSERT #conflict_payload (id, table_name, index_name, content) SELECT ', @curr_id, ' AS id, ', QUOTENAME(wrk.table_name, ''''), ' AS [table], ', QUOTENAME(wrk.index_name, ''''), ' AS [index], x.content FROM (SELECT ', wrk.select_names, ' FROM ', wrk.table_name, wrk.filter_definition, ' GROUP BY ', wrk.group_by_names, ' HAVING COUNT_BIG(*) >= 2 FOR JSON AUTO) AS x(content) WHERE x.content IS NOT NULL;')
                                        FROM    #conflict_indexes AS wrk
                                        WHERE   wrk.id = @curr_id;

                                        -- Execute statement
                                        EXEC    (@sql);

                                        IF @@ROWCOUNT >= 1
                                                BEGIN
                                                        RAISERROR('    Conflict found with index %s on table %s...', 10, 1, @index_name, @table_name) WITH NOWAIT;
                                                END;

                                        -- Move to next index
                                        SET     @curr_id += 1;
                                END;

                        -- Present the index conflict result
                        IF EXISTS(SELECT * FROM #conflict_payload AS cfl)
                                BEGIN
                                        SELECT          cfl.table_name,
                                                        cfl.index_name,
                                                        CONCAT('{', STRING_AGG(CAST(x.[key] AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY x.[key]), '}') AS key_columns,
                                                        CONCAT('{', STRING_AGG(CAST(x.[value] AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY x.[key]), '}') AS payload_values,
                                                        CONCAT('SELECT * FROM ', cfl.table_name, ' WHERE ', STRING_AGG(CAST(CONCAT(x.[key] COLLATE DATABASE_DEFAULT, ' = ''', REPLACE(x.[value] COLLATE DATABASE_DEFAULT, '''', ''''''), '''') AS VARCHAR(MAX)), ' AND ') WITHIN GROUP (ORDER BY x.[key]), ';') AS sql_statement
                                        FROM            #conflict_payload AS cfl
                                        CROSS APPLY     OPENJSON(cfl.content, '$') AS c
                                        CROSS APPLY     OPENJSON(c.value, '$') AS x
                                        GROUP BY        cfl.table_name,
                                                        cfl.index_name,
                                                        cfl.id,
                                                        c.[key],
                                                        cfl.id
                                        ORDER BY        cfl.id,
                                                        payload_values;

                                        RAISERROR('Collation conflicts were found at index level.', 16, 1);
                                END;
                        ELSE
                                BEGIN
                                        RAISERROR('  No collation conflicts were found at index level.', 10, 1);
                                END;
                END;

        /*
                Check if inline conversion is possible
        */

        IF @execute_inline_conversion_check = 1 AND @database_collation_name IS NOT NULL
                BEGIN
                        RAISERROR('Checking if inline conversion is possible...', 10, 1, @curr_id, @stop_id) WITH NOWAIT;

                        SELECT  @curr_id = 1,
                                @stop_id = MAX(cfg.id)
                        FROM    #configurations AS cfg

                        WHIlE @curr_id <= @stop_id
                                BEGIN
                                        -- Log intention
                                        IF @curr_id IN (1, @stop_id) OR @curr_id % 100 = 0
                                                BEGIN
                                                        RAISERROR('  Working with column %d of %d...', 10, 1, @curr_id, @stop_id) WITH NOWAIT;
                                                END;

                                        -- Build statement
                                        SELECT  @helper =       CASE
                                                                        WHEN cfg.datatype_name IN ('binary', 'char', 'nchar', 'nvarchar', 'varbinary', 'varchar') THEN CONCAT('(', cfg.max_length, ')')
                                                                        WHEN cfg.datatype_name IN ('decimal', 'numeric') THEN CONCAT('(', cfg.precision, ', ', cfg.scale, ')')
                                                                        WHEN cfg.datatype_name IN ('datetime2', 'datetimeoffet', 'decimal', 'numeric', 'time') THEN CONCAT('(', cfg.scale, ')')
                                                                        WHEN cfg.datatype_name = 'xml' THEN CONCAT('(', cfg.xml_collection_name, ')')
                                                                        ELSE ''
                                                                END
                                        FROM    #configurations AS cfg
                                        WHERE   cfg.id = @curr_id;

                                        SELECT  @helper += CONCAT(' COLLATE ', cfg.collation_name)
                                        FROM    #configurations AS cfg
                                        WHERE   cfg.id = @curr_id
                                                AND cfg.datatype_name IN ('char', 'nchar', 'nvarchar', 'sysname', 'varchar')
                                                AND cfg.collation_name > '';

                                        -- TODO::: Specificera per datatyp ex max_length för strängar eller domain för integers
                                        SELECT  @sql = CONCAT('IF EXISTS(SELECT * FROM ', QUOTENAME(PARSENAME(cfg.table_name, 2)), '.', QUOTENAME(PARSENAME(cfg.table_name, 1)), ' WHERE TRY_CAST(', QUOTENAME(cfg.column_name), ' AS ', QUOTENAME(cfg.datatype_name), @helper, ') IS NULL AND ', QUOTENAME(cfg.column_name), ' IS NOT NULL OR TRY_CAST(', QUOTENAME(cfg.column_name), ' AS ', QUOTENAME(cfg.datatype_name), @helper, ') <> ', QUOTENAME(cfg.column_name), ') BEGIN UPDATE cfg SET cfg.is_inline_conversion_possible = 0 WHERE cfg.id = ', @curr_id, ' END;')
                                        FROM    #configurations AS cfg
                                        WHERE   cfg.id = @curr_id;

                                        EXEC    (@sql);

                                        SET     @curr_id += 1;
                                END;

                        -- Present the inline conversion result
                        IF EXISTS(SELECT * FROM #configurations AS cfg WHERE cfg.is_inline_conversion_possible = 0)
                                BEGIN
                                        SELECT          *
                                        FROM            #configurations AS cfg
                                        ORDER BY        cfg.id;

                                        RAISERROR('Inline conversion conflicts were found.', 16, 1);
                                END;
                        ELSE
                                BEGIN
                                        RAISERROR('  No inline conversion conflicts were found.', 10, 1);
                                END;
                END;

        /*
                Populate queue
        */

        -- endt = Enable database triggers
        -- didt = Disable database triggers
        RAISERROR('Adding database trigger statements to queue...', 10, 1) WITH NOWAIT;

        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        DB_NAME() AS entity,
                        act.phase,
                        act.sql_text
        FROM            sys.sql_modules AS sqm
        INNER JOIN      sys.triggers AS trg ON trg.object_id = sqm.object_id
                                AND trg.is_ms_shipped = 0
        CROSS APPLY     (
                                VALUES  (
                                                'didt',
                                                0,
                                                0,
                                                CONCAT('DISABLE TRIGGER ', QUOTENAME(trg.name COLLATE DATABASE_DEFAULT), ' ON DATABASE;')
                                        ),
                                        (
                                                'endt',
                                                440,
                                                14,
                                                CONCAT('ENABLE TRIGGER ', QUOTENAME(trg.name COLLATE DATABASE_DEFAULT), ' ON DATABASE;')
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        WHERE           trg.parent_class_desc = 'DATABASE'
        OPTION          (RECOMPILE);

        -- crix = Create index
        -- drix = Drop index
        RAISERROR('Adding index statements to queue...', 10, 1) WITH NOWAIT;

        WITH cte_candidates(is_unique, table_id, index_id, index_name)
        AS (
                SELECT          ind.is_unique,
                                ind.object_id,
                                ind.index_id,
                                ind.name AS index_name
                FROM            sys.indexes AS ind
                INNER JOIN      sys.index_columns AS icl ON icl.object_id = ind.object_id
                                        AND icl.index_id = ind.index_id
                INNER JOIN      #configurations AS cfg ON cfg.table_id = icl.object_id
                                        AND cfg.column_id = icl.column_id

                UNION

                SELECT          ind.is_unique,
                                ind.object_id,
                                ind.index_id,
                                ind.name AS index_name
                FROM            sys.indexes AS ind
                INNER JOIN      #configurations AS cfg ON cfg.table_id = ind.object_id
                WHERE           CHARINDEX(QUOTENAME(cfg.column_name), ind.filter_definition COLLATE DATABASE_DEFAULT) >= 1

                UNION

                SELECT          ind.is_unique,
                                ind.object_id,
                                ind.index_id,
                                ind.name AS index_name
                FROM            sys.indexes AS ind
                INNER JOIN      sys.index_columns AS icl ON icl.object_id = ind.object_id
                                        AND icl.index_id = ind.index_id
                INNER JOIN      sys.computed_columns AS cc ON cc.object_id = icl.object_id
                                        AND cc.column_id = icl.column_id
                WHERE           @database_collation_name IS NOT NULL

                UNION

                SELECT          ind.is_unique,
                                ind.object_id,
                                ind.index_id,
                                ind.name AS index_name
                FROM            sys.indexes AS ind
                WHERE           ind.has_filter = 1
                                AND @database_collation_name IS NOT NULL
        )
        INSERT          #index_columns
                        (
                                is_unique,
                                table_id,
                                table_name,
                                entity,
                                index_id,
                                index_name,
                                column_id,
                                column_name,
                                new_column_name,
                                key_ordinal,
                                is_included_column,
                                partition_ordinal,
                                column_store_order_ordinal,
                                is_descending_key,
                                index_column_id
                        )
        SELECT          cte.is_unique,
                        tbl.object_id AS table_id,
                        CONCAT(QUOTENAME(sch.name COLLATE DATABASE_DEFAULT), '.', QUOTENAME(tbl.name COLLATE DATABASE_DEFAULT)) AS table_name,
                        CONCAT(sch.name COLLATE DATABASE_DEFAULT, '.', tbl.name COLLATE DATABASE_DEFAULT) AS entity,
                        cte.index_id,
                        QUOTENAME(cte.index_name COLLATE DATABASE_DEFAULT) AS index_name,
                        col.column_id,
                        QUOTENAME(col.name COLLATE DATABASE_DEFAULT) AS column_name,
                        QUOTENAME(cfg.new_column_name) AS new_column_name,
                        icl.key_ordinal,
                        icl.is_included_column,
                        icl.partition_ordinal,
                        icl.column_store_order_ordinal,
                        icl.is_descending_key,
                        icl.index_column_id
        FROM            cte_candidates AS cte
        INNER JOIN      sys.index_columns AS icl ON icl.object_id = cte.table_id
                                AND icl.index_id = cte.index_id
        INNER JOIN      sys.columns AS col ON col.object_id = icl.object_id
                                AND col.column_id = icl.column_id
        INNER JOIN      sys.tables AS tbl ON tbl.object_id = col.object_id
        INNER JOIN      sys.schemas AS sch ON sch.schema_id = tbl.schema_id
        LEFT JOIN       #configurations AS cfg ON cfg.table_id = icl.object_id
                                AND cfg.column_id = icl.column_id
        OPTION          (RECOMPILE);

        -- Fetch metadata
        WITH cte_indexes(table_id, table_name, entity, index_id, index_name, index_type_major, index_type_minor, is_memory_optimized, data_space_definition, data_space_type, data_compression, key_columns, include_columns, other_columns, bucket_count, is_primary_key, is_unique_constraint, is_unique, compression_delay, filter_definition, xml_type_desc, primary_xml_index_name, tessellation_scheme, online, drop_existing, pad_index, statistics_norecompute, sort_in_tempdb, ignore_dup_key, allow_row_locks, allow_page_locks, fill_factor, page_count, bounding_box, grids, cells_per_object, is_disabled, optimize_for_sequential_key, column_store_order)
        AS (
                SELECT DISTINCT wrk.table_id,
                                wrk.table_name,
                                wrk.entity,
                                wrk.index_id,
                                wrk.index_name,
                                ind.type AS index_type_major,
                                COALESCE(xix.xml_index_type, six.spatial_index_type) AS index_type_minor,
                                tbl.is_memory_optimized,
                                CASE
                                        WHEN dsp.name IS NULL THEN ''
                                        WHEN partition_keys.content IS NULL THEN CONCAT(' ON ', QUOTENAME(dsp.name COLLATE DATABASE_DEFAULT))
                                        ELSE CONCAT(' ON ', QUOTENAME(dsp.name COLLATE DATABASE_DEFAULT), '(', partition_keys.content, ')')
                                END AS data_space_definition,
                                dsp.type AS data_space_type,
                                compression_data.content AS data_compression,
                                CASE
                                        WHEN key_columns.content IS NULL THEN ''
                                        ELSE key_columns.content
                                END AS key_columns,
                                CASE
                                        WHEN include_columns.content IS NULL THEN ''
                                        ELSE include_columns.content
                                END AS include_columns,
                                other_columns.content AS other_columns,
                                CONCAT('BUCKET_COUNT = ', his.total_bucket_count) AS bucket_count,
                                ind.is_primary_key,
                                ind.is_unique_constraint,
                                ind.is_unique,
                                CONCAT('COMPRESSION_DELAY = ', ind.compression_delay) AS compression_delay,
                                CASE
                                        WHEN ind.filter_definition IS NULL THEN ''
                                        ELSE CONCAT(' WHERE ', ind.filter_definition COLLATE DATABASE_DEFAULT)
                                END AS filter_definition,
                                xix.secondary_type_desc COLLATE DATABASE_DEFAULT AS xml_type_desc,
                                yix.name COLLATE DATABASE_DEFAULT AS primary_xml_index_name,
                                six.tessellation_scheme COLLATE DATABASE_DEFAULT AS tessellation_scheme,
                                'ONLINE = OFF' AS online,
                                'DROP_EXISTING = OFF' AS drop_existing,
                                CASE
                                        WHEN ind.is_padded = 1 THEN 'PAD_INDEX = ON'
                                        ELSE 'PAD_INDEX = OFF'
                                END AS pad_index,
                                CASE
                                        WHEN sta.no_recompute = 1 THEN 'STATISTICS_NORECOMPUTE = ON'
                                        ELSE 'STATISTICS_NORECOMPUTE = OFF'
                                END statistics_norecompute,
                                'SORT_IN_TEMPDB = ON' AS sort_in_tempdb,
                                CASE
                                        WHEN ind.ignore_dup_key = 1 THEN 'IGNORE_DUP_KEY = ON'
                                        ELSE 'IGNORE_DUP_KEY = OFF'
                                END AS ignore_dup_key,
                                CASE
                                        WHEN ind.allow_row_locks = 1 THEN 'ALLOW_ROW_LOCKS = ON'
                                        ELSE 'ALLOW_ROW_LOCKS = OFF'
                                END AS allow_row_locks,
                                CASE 
                                        WHEN ind.allow_page_locks = 1 THEN 'ALLOW_PAGE_LOCKS = ON'
                                        ELSE 'ALLOW_PAGE_LOCKS = OFF'
                                END AS allow_page_locks,
                                CONCAT('FILLFACTOR = ', COALESCE(cfg.fill_factor, 100)) AS fill_factor,
                                COALESCE(pc.page_count, 0) AS page_count,
                                CASE
                                        WHEN six.spatial_index_type = 1 THEN CONCAT('BOUNDING_BOX = (', sit.bounding_box_xmin, ', ', sit.bounding_box_ymin, ', ', sit.bounding_box_xmax, ', ', sit.bounding_box_ymax, ')')
                                        ELSE ''
                                END AS bounding_box,
                                CONCAT('GRIDS = (LEVEL_1 = ', sit.level_1_grid_desc, ', LEVEL_2 = ', sit.level_2_grid_desc, ', LEVEL_3 = ', sit.level_3_grid_desc, ', LEVEL_4 = ', sit.level_4_grid_desc, ')') AS grids,
                                CONCAT(', CELLS_PER_OBJECT = ', sit.cells_per_object) AS cells_per_object,
                                ind.is_disabled,
                                CASE
                                        WHEN ind.optimize_for_sequential_key = 1 THEN 'OPTIMIZE_FOR_SEQUENTIAL_KEY = ON'
                                        ELSE 'OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF'
                                END AS optimize_for_sequential_key,
                                columnstore_keys.content AS column_store_order
                FROM            #index_columns AS wrk
                INNER JOIN      sys.indexes AS ind ON ind.object_id = wrk.table_id
                                        AND ind.index_id = wrk.index_id
                INNER JOIN      sys.tables AS tbl ON tbl.object_id = wrk.table_id
                LEFT JOIN       sys.data_spaces AS dsp ON dsp.data_space_id = ind.data_space_id
                LEFT JOIN       sys.dm_db_xtp_hash_index_stats AS his ON his.object_id = ind.object_id
                                        AND his.index_id = ind.index_id
                LEFT JOIN       sys.xml_indexes AS xix ON xix.object_id = ind.object_id
                                        AND xix.index_id = ind.index_id
                LEFT JOIN       sys.xml_indexes AS yix ON yix.object_id = xix.object_id
                                        AND yix.index_id = xix.using_xml_index_id
                LEFT JOIN       sys.spatial_indexes AS six ON six.object_id = ind.object_id
                                        AND six.index_id = ind.index_id
                LEFT JOIN       sys.stats AS sta ON sta.object_id = ind.object_id
                                        AND sta.stats_id = ind.index_id
                LEFT JOIN       sys.spatial_index_tessellations AS sit ON sit.object_id = ind.object_id
                                        AND sit.index_id = ind.index_id
                LEFT JOIN       #page_counts AS pc ON pc.object_id = ind.object_id
                OUTER APPLY     (
                                        SELECT  CASE
                                                        WHEN ind.fill_factor = 0 AND CONVERT(TINYINT, value) = 0 THEN CAST(100 AS TINYINT)
                                                        WHEN ind.fill_factor = 0 THEN CONVERT(TINYINT, value)
                                                        ELSE ind.fill_factor
                                                END AS fill_factor
                                        FROM    sys.configurations
                                        WHERE   configuration_id = 109
                                ) AS cfg(fill_factor)
                OUTER APPLY     (
                                        SELECT  STRING_AGG(CAST(CONCAT(COALESCE(ixs.new_column_name, ixs.column_name), CASE WHEN ixs.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY ixs.key_ordinal)
                                        FROM    #index_columns AS ixs
                                        WHERE   ixs.table_id = wrk.table_id
                                                AND ixs.index_id = wrk.index_id
                                                AND ixs.key_ordinal >= 1
                                ) AS key_columns(content)
                OUTER APPLY     (
                                        SELECT  STRING_AGG(CAST(COALESCE(ixs.new_column_name, ixs.column_name) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY ixs.index_column_id)
                                        FROM    #index_columns AS ixs
                                        WHERE   ixs.table_id = wrk.table_id
                                                AND ixs.index_id = wrk.index_id
                                                AND ixs.is_included_column = 1
                                ) AS include_columns(content)
                OUTER APPLY     (
                                        SELECT  STRING_AGG(CAST(COALESCE(ixs.new_column_name, ixs.column_name) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY ixs.partition_ordinal)
                                        FROM    #index_columns AS ixs
                                        WHERE   ixs.table_id = wrk.table_id
                                                AND ixs.index_id = wrk.index_id
                                                AND ixs.partition_ordinal >= 1
                                ) AS partition_keys(content)
                OUTER APPLY     (
                                        SELECT  STRING_AGG(CAST(COALESCE(ixs.new_column_name, ixs.column_name) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY ixs.partition_ordinal)
                                        FROM    #index_columns AS ixs
                                        WHERE   ixs.table_id = wrk.table_id
                                                AND ixs.index_id = wrk.index_id
                                                AND ixs.key_ordinal = 0
                                                AND ixs.is_included_column = 0
                                                AND ixs.partition_ordinal= 0
                                ) AS other_columns(content)
                OUTER APPLY     (
                                        SELECT  STRING_AGG(CAST(COALESCE(ixs.new_column_name, ixs.column_name) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY ixs.column_store_order_ordinal)
                                        FROM    #index_columns AS ixs
                                        WHERE   ixs.table_id = wrk.table_id
                                                AND ixs.index_id = wrk.index_id
                                                AND ixs.key_ordinal = 0
                                                AND ixs.is_included_column = 1
                                                AND ixs.partition_ordinal= 0
                                                AND ixs.column_store_order_ordinal >= 1
                                ) AS columnstore_keys(content)
                OUTER APPLY     (
                                        SELECT          COUNT(*) AS items,
                                                        STRING_AGG(CAST(l.content COLLATE DATABASE_DEFAULT AS VARCHAR(MAX)), ', ')
                                        FROM            (
                                                                SELECT          par.index_id,
                                                                                CONCAT('DATA_COMPRESSION = ', par.data_compression_desc, CASE WHEN dsp.type = 'PS' THEN CONCAT(' ON PARTITIONS (', STRING_AGG(par.partition_number, ', ') WITHIN GROUP (ORDER BY par.partition_number)) ELSE '' END, ')')
                                                                FROM            sys.partitions AS par
                                                                WHERE           par.object_id = ind.object_id
                                                                                AND par.index_id = ind.index_id
                                                                GROUP BY        par.index_id,
                                                                                par.data_compression_desc
                                                        ) AS l(index_id, content)
                                        GROUP BY        l.index_id
                                 ) AS compression_data(items, content)
        )
        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        cte.entity,
                        act.phase,
                        act.sql_text
        FROM            cte_indexes AS cte
        CROSS APPLY     (
                                SELECT  'crix' AS action_code,
                                        340 AS sort_order,
                                        9 AS phase,
                                        CASE
                                                -- Nonclustered hash index
                                                WHEN cte.index_type_major = 7 AND cte.is_unique = 1 THEN CONCAT('ALTER TABLE ', cte.table_name, ' ADD INDEX ', cte.index_name, ' UNIQUE NONCLUSTERED HASH (', cte.key_columns, ') WITH (', cte.bucket_count, ');')
                                                WHEN cte.index_type_major = 7 THEN CONCAT('ALTER TABLE ', cte.table_name, ' ADD INDEX ', cte.index_name, ' NONCLUSTERED HASH (', cte.key_columns, ') WITH (', cte.bucket_count, ');')
                                                -- Nonclustered columnstore index
                                                WHEN cte.index_type_major = 6 THEN CONCAT('CREATE NONCLUSTERED COLUMNSTORE INDEX ', cte.index_name, ' ON ', cte.table_name, ' (', cte.include_columns, ') WITH (', cte.drop_existing, ', ', cte.compression_delay, ', ', cte.data_compression, ')', cte.data_space_definition, ';')
                                                -- Clustered columnstore index
                                                WHEN cte.index_type_major = 5 AND cte.is_memory_optimized = 1 THEN CONCAT('ALTER TABLE ', cte.table_name, ' ADD INDEX ', cte.index_name, ' CLUSTERED COLUMNSTORE', CASE WHEN cte.column_store_order > '' THEN CONCAT(' ORDER(', cte.column_store_order, ')') ELSE '' END , ' WITH (', cte.compression_delay, ')', cte.data_space_definition, ';')
                                                WHEN cte.index_type_major = 5 THEN CONCAT('CREATE CLUSTERED COLUMNSTORE INDEX ', cte.index_name, ' ON ', cte.table_name, CASE WHEN cte.column_store_order > '' THEN CONCAT(' ORDER(', cte.column_store_order, ')') ELSE '' END, ' WITH (', cte.drop_existing, ', ', cte.compression_delay, ', ', cte.data_compression, ')', cte.data_space_definition, ';')
                                                -- Spatial index
                                                WHEN cte.index_type_major = 4 THEN CONCAT('CREATE SPATIAL INDEX ', cte.index_name, ' ON ', cte.table_name, ' (', cte.other_columns, ') USING ', QUOTENAME(cte.tessellation_scheme), ' WITH (', cte.bounding_box, ', ', cte.grids, ', ', cte.cells_per_object, ', ', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.drop_existing, ', ', cte.online, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ')', cte.data_space_definition, ';')
                                                -- XML primary index
                                                WHEN cte.index_type_major = 3 AND cte.index_type_minor = 0 THEN CONCAT('CREATE PRIMARY XML INDEX ', cte.index_name, ' ON ', cte.table_name, ' (', cte.other_columns, ') WITH (', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.drop_existing, ', ', cte.online, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ');')
                                                -- XML index
                                                WHEN cte.index_type_major = 3 AND cte.index_type_minor = 1 THEN CONCAT('CREATE XML INDEX ', cte.index_name, ' ON ', cte.table_name, ' (', cte.other_columns, ') USING XML INDEX ', QUOTENAME(cte.primary_xml_index_name), ' FOR ', QUOTENAME(cte.xml_type_desc), ' WITH (', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.drop_existing, ', ', cte.online, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ');')
                                                -- Nonclustered index
                                                WHEN cte.index_type_major = 2 AND cte.is_primary_key = 1 THEN CONCAT('ALTER TABLE ', cte.table_name, ' ADD CONSTRAINT ', cte.index_name, ' PRIMARY KEY NONCLUSTERED (', cte.key_columns, ')', CASE WHEN cte.include_columns > '' THEN ' INCLUDE (' + cte.include_columns + ')' ELSE '' END, cte.filter_definition, ' WITH (', cte.online, ', ', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.ignore_dup_key, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ', ', cte.fill_factor, ', ', cte.optimize_for_sequential_key, ')', cte.data_space_definition, ';')
                                                WHEN cte.index_type_major = 2 AND cte.is_unique_constraint = 1 THEN CONCAT('ALTER TABLE ', cte.table_name, ' ADD CONSTRAINT ', cte.index_name, ' UNIQUE NONCLUSTERED (', cte.key_columns, ')', CASE WHEN cte.include_columns > '' THEN ' INCLUDE (' + cte.include_columns + ')' ELSE '' END, cte.filter_definition, ' WITH (', cte.online, ', ', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.ignore_dup_key, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ', ', cte.fill_factor, ', ', cte.optimize_for_sequential_key, ')', cte.data_space_definition, ';')
                                                WHEN cte.index_type_major = 2 AND cte.is_unique = 1 THEN CONCAT('CREATE UNIQUE NONCLUSTERED INDEX ', cte.index_name, ' ON ', cte.table_name, ' (', cte.key_columns, ')', CASE WHEN cte.include_columns > '' THEN ' INCLUDE (' + cte.include_columns + ')' ELSE '' END, cte.filter_definition, ' WITH (', cte.online, ', ', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.ignore_dup_key, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ', ', cte.fill_factor, ', ', cte.optimize_for_sequential_key, ')', cte.data_space_definition, ';')
                                                WHEN cte.index_type_major = 2 THEN CONCAT('CREATE NONCLUSTERED INDEX ', cte.index_name, ' ON ', cte.table_name, ' (', key_columns, ')', CASE WHEN cte.include_columns > '' THEN ' INCLUDE (' + cte.include_columns + ')' ELSE '' END, cte.filter_definition, ' WITH (', cte.online, ', ', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.ignore_dup_key, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ', ', cte.fill_factor, ', ', cte.optimize_for_sequential_key, ')', cte.data_space_definition, ';')
                                                -- Clustered index
                                                WHEN cte.index_type_major = 1 AND cte.is_primary_key = 1 THEN CONCAT('ALTER TABLE ', cte.table_name, ' ADD CONSTRAINT ', cte.index_name, ' PRIMARY KEY CLUSTERED (', cte.key_columns, ') WITH (', cte.online, ', ', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.ignore_dup_key, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ', ', cte.fill_factor, ', ', cte.optimize_for_sequential_key, ')', cte.data_space_definition, ';')
                                                WHEN cte.index_type_major = 1 AND cte.is_unique_constraint = 1 THEN CONCAT('ALTER TABLE ', cte.table_name, ' ADD CONSTRAINT ', cte.index_name, ' UNIQUE CLUSTERED (', cte.key_columns, ') WITH (', cte.online, ', ', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.ignore_dup_key, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ', ', cte.fill_factor, ', ', cte.optimize_for_sequential_key, ')', cte.data_space_definition, ';')
                                                WHEN cte.index_type_major = 1 AND cte.is_unique = 1 THEN CONCAT('CREATE UNIQUE CLUSTERED INDEX ', cte.index_name, ' ON ', cte.table_name, ' (', cte.key_columns, ') WITH (', cte.online, ', ', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.ignore_dup_key, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ', ', cte.fill_factor, ', ', cte.optimize_for_sequential_key, ')', cte.data_space_definition, ';')
                                                WHEN cte.index_type_major = 1 THEN CONCAT('CREATE CLUSTERED INDEX ', cte.index_name, ' ON ', cte.table_name, ' (', cte.key_columns, ') WITH (', cte.online, ', ', cte.pad_index, ', ', cte.statistics_norecompute, ', ', cte.sort_in_tempdb, ', ', cte.ignore_dup_key, ', ', cte.allow_row_locks, ', ', cte.allow_page_locks, ', ', cte.fill_factor, ', ', cte.optimize_for_sequential_key, ')', cte.data_space_definition, ';')
                                                ELSE ''
                                        END AS sql_text

                                UNION ALL

                                SELECT  'drix',
                                        80 AS sort_order,
                                        3 AS phase,
                                        CASE
                                                WHEN cte.is_memory_optimized = 1 THEN CONCAT('ALTER TABLE ', cte.table_name, ' DROP INDEX ', cte.index_name, ');')
                                                WHEN 1 IN (cte.is_primary_key, cte.is_unique_constraint) THEN CONCAT('ALTER TABLE ', cte.table_name, ' DROP CONSTRAINT ', cte.index_name, ' WITH (', cte.online, ');')
                                                ELSE CONCAT('DROP INDEX ', cte.index_name, ' ON ', cte.table_name, ' WITH (', cte.online, ');')
                                        END
                        ) AS act (action_code, sort_order, phase, sql_text)
        ORDER BY        cte.page_count DESC
        OPTION          (RECOMPILE);

        -- entg = Enable table triggers
        -- ditg = Disable table triggers
        RAISERROR('Adding table trigger statements to queue...', 10, 1) WITH NOWAIT;

        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        wrk.entity,
                        act.phase,
                        act.sql_text
        FROM            (
                                SELECT DISTINCT ind.table_id,
                                                ind.entity,
                                                ind.table_name
                                FROM            #index_columns AS ind
                        ) AS wrk
        INNER JOIN      sys.triggers AS trg ON trg.parent_id = wrk.table_id
                                AND trg.is_ms_shipped = 0
        INNER JOIN      sys.sql_modules AS sqm ON sqm.object_id = trg.object_id
        CROSS APPLY     (
                                VALUES  (
                                                'ditg',
                                                20,
                                                1,
                                                CONCAT('ALTER TABLE ', wrk.table_name, ' DISABLE TRIGGER ', QUOTENAME(trg.name COLLATE DATABASE_DEFAULT), ';')
                                        ),
                                        (
                                                'entg',
                                                420,
                                                13,
                                                CONCAT('ALTER TABLE ', wrk.table_name, ' ENABLE TRIGGER ', QUOTENAME(trg.name COLLATE DATABASE_DEFAULT), ';')
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        WHERE           trg.parent_class_desc = 'OBJECT_OR_COLUMN'
        OPTION          (RECOMPILE);

        -- crfk = Create foreign keys
        -- drfk = Drop foreign keys
        RAISERROR('Adding foreign key statements to queue...', 10, 1) WITH NOWAIT;

        WITH cte_candidates(foreign_key_id)
        AS (
                SELECT  fkc.constraint_object_id AS foreign_key_id
                FROM    sys.foreign_key_columns AS fkc
                WHERE   EXISTS(SELECT * FROM #configurations AS cfg WHERE cfg.table_id = fkc.referenced_object_id)      -- Only on parent object to avoid incompatible schema locks
                        AND EXISTS(SELECT * FROM #configurations AS cfg WHERE cfg.table_id = fkc.parent_object_id)

                UNION

                SELECT  fkc.constraint_object_id AS foreign_key_id
                FROM    sys.foreign_key_columns AS fkc
                WHERE   EXISTS(SELECT * FROM #index_columns AS ixs WHERE ixs.table_id = fkc.parent_object_id AND ixs.column_id = fkc.parent_column_id AND ixs.is_unique = 1)
                        OR EXISTS(SELECT * FROM #index_columns AS ixs WHERE ixs.table_id = fkc.referenced_object_id AND ixs.column_id = fkc.referenced_column_id AND ixs.is_unique = 1)

                UNION

                SELECT  fkc.constraint_object_id AS foreign_key_id
                FROM    sys.foreign_key_columns AS fkc
                WHERE   @database_collation_name IS NOT NULL
                        AND     (
                                        EXISTS(SELECT * FROM sys.computed_columns AS cc WHERE cc.object_id = fkc.referenced_object_id AND cc.column_id = fkc.referenced_column_id)
                                        OR EXISTS(SELECT * FROM sys.computed_columns AS cc WHERE cc.object_id = fkc.parent_object_id AND cc.column_id = fkc.parent_column_id)
                                )
        )
        INSERT          #foreign_keys
                        (
                                id,
                                name,
                                delete_action,
                                update_action,
                                referenced_table_name,
                                parent_table_name,
                                entity,
                                page_count
                        )
        SELECT          fk.object_id AS id,
                        QUOTENAME(fk.name COLLATE DATABASE_DEFAULT) AS name,
                        CASE
                                WHEN fk.delete_referential_action = 1 THEN 'ON DELETE CASCADE'
                                WHEN fk.delete_referential_action = 2 THEN 'ON DELETE SET NULL'
                                WHEN fk.delete_referential_action = 3 THEN 'ON DELETE SET DEFAULT'
                                ELSE 'ON DELETE NO ACTION'
                        END AS delete_action,
                        CASE
                                WHEN fk.update_referential_action = 1 THEN 'ON UPDATE CASCADE'
                                WHEN fk.update_referential_action = 2 THEN 'ON UPDATE SET NULL'
                                WHEN fk.update_referential_action = 3 THEN 'ON UPDATE SET DEFAULT'
                                ELSE 'ON UPDATE NO ACTION'
                        END AS update_action,
                        CONCAT(QUOTENAME(SCHEMA_NAME(ref.schema_id) COLLATE DATABASE_DEFAULT), '.', QUOTENAME(ref.name COLLATE DATABASE_DEFAULT)) AS referenced_table_name,
                        CONCAT(QUOTENAME(SCHEMA_NAME(par.schema_id) COLLATE DATABASE_DEFAULT), '.', QUOTENAME(par.name COLLATE DATABASE_DEFAULT)) AS parent_table_name,
                        CONCAT(SCHEMA_NAME(par.schema_id) COLLATE DATABASE_DEFAULT, '.', par.name COLLATE DATABASE_DEFAULT) AS entity,
                        COALESCE(pc1.page_count, 1E) * COALESCE(pc2.page_count, 1E) AS page_count
        FROM            cte_candidates AS cte
        INNER JOIN      sys.foreign_keys AS fk ON fk.object_id = cte.foreign_key_id
        INNER JOIN      sys.objects AS ref ON ref.object_id = fk.referenced_object_id
        INNER JOIN      sys.objects AS par ON par.object_id = fk.parent_object_id
        LEFT JOIN       #page_counts AS pc1 ON pc1.object_id = fk.referenced_object_id
        LEFT JOIN       #page_counts AS pc2 ON pc2.object_id = fk.parent_object_id
        OPTION          (RECOMPILE);

        UPDATE          fk
        SET             fk.referenced_columns = ref.column_names,
                        fk.parent_columns = par.column_names
        FROM            #foreign_keys AS fk
        CROSS APPLY     (
                                SELECT          STRING_AGG(CAST(QUOTENAME(col.name COLLATE DATABASE_DEFAULT) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS column_names
                                FROM            sys.foreign_key_columns AS fkc
                                INNER JOIN      sys.columns AS col ON col.object_id = fkc.referenced_object_id
                                                        AND col.column_id = fkc.referenced_column_id
                                WHERE           fkc.constraint_object_id = fk.id
                        ) AS ref(column_names)
        CROSS APPLY     (
                                SELECT          STRING_AGG(CAST(QUOTENAME(col.name COLLATE DATABASE_DEFAULT) AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS column_names
                                FROM            sys.foreign_key_columns AS fkc
                                INNER JOIN      sys.columns AS col ON col.object_id = fkc.parent_object_id
                                                        AND col.column_id = fkc.parent_column_id
                                WHERE           fkc.constraint_object_id = fk.id
                        ) AS par(column_names)
        OPTION          (RECOMPILE);

        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        fk.entity,
                        act.phase,
                        act.sql_text
        FROM            #foreign_keys AS fk
        CROSS APPLY     (
                                VALUES  (
                                                'drfk',
                                                50,
                                                2,
                                                CONCAT('ALTER TABLE ', fk.parent_table_name, ' DROP CONSTRAINT ', fk.name, ';')
                                        ),
                                        (
                                                'crfk',
                                                370,
                                                10,
                                                CONCAT('ALTER TABLE ', fk.parent_table_name, ' WITH CHECK ADD CONSTRAINT ', fk.name, ' FOREIGN KEY (', fk.parent_columns, ') REFERENCES ', fk.referenced_table_name, ' (', fk.referenced_columns, ') ', fk.delete_action, ' ', fk.update_action, ';')
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        ORDER BY        fk.page_count DESC
        OPTION          (RECOMPILE);

        -- crvw = Create view
        -- drvw = Drop view
        RAISERROR('Adding view statements to queue...', 10, 1) WITH NOWAIT;

        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        CONCAT(sch.name COLLATE DATABASE_DEFAULT, '.', vw.name COLLATE DATABASE_DEFAULT) AS entity,
                        act.phase,
                        act.sql_text
        FROM            #dependencies AS dep
        INNER JOIN      sys.views AS vw ON vw.object_id = dep.object_id
        INNER JOIN      sys.sql_modules AS sqm ON sqm.object_id = vw.object_id
        INNER JOIN      sys.schemas AS sch ON sch.schema_id = vw.schema_id
        CROSS APPLY     (
                                VALUES  (
                                                'drvw',
                                                90,
                                                3,
                                                CONCAT('DROP VIEW ', QUOTENAME(sch.name COLLATE DATABASE_DEFAULT), '.', QUOTENAME(vw.name COLLATE DATABASE_DEFAULT), ';')
                                        ),
                                        (
                                                'crvw',
                                                330,
                                                9,
                                                CONCAT(sqm.definition COLLATE DATABASE_DEFAULT, ';')
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        OPTION          (RECOMPILE);

        -- crfn = Create function
        -- drfn = Drop function
        RAISERROR('Adding function statements to queue...', 10, 1) WITH NOWAIT;

        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        CONCAT(sch.name COLLATE DATABASE_DEFAULT, '.', obj.name COLLATE DATABASE_DEFAULT) AS entity,
                        act.phase,
                        act.sql_text
        FROM            (
                                SELECT  dep.object_id
                                FROM    #dependencies AS dep

                                UNION

                                SELECT          col.object_id
                                FROM            sys.columns AS col
                                INNER JOIN      sys.types AS usr ON usr.user_type_id = col.user_type_id
                                                        AND usr.is_user_defined = 1
                                WHERE           col.collation_name COLLATE DATABASE_DEFAULT <> @database_collation_name
                        ) AS wrk
        INNER JOIN      sys.objects AS obj ON obj.object_id = wrk.object_id
                                AND obj.type COLLATE DATABASE_DEFAULT = 'TF'
        INNER JOIN      sys.sql_modules AS sqm ON sqm.object_id = obj.object_id
        INNER JOIN      sys.schemas AS sch ON sch.schema_id = obj.schema_id
        CROSS APPLY     (
                                VALUES  (
                                                'drfn',
                                                100,
                                                3,
                                                CONCAT('DROP FUNCTION ', QUOTENAME(sch.name COLLATE DATABASE_DEFAULT), '.', QUOTENAME(obj.name COLLATE DATABASE_DEFAULT), ';')
                                        ),
                                        (
                                                'crfn',
                                                320,
                                                9,
                                                CONCAT(sqm.definition COLLATE DATABASE_DEFAULT, ';')
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        OPTION          (RECOMPILE);

        -- crst = Create user defined statistics
        -- drst = Drop user defined statistics
        WITH cte_statistics(entity, drop_definition, create_definition, page_count)
        AS (
                SELECT          CONCAT(sch.name COLLATE DATABASE_DEFAULT, '.', tbl.name COLLATE DATABASE_DEFAULT) AS entity,
                                CONCAT('DROP STATISTICS ', QUOTENAME(sch.name) COLLATE DATABASE_DEFAULT, '.', QUOTENAME(tbl.name) COLLATE DATABASE_DEFAULT, '.', QUOTENAME(sts.name) COLLATE DATABASE_DEFAULT, ';') AS drop_definition,
                                CONCAT('CREATE STATISTICS ', QUOTENAME(sts.name) COLLATE DATABASE_DEFAULT, ' ON ', QUOTENAME(sch.name) COLLATE DATABASE_DEFAULT, '.', QUOTENAME(tbl.name) COLLATE DATABASE_DEFAULT, ' (', col.content, ')', CASE WHEN sts.has_filter = 1 THEN CONCAT(' WHERE ', sts.filter_definition) ELSE NULL END, ' WITH INCREMENTAL = ', CASE WHEN sts.is_incremental = 1 THEN 'ON' ELSE 'OFF' END, CASE WHEN sts.no_recompute = 1 THEN ', NORECOMPUTE' ELSE '' END, CASE WHEN sts.has_persisted_sample = 1 THEN ', PERSIST_SAMPLE_PERCENT = ON' ELSE NULL END, CASE WHEN f.persisted_sample_percent = 0 THEN ', FULLSCAN' WHEN ROUND(f.persisted_sample_percent, 0) = f.persisted_sample_percent THEN CONCAT(', SAMPLE ', ROUND(f.persisted_sample_percent, 0), ' PERCENT') ELSE CONCAT(', SAMPLE ', ROUND(f.persisted_sample_percent * f.rows_sampled / 100, 0), ' ROWS') END, ';') AS create_definition,
                                COALESCE(pc.page_count, 0) AS page_count
                FROM            sys.stats AS sts
                INNER JOIN      sys.tables AS tbl ON tbl.object_id = sts.object_id
                INNER JOIN      sys.schemas AS sch ON sch.schema_id = tbl.schema_id
                CROSS APPLY     (
                                        SELECT          STRING_AGG(CAST(QUOTENAME(col.name) COLLATE DATABASE_DEFAULT AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY stc.stats_column_id)
                                        FROM            sys.stats_columns AS stc
                                        INNER JOIN      sys.columns AS col ON col.object_id = stc.object_id
                                                                AND col.column_id = stc.column_id
                                        LEFT JOIN       #configurations AS cfg ON cfg.table_id = stc.object_id
                                                                AND cfg.column_id = stc.column_id
                                        WHERE           stc.object_id = sts.object_id
                                                        AND stc.stats_id = sts.stats_id
                                        HAVING          MAX(CASE WHEN cfg.table_id IS NULL THEN 0 ELSE 1 END) = 1
                                ) AS col(content)
                CROSS APPLY     sys.dm_db_stats_properties(sts.object_id, sts.stats_id) AS f
                LEFT JOIN       #page_counts AS pc ON pc.object_id = tbl.object_id
                WHERE           sts.user_created = 1
                                AND col.content IS NOT NULL
        )
        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        cte.entity,
                        act.phase,
                        act.sql_text
        FROM            cte_statistics AS cte
        CROSS APPLY     (
                                VALUES  (
                                                'drst',
                                                60,
                                                3,
                                                cte.drop_definition
                                        ),
                                        (
                                                'crst',
                                                360,
                                                9,
                                                cte.create_definition
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        ORDER BY        cte.page_count DESC
        OPTION          (RECOMPILE);

        -- crck = Create table check constraint
        -- drck = Drop table check constraint
        RAISERROR('Adding table check constraint statements to queue...', 10, 1) WITH NOWAIT;

        WITH cte_check_constraints(table_name, entity, check_constraint_name, check_definition, page_count)
        AS (
                SELECT DISTINCT CONCAT(QUOTENAME(sch.name COLLATE DATABASE_DEFAULT), '.', QUOTENAME(tbl.name COLLATE DATABASE_DEFAULT)) AS table_name,
                                CONCAT(sch.name COLLATE DATABASE_DEFAULT, '.', tbl.name COLLATE DATABASE_DEFAULT) AS entity,
                                QUOTENAME(chc.name COLLATE DATABASE_DEFAULT) AS check_constraint_name,
                                CASE
                                        WHEN wrk.new_column_name > '' THEN REPLACE(chc.definition COLLATE DATABASE_DEFAULT, QUOTENAME(wrk.column_name), QUOTENAME(wrk.new_column_name))
                                        ELSE chc.definition COLLATE DATABASE_DEFAULT
                                END AS check_definition,
                                COALESCE(pc.page_count, 0) AS page_count
                FROM            sys.check_constraints AS chc
                INNER JOIN      sys.objects AS tbl ON tbl.object_id = chc.parent_object_id
                                        AND tbl.type COLLATE DATABASE_DEFAULT = 'U'
                INNER JOIN      sys.schemas AS sch ON sch.schema_id = tbl.schema_id
                LEFT JOIN       #configurations AS wrk ON wrk.table_id = chc.parent_object_id
                LEFT JOIN       sys.columns AS col ON col.object_id = tbl.object_id
                                        AND col.name COLLATE DATABASE_DEFAULT = wrk.column_name
                LEFT JOIN       #page_counts AS pc ON pc.object_id = tbl.object_id
                WHERE           (
                                        col.column_id = chc.parent_column_id
                                        OR CHARINDEX(QUOTENAME(wrk.column_name), chc.definition COLLATE DATABASE_DEFAULT) >= 1
                                )
                                OR @database_collation_name IS NOT NULL
        )
        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        cte.entity,
                        act.phase,
                        act.sql_text
        FROM            cte_check_constraints AS cte
        CROSS APPLY     (
                                VALUES  (
                                                'drck',
                                                120,
                                                3,
                                                CONCAT('ALTER TABLE ', cte.table_name, ' DROP CONSTRAINT ', cte.check_constraint_name, ';')
                                        ),
                                        (
                                                'crck',
                                                300,
                                                9,
                                                CONCAT('ALTER TABLE ', cte.table_name, ' WITH CHECK ADD CONSTRAINT ', cte.check_constraint_name, ' CHECK ', cte.check_definition, ';')
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        ORDER BY        cte.page_count DESC
        OPTION          (RECOMPILE);

        -- crdk = Create table default constraint
        -- drdk = Drop table default constraint
        RAISERROR('Adding table default constraint statements to queue...', 10, 1) WITH NOWAIT;

        WITH cte_default_constraints(table_name, entity, column_name, default_constraint_name, default_definition, page_count)
        AS (
                SELECT DISTINCT CONCAT(QUOTENAME(sch.name COLLATE DATABASE_DEFAULT), '.', QUOTENAME(tbl.name COLLATE DATABASE_DEFAULT)) AS table_name,
                                wrk.table_name AS entity,
                                QUOTENAME(col.name COLLATE DATABASE_DEFAULT) AS column_name,
                                QUOTENAME(dfc.name COLLATE DATABASE_DEFAULT) AS default_constraint_name,
                                CASE
                                        WHEN col.name COLLATE DATABASE_DEFAULT = wrk.column_name AND wrk.new_column_name > '' THEN REPLACE(dfc.definition COLLATE DATABASE_DEFAULT, QUOTENAME(wrk.column_name), QUOTENAME(wrk.new_column_name))
                                        ELSE dfc.definition COLLATE DATABASE_DEFAULT
                                END AS default_definition,
                                COALESCE(pc.page_count, 0) AS page_count
                FROM            sys.default_constraints AS dfc
                INNER JOIN      #configurations AS wrk ON wrk.table_id = dfc.parent_object_id
                INNER JOIN      sys.objects AS tbl ON tbl.object_id = dfc.parent_object_id
                                        AND tbl.type COLLATE DATABASE_DEFAULT = 'U'
                INNER JOIN      sys.schemas AS sch ON sch.schema_id = tbl.schema_id
                INNER JOIN      sys.columns AS col ON col.object_id = dfc.parent_object_id
                                        AND col.column_id = dfc.parent_column_id
                                        AND col.name COLLATE DATABASE_DEFAULT = wrk.column_name
                LEFT JOIN       #page_counts AS pc ON pc.object_id = tbl.object_id
                WHERE           dfc.is_ms_shipped = 0
        )
        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        cte.entity,
                        act.phase,
                        act.sql_text
        FROM            cte_default_constraints AS cte
        CROSS APPLY     (
                                VALUES  (
                                                'drdk',
                                                130,
                                                3,
                                                CONCAT('ALTER TABLE ', cte.table_name, ' DROP CONSTRAINT ', cte.default_constraint_name, ';')
                                        ),
                                        (
                                                'crdk',
                                                290,
                                                9,
                                                CONCAT('ALTER TABLE ', cte.table_name, ' ADD CONSTRAINT ', cte.default_constraint_name, ' DEFAULT ', cte.default_definition, ' FOR ', cte.column_name, ';')
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        ORDER BY        cte.page_count DESC
        OPTION          (RECOMPILE);

        -- drcc = Drop Computed Columns
        -- crcc = Create Computed Columns
        RAISERROR('Adding computed column statements to queue...', 10, 1) WITH NOWAIT;

        WITH cte_computed_columns(table_name, entity, column_name, is_persisted, computed_definition, page_count)
        AS (
                SELECT DISTINCT CONCAT(QUOTENAME(sch.name COLLATE DATABASE_DEFAULT), '.', QUOTENAME(tbl.name COLLATE DATABASE_DEFAULT)) AS table_name,
                                CONCAT(sch.name COLLATE DATABASE_DEFAULT, '.', tbl.name COLLATE DATABASE_DEFAULT) AS entity,
                                QUOTENAME(col.name COLLATE DATABASE_DEFAULT) AS column_name,
                                col.is_persisted,
                                CASE
                                        WHEN wrk.new_column_name > '' THEN REPLACE(col.definition COLLATE DATABASE_DEFAULT, QUOTENAME(wrk.column_name), QUOTENAME(wrk.new_column_name))
                                        ELSE col.definition COLLATE DATABASE_DEFAULT
                                END AS computed_definition,
                                COALESCE(pc.page_count, 0) AS page_count
                FROM            sys.computed_columns AS col
                INNER JOIN      sys.objects AS tbl ON tbl.object_id = col.object_id
                                        AND tbl.type COLLATE DATABASE_DEFAULT = 'U'
                INNER JOIN      sys.schemas AS sch ON sch.schema_id = tbl.schema_id
                LEFT JOIN       #configurations AS wrk ON wrk.table_id = col.object_id
                LEFT JOIN       #page_counts AS pc ON pc.object_id = tbl.object_id
                WHERE           (
                                        wrk.column_id = col.column_id
                                        OR CHARINDEX(QUOTENAME(wrk.column_name), col.definition COLLATE DATABASE_DEFAULT) >= 1
                                )
                                OR @database_collation_name IS NOT NULL
        )
        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        cte.entity,
                        act.phase,
                        act.sql_text
        FROM            cte_computed_columns AS cte
        CROSS APPLY     (
                                VALUES  (
                                                'drcc',
                                                140,
                                                3,
                                                CONCAT('ALTER TABLE ', cte.table_name, ' DROP COLUMN ', cte.column_name, ';')
                                        ),
                                        (
                                                'crcc',
                                                280,
                                                9,
                                                CONCAT('ALTER TABLE ', cte.table_name, ' ADD ', cte.column_name, ' AS ', cte.computed_definition, CASE WHEN cte.is_persisted = 1 THEN ' PERSISTED;' ELSE ';' END)
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        ORDER BY        cte.page_count DESC
        OPTION          (RECOMPILE);

        -- undf = Unbind column default
        -- bidf = Bind column default
        RAISERROR('Adding datatype column default statements to queue...', 10, 1) WITH NOWAIT;

        WITH cte_defaults(table_name, entity, column_name, new_column_name, datatype_default_name, default_definition)
        AS (
                SELECT DISTINCT CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(tbl.object_id)), '.', QUOTENAME(tbl.name COLLATE DATABASE_DEFAULT)) AS table_name,
                                cfg.table_name AS entity,
                                QUOTENAME(col.name COLLATE DATABASE_DEFAULT) AS column_name,
                                cfg.new_column_name,
                                cfg.datatype_default_name,
                                sqm.definition COLLATE DATABASE_DEFAULT AS default_definition
                FROM            sys.columns AS col
                INNER JOIN      #configurations AS cfg ON cfg.table_id = col.object_id
                                        AND cfg.column_name = col.name COLLATE DATABASE_DEFAULT
                INNER JOIN      sys.objects AS tbl ON tbl.object_id = col.object_id
                                        AND tbl.type COLLATE DATABASE_DEFAULT = 'U'
                LEFT JOIN       sys.objects AS def ON def.object_id = col.default_object_id
                                        AND def.type COLLATE DATABASE_DEFAULT = 'D'
                LEFT JOIN       sys.sql_modules AS sqm ON sqm.object_id = def.object_id
        )
        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        cte.entity,
                        act.phase,
                        act.sql_text
        FROM            cte_defaults AS cte
        CROSS APPLY     (
                                VALUES  (
                                                'undf',
                                                CASE WHEN cte.default_definition >= '' THEN 150 ELSE NULL END,
                                                3,
                                                CONCAT('EXEC sys.sp_unbindefault @objname = ''', REPLACE(CONCAT(cte.table_name, '.', cte.column_name), '''', ''''''), ''';')
                                        ),
                                        (
                                                'bidf',
                                                CASE WHEN cte.datatype_default_name > '' THEN 270 ELSE NULL END,
                                                9,
                                                CONCAT('EXEC sys.sp_bindefault @defname = ', QUOTENAME(cte.datatype_default_name, ''''), ', @objname = ''', REPLACE(CONCAT(cte.table_name, '.', CASE WHEN cte.new_column_name > '' THEN QUOTENAME(cte.new_column_name) ELSE cte.column_name END), '''', ''''''), ''';')
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        WHERE           act.sort_order IS NOT NULL
        OPTION          (RECOMPILE);

        -- unru = Unbind column rule
        -- biru = Bind column rule
        RAISERROR('Adding datatype column rule statements to queue...', 10, 1) WITH NOWAIT;

        WITH cte_rules(table_name, entity, column_name, new_column_name, datatype_rule_name, rule_definition)
        AS (
                SELECT DISTINCT CONCAT(QUOTENAME(SCHEMA_NAME(tbl.schema_id)), '.', QUOTENAME(tbl.name COLLATE DATABASE_DEFAULT)) AS table_name,
                                CONCAT(SCHEMA_NAME(tbl.schema_id) COLLATE DATABASE_DEFAULT, '.', tbl.name COLLATE DATABASE_DEFAULT) AS table_name,
                                QUOTENAME(col.name COLLATE DATABASE_DEFAULT) AS column_name,
                                QUOTENAME(cfg.new_column_name) AS new_column_name,
                                cfg.datatype_rule_name,
                                sqm.definition COLLATE DATABASE_DEFAULT AS rule_definition
                FROM            sys.columns AS col
                INNER JOIN      #configurations AS cfg ON cfg.table_id = col.object_id
                                        AND cfg.column_name = col.name COLLATE DATABASE_DEFAULT
                INNER JOIN      sys.objects AS tbl ON tbl.object_id = col.object_id
                                        AND tbl.type COLLATE DATABASE_DEFAULT = 'U'
                LEFT JOIN       sys.objects AS rul ON rul.object_id = col.rule_object_id
                                        AND rul.type COLLATE DATABASE_DEFAULT = 'R'
                LEFT JOIN       sys.sql_modules AS sqm ON sqm.object_id = rul.object_id
                WHERE           col.rule_object_id <> 0
        )
        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          act.action_code,
                        'L' AS status_code,
                        act.sort_order,
                        cte.entity,
                        act.phase,
                        act.sql_text
        FROM            cte_rules AS cte
        CROSS APPLY     (
                                VALUES  (
                                                'unru',
                                                CASE WHEN cte.rule_definition >= '' THEN 170 ELSE NULL END,
                                                3,
                                                CONCAT('EXEC sys.sp_unbindrule @objname = ''', REPLACE(CONCAT(cte.table_name, '.', cte.column_name), '''', ''''''), ''';')
                                        ),
                                        (
                                                'biru',
                                                CASE WHEN cte.datatype_rule_name > '' THEN 250 ELSE NULL END,
                                                9,
                                                CONCAT('EXEC sys.sp_bindrule @rulename = ', QUOTENAME(cte.datatype_rule_name), ', @objname = ''', REPLACE(CONCAT(cte.table_name, '.', CASE WHEN cte.new_column_name > '' THEN cte.new_column_name ELSE cte.column_name END), '''', ''''''), ''';')
                                        )
                        ) AS act(action_code, sort_order, phase, sql_text)
        WHERE           act.sort_order IS NOT NULL
        OPTION          (RECOMPILE);

        -- aldb = Alter database
        RAISERROR('Adding alter database statements to queue...', 10, 1) WITH NOWAIT;

        IF @database_collation_name IS NOT NULL
                BEGIN
                        INSERT  tools.atac_queue
                                (
                                        action_code,
                                        status_code,
                                        sort_order,
                                        phase,
                                        entity,
                                        sql_text
                                )
                        SELECT  'aldb' AS action_code,
                                'L' AS status_code,
                                200 AS sort_order,
                                5 AS phase,
                                DB_NAME() AS entity,
                                CONCAT(N'DECLARE @sql NVARCHAR(MAX); SELECT @sql = STRING_AGG(CAST(CONCAT(N''KILL '', es.session_id, N'';'') AS NVARCHAR(MAX)), N'' '') WITHIN GROUP (ORDER BY es.session_id) FROM sys.dm_exec_sessions AS es WHERE es.database_id = DB_ID() AND es.session_id <> @@SPID AND es.is_user_process = 1; EXEC (@sql); ALTER DATABASE CURRENT COLLATE ', QUOTENAME(@database_collation_name), ';') AS sql_text
                        OPTION  (RECOMPILE);
                END;

        -- alco = Alter column
        RAISERROR('Adding alter column statements to queue...', 10, 1) WITH NOWAIT;

        WITH cte_column(table_name, entity, column_name, is_nullable, datatype_name, max_length, precision_and_scale, collation_name, xml_collection_name, page_count)
        AS (
                SELECT          CONCAT(QUOTENAME(PARSENAME(cfg.table_name, 2)), '.', QUOTENAME(PARSENAME(cfg.table_name, 1))) AS table_name,
                                cfg.table_name AS entity,
                                QUOTENAME(cfg.column_name) AS column_name,
                                CASE
                                        WHEN cfg.is_nullable = 'true' THEN ' NULL'
                                        ELSE ' NOT NULL'
                                END AS is_nullable,
                                QUOTENAME(cfg.datatype_name) AS datatype_name,
                                CASE
                                        WHEN cfg.max_length IS NULL THEN ''
                                        ELSE CONCAT('(', cfg.max_length, ')')
                                END AS max_length,
                                CASE
                                        WHEN cfg.precision IS NULL AND cfg.scale IS NULL THEN ''
                                        WHEN cfg.precision IS NULL THEN CONCAT('(', cfg.scale, ')')
                                        ELSE CONCAT('(', cfg.precision, ', ', cfg.scale, ')')
                                END AS precision_and_scale,
                                CASE
                                        WHEN cfg.collation_name > '' THEN CONCAT(' COLLATE ', cfg.collation_name)
                                        ELSE ''
                                END AS collation_name,
                                CASE
                                        WHEN cfg.xml_collection_name > '' THEN CONCAT('(', QUOTENAME(PARSENAME(cfg.xml_collection_name, 2)), '.', QUOTENAME(PARSENAME(cfg.xml_collection_name, 1)), ')')
                                        ELSE ''
                                END AS xml_collection_name,
                                COALESCE(pc.page_count, 0) AS page_count
                FROM            #configurations AS cfg
                LEFT JOIN       #page_counts AS pc ON pc.object_id = cfg.table_id
                WHERE           cfg.is_computed = 0
        )
        INSERT          tools.atac_queue
                        (
                                action_code,
                                status_code,
                                sort_order,
                                entity,
                                phase,
                                sql_text
                        )
        SELECT          'alco' AS action_code,
                        'L' AS status_code,
                        210 AS sort_order,
                        cte.entity,
                        6 AS phase,
                        CONCAT('ALTER TABLE ', cte.table_name, ' ALTER COLUMN ', cte.column_name, ' ', cte.datatype_name, cte.max_length, cte.precision_and_scale, cte.collation_name, cte.xml_collection_name, cte.is_nullable, ';') AS sql_text
        FROM            cte_column AS cte
        ORDER BY        cte.page_count DESC
        OPTION          (RECOMPILE);

        -- reco = Rename a column
        RAISERROR('Adding column rename statements to queue...', 10, 1) WITH NOWAIT;

        WITH cte_column(table_name, entity, column_name, new_column_name)
        AS (
                SELECT DISTINCT CONCAT(QUOTENAME(PARSENAME(cfg.table_name, 2)), '.', QUOTENAME(PARSENAME(cfg.table_name, 1))) AS table_name,
                                cfg.table_name AS table_name,
                                QUOTENAME(cfg.column_name) AS column_name,
                                QUOTENAME(cfg.new_column_name, '''') AS new_column_name
                FROM            #configurations AS cfg
                WHERE           cfg.new_column_name > ''
        )
        INSERT  tools.atac_queue
                (
                        action_code,
                        status_code,
                        sort_order,
                        entity,
                        phase,
                        sql_text
                )
        SELECT  'reco' AS action_code,
                'L' AS status_code,
                220 sort_order,
                cte.entity,
                7 AS phase,
                CONCAT('EXEC sys.sp_rename @objname = ''', REPLACE(CONCAT(cte.table_name, '.', cte.column_name), '''', ''''''), ''', @newname = ', cte.new_column_name, ', @objtype = ''COLUM'';') AS sql_text
        FROM    cte_column AS cte
        OPTION  (RECOMPILE);

        -- cltb = Clean tables
        IF @execute_cleantable_check = 1
                BEGIN
                        RAISERROR('Adding clean table statements to queue...', 10, 1) WITH NOWAIT;

                        WITH cte_tables(entity, sql_text, page_count)
                        AS (
                                        SELECT DISTINCT cfg.table_name AS entity,
                                                        CONCAT('DBCC CLEANTABLE(', QUOTENAME(DB_NAME()), ', ''', REPLACE(cfg.table_name, '''', ''''''), ''');') AS sql_text,
                                                        COALESCE(pc.page_count, 0) AS page_count
                                        FROM            #configurations AS cfg
                                        INNER JOIN      sys.columns AS col ON col.object_id = cfg.table_id
                                                                AND col.column_id = cfg.column_id
                                        INNER JOIN      sys.types AS typ ON typ.user_type_id = col.system_type_id
                                                                AND typ.name IN ('varchar', 'nvarchar', 'varbinary', 'text', 'ntext', 'image', 'sql_variant', 'xml')
                                        LEFT JOIN       #page_counts AS pc ON pc.object_id = col.object_id
                        )
                        INSERT          tools.atac_queue
                                        (
                                                action_code,
                                                status_code,
                                                sort_order,
                                                entity,
                                                phase,
                                                sql_text
                                        )
                        SELECT          'cltb' AS action_code,
                                        'L' AS status_code,
                                        390 AS sort_order,
                                        cte.entity,
                                        11 AS phase,
                                        cte.sql_text
                        FROM            cte_tables AS cte
                        ORDER BY        cte.page_count DESC
                        OPTION          (RECOMPILE);
                END;

        -- remo = Refresh modules
        IF @execute_refreshmodule_check = 1
                BEGIN
                        RAISERROR('Adding module refresh statements to queue...', 10, 1) WITH NOWAIT;
                                                
                        INSERT          tools.atac_queue
                                        (
                                                action_code,
                                                status_code,
                                                sort_order,
                                                entity,
                                                phase,
                                                sql_text
                                        )
                        SELECT          'remo' AS action_code,
                                        'L' AS status_code,
                                        400 AS sort_order,
                                        CONCAT(SCHEMA_NAME(obj.schema_id) COLLATE DATABASE_DEFAULT, '.', obj.name COLLATE DATABASE_DEFAULT) AS entity,
                                        12 AS phase,
                                        CONCAT('EXEC sys.sp_refreshsqlmodule @name = ''', REPLACE(CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(dep.object_id) COLLATE DATABASE_DEFAULT), '.', QUOTENAME(OBJECT_NAME(dep.object_id) COLLATE DATABASE_DEFAULT)), '''', ''''''), ''';') AS sql_text
                        FROM            #dependencies AS dep
                        INNER JOIN      sys.objects AS obj ON obj.object_id = dep.object_id
                        ORDER BY        dep.level,
                                        CONCAT(QUOTENAME(OBJECT_SCHEMA_NAME(dep.object_id) COLLATE DATABASE_DEFAULT), '.', QUOTENAME(OBJECT_NAME(dep.object_id) COLLATE DATABASE_DEFAULT))
                        OPTION          (RECOMPILE);
                END;

        -- Sort statements in proper processing order
        WITH cte_duplicates(rnk)
        AS (
                SELECT  ROW_NUMBER() OVER (PARTITION BY taq.action_code, taq.entity, taq.sql_text ORDER BY taq.queue_id) AS rnk
                FROM    tools.atac_queue AS taq
        )
        DELETE  cte
        FROM    cte_duplicates AS cte
        WHERE   cte.rnk >= 2;

        WITH cte_sort(statement_id, rnk)
        AS (
                SELECT  taq.statement_id,
                        ROW_NUMBER() OVER (ORDER BY taq.sort_order, taq.queue_id) AS rnk
                FROM    tools.atac_queue AS taq
        )
        UPDATE  cte
        SET     cte.statement_id = cte.rnk
        FROM    cte_sort AS cte
        WHERE   cte.statement_id <> cte.rnk;

        ALTER INDEX ALL ON tools.atac_queue REBUILD WITH (FILLFACTOR = 100, DATA_COMPRESSION = NONE);

        /*
                Prepare SQL Agent jobs
        */

        -- Refresh SQL Agent jobs
        RAISERROR('Setting up SQL Agent jobs...', 10, 1) WITH NOWAIT;

        -- Clean up
        SELECT  @sql = STRING_AGG(CAST(CONCAT('EXEC msdb.dbo.sp_delete_job @job_name = ', QUOTENAME(jbs.name COLLATE DATABASE_DEFAULT, ''''), ', @delete_unused_schedule = 1;') AS VARCHAR(MAX)), '; ') WITHIN GROUP (ORDER BY jbs.job_id)
        FROM    msdb.dbo.sysjobs AS jbs
        WHERE   jbs.name COLLATE DATABASE_DEFAULT LIKE 'ATAC - Process%';

        EXEC    (@sql);

        IF @use_sql_agent = 1
                BEGIN
                        -- Prepare category
                        SET     @sql = 'IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name = ''Database Maintenance'' AND category_class = 1) BEGIN EXEC msdb.dbo.sp_add_category @class = ''JOB'', @type= ''LOCAL'', @name = ''Database Maintenance''; END;';

                        EXEC    (@sql);

                        -- Setup jobs
                        SET     @curr_id = 1;

                        WHILE @curr_id <= @number_of_processes
                                BEGIN
                                        SET     @sql = CONCAT('DECLARE @jobid BINARY(16); EXEC msdb.dbo.sp_add_job @job_name = ''ATAC - Process ', @curr_id, ' of ', @number_of_processes, ''', @enabled = 1, @notify_level_eventlog = 2, @notify_level_email = 0, @notify_level_netsend = 0, @notify_level_page = 0, @delete_level = 1, @category_name = ''Database Maintenance'', @owner_login_name = ', QUOTENAME(ORIGINAL_LOGIN(), ''''), ', @job_id = @jobid OUTPUT; EXEC msdb.dbo.sp_add_jobstep @job_id = @jobid, @step_name = ''Process'', @step_id = 1, @cmdexec_success_code = 0, @on_success_action = 1, @on_success_step_id = 0, @on_fail_action = 2, @on_fail_step_id = 0, @retry_attempts = 10, @retry_interval = 1, @os_run_priority = 0, @subsystem = ''TSQL'', @command = ''EXEC tools.usp_atac_process @process_statements = ', @process_statements, ', @maximum_retry_count = ', @maximum_retry_count,', @wait_time = ''', QUOTENAME(@wait_time, ''''), ''';'', @database_name = ', QUOTENAME(DB_NAME(), ''''), ', @flags = 0; EXEC msdb.dbo.sp_update_job @job_id = @jobid, @start_step_id = 1; EXEC msdb.dbo.sp_add_jobserver @job_id = @jobid, @server_name = ''(local)'';')
                                        
                                        EXEC    (@sql);

                                        SET     @curr_id += 1;
                                END;
                END;

        -- If viewing statements only
        IF @verbose = 1
                BEGIN
                        SELECT          taq.action_code,
                                        taq.status_code,
                                        taq.entity,
                                        taq.sql_text
                        FROM            tools.atac_queue AS taq
                        ORDER BY        taq.statement_id;

                        RETURN;
                END;

        /*
                Release the queue
        */

        UPDATE  taq
        SET     taq.status_code = 'R'
        FROM    tools.atac_queue AS taq
        WHERE   taq.statement_id = 1;

        -- Start SQL Agent jobs
        IF @use_sql_agent = 1
                BEGIN
                        SET     @curr_id = 1;

                        WHILE @curr_id <= @number_of_processes
                                BEGIN
                                        SET     @sql = CONCAT('EXEC msdb.tools.sp_start_job @job_name = ''ATAC - Process ', @curr_id, ' of ', @number_of_processes, ''';');

                                        EXEC    (@sql);

                                        SET     @curr_id += 1;
                                END;

                        RAISERROR('', 10, 1) WITH NOWAIT;
                        RAISERROR('Processing has started.', 10, 1) WITH NOWAIT;
                END;
        ELSE
                BEGIN
                        RAISERROR('', 10, 1) WITH NOWAIT;
                        RAISERROR('You can now run the following statement', 10, 1) WITH NOWAIT;
                        RAISERROR('EXEC tools.usp_atac_process;', 10, 1) WITH NOWAIT;
                END;
END TRY
BEGIN CATCH
        THROW;
END CATCH;
GO
