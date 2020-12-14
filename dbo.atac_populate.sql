IF OBJECT_ID(N'dbo.atac_populate', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE dbo.atac_populate AS');
GO
ALTER PROCEDURE dbo.atac_populate
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Exit here if configurations is missing
IF NOT EXISTS (SELECT * FROM dbo.atac_configuration)
        BEGIN
                RETURN;
        END;
ELSE IF EXISTS (SELECT * FROM dbo.atac_configuration WHERE log_code = N'E')
        BEGIN
                RAISERROR(N'There at least one error in the configurations.', 16, 1);
                
                RETURN  -1000;
        END;
ELSE IF EXISTS (SELECT * FROM dbo.atac_queue WHERE status_code <> N'L')
        BEGIN
                RAISERROR(N'Processing has already begun.', 16, 1);
                
                RETURN  -1010;
        END;

-- Always validate
EXEC    dbo.atac_validate;

-- Get current configurations
CREATE TABLE    #settings
                (        
                        schema_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        table_id INT NULL,
                        table_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        column_id INT NULL,
                        column_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        new_column_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        datatype_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        max_length NVARCHAR(4) COLLATE DATABASE_DEFAULT NULL,
                        precision TINYINT NULL,
                        scale TINYINT NULL,
                        collation_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        is_nullable NVARCHAR(3) COLLATE DATABASE_DEFAULT NOT NULL,
                        xml_collection_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        datatype_default_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        datatype_rule_name SYSNAME COLLATE DATABASE_DEFAULT NULL
                );

INSERT          #settings
                (
                        schema_name,
                        table_id,
                        table_name,
                        column_id,
                        column_name,
                        new_column_name,
                        datatype_name,
                        max_length,
                        precision,
                        scale,
                        collation_name,
                        is_nullable,
                        xml_collection_name,
                        datatype_default_name,
                        datatype_rule_name
                )
SELECT          cfg.schema_name,
                acm.table_id,
                cfg.table_name,
                acm.column_id,
                cfg.column_name,
                cfg.new_column_name,
                cfg.datatype_name,
                cfg.max_length,
                cfg.precision,
                cfg.scale,
                cfg.collation_name,
                cfg.is_nullable,
                cfg.xml_collection_name,
                cfg.datatype_default_name,
                cfg.datatype_rule_name
FROM            dbo.atac_configuration AS cfg
INNER JOIN      (
                        SELECT          sch.name COLLATE DATABASE_DEFAULT AS schema_name,
                                        tbl.object_id AS table_id,
                                        tbl.name COLLATE DATABASE_DEFAULT AS table_name,
                                        col.column_id,
                                        col.name COLLATE DATABASE_DEFAULT AS column_name,
                                        usr.name COLLATE DATABASE_DEFAULT AS datatype_name,
                                        CASE
                                                WHEN usr.name COLLATE DATABASE_DEFAULT IN (N'nvarchar', N'varbinary', N'varchar') AND col.max_length = -1 THEN CAST(N'MAX' AS NVARCHAR(4))
                                                WHEN usr.name COLLATE DATABASE_DEFAULT IN (N'binary', N'char', N'varbinary', N'varchar') THEN CAST(col.max_length AS NVARCHAR(4))
                                                WHEN usr.name COLLATE DATABASE_DEFAULT IN (N'nchar', N'nvarchar') THEN CAST(col.max_length / 2 AS NVARCHAR(4))
                                                ELSE CAST(NULL AS NVARCHAR(4))
                                        END AS max_length,
                                        CASE 
                                                WHEN usr.name COLLATE DATABASE_DEFAULT IN (N'decimal', N'numeric') THEN col.precision
                                                ELSE CAST(NULL AS TINYINT)
                                        END AS precision,
                                        CASE 
                                                WHEN usr.name COLLATE DATABASE_DEFAULT IN (N'datetime2', N'datetimeoffset', N'decimal', N'numeric', N'time') THEN col.scale
                                                ELSE CAST(NULL AS TINYINT)
                                        END AS scale,
                                        col.collation_name COLLATE DATABASE_DEFAULT AS collation_name,
                                        CASE
                                                WHEN col.is_nullable = 1 THEN CAST(N'yes' AS NVARCHAR(3))
                                                ELSE CAST(N'no' AS NVARCHAR(3))
                                        END AS is_nullable,
                                        xsc.name COLLATE DATABASE_DEFAULT AS xml_collection_name,
                                        def.name COLLATE DATABASE_DEFAULT AS datatype_default_name,
                                        rul.name COLLATE DATABASE_DEFAULT AS datatype_rule_name
                        FROM            sys.schemas AS sch
                        INNER JOIN      sys.tables AS tbl ON tbl.schema_id = sch.schema_id
                                                AND tbl.type COLLATE DATABASE_DEFAULT = 'U'
                        INNER JOIN      sys.columns AS col ON col.object_id = tbl.object_id
                        INNER JOIN      sys.types AS usr ON usr.user_type_id = col.user_type_id
                        LEFT JOIN       sys.xml_schema_collections AS xsc ON xsc.xml_collection_id = col.xml_collection_id
                        LEFT JOIN       sys.objects AS def ON def.object_id = usr.default_object_id
                        LEFT JOIN       sys.objects AS rul ON rul.object_id = usr.rule_object_id
                ) AS acm ON acm.schema_name = cfg.schema_name
                        AND acm.table_name = cfg.table_name
                        AND acm.column_name = cfg.column_name
WHERE           (
                        cfg.log_code IS NULL
                        OR cfg.log_code = N'W'
                )
                AND EXISTS      (
                                        SELECT  COALESCE(cfg.new_column_name, cfg.column_name),
                                                cfg.datatype_name, 
                                                cfg.max_length,
                                                cfg.precision,
                                                cfg.scale,
                                                cfg.collation_name,
                                                cfg.is_nullable,
                                                cfg.xml_collection_name,
                                                cfg.datatype_default_name,
                                                cfg.datatype_rule_name

                                        EXCEPT

                                        SELECT  acm.column_name,
                                                acm.datatype_name, 
                                                acm.max_length,
                                                acm.precision,
                                                acm.scale,
                                                acm.collation_name,
                                                acm.is_nullable,
                                                acm.xml_collection_name,
                                                acm.datatype_default_name,
                                                acm.datatype_rule_name
                                );

-- No changes detected
IF NOT EXISTS (SELECT * FROM #settings)
        BEGIN
                RETURN;
        END;

-- Add database trigger statements to the queue
IF EXISTS (SELECT * FROM sys.triggers WHERE parent_class_desc = N'DATABASE')
        BEGIN
                INSERT  dbo.atac_queue
                        (
                                entity,
                                action_code,
                                status_code,
                                sql_text,
                                sort_order,
                                phase
                        )
                SELECT  N'' AS entity,
                        act.action_code,
                        N'L' AS status_code,
                        act.sql_text,
                        act.sort_order,
                        act.phase
                FROM    (
                                VALUES  (
                                                N'didt', 
                                                N'DISABLE TRIGGER ALL ON DATABASE;', 
                                                10, 
                                                1
                                        ),
                                        (
                                                N'endt', 
                                                N'ENABLE TRIGGER ALL ON DATABASE;', 
                                                230, 
                                                4
                                        )
                        ) AS act(action_code, sql_text, sort_order, phase);
        END;

-- Add table trigger statements to the queue
WITH cteTriggers(schema_name, table_name)
AS (
        SELECT DISTINCT schema_name,
                        table_name
        FROM            #settings AS cfg
        INNER JOIN      sys.triggers AS trg ON trg.parent_id = cfg.table_id
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity,
                act.action_code,
                N'L' AS status_code,
                act.sql_text,
                act.sort_order,
                act.phase
FROM            cteTriggers AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'ditg',
                                        CONCAT(N'DISABLE TRIGGER ALL ON ', CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)), N';'),
                                        20,
                                        2
                                ),
                                (
                                        N'entg',
                                        CONCAT(N'ENABLE TRIGGER ALL ON ', CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)), N';'),
                                        210,
                                        3
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Add foreign key statements to the queue
WITH cteForeignKeys(parent_schema_name, parent_table_name, child_schema_name, child_table_name, foreign_key_id)
AS (
        -- Parent columns
        SELECT          p.schema_name AS parent_schema_name,
                        p.table_name AS parent_table_name,
                        c.schema_name AS child_schema_name,
                        c.table_name AS child_table_name,
                        fkc.constraint_object_id AS foreign_key_id
        FROM            sys.foreign_key_columns AS fkc
        INNER JOIN      #settings AS p ON p.table_id = fkc.referenced_object_id
                                AND p.column_id = fkc.referenced_column_id
        INNER JOIN      #settings AS c ON c.table_id = fkc.parent_object_id
                                AND c.column_id = fkc.parent_column_id

        -- Take care of self-referecing
        UNION

        -- Child columns
        SELECT          p.schema_name AS parent_schema_name,
                        p.table_name AS parent_table_name,
                        c.schema_name AS child_schema_name,
                        c.table_name AS child_table_name,
                        fkc.constraint_object_id AS foreign_key_id
        FROM            sys.foreign_key_columns AS fkc
        INNER JOIN      #settings AS p ON p.table_id = fkc.referenced_object_id
                                AND p.column_id = fkc.referenced_column_id
        INNER JOIN      #settings AS c ON c.table_id = fkc.parent_object_id
                                AND c.column_id = fkc.parent_column_id
), cteReferences(parent_schema_name, parent_table_name, child_schema_name, child_table_name, foreign_key_name, parent_columnlist, child_columnlist, update_action, delete_action, status_code, precheck)
AS (
        SELECT          cte.parent_schema_name, 
                        cte.parent_table_name,
                        cte.child_schema_name, 
                        cte.child_table_name, 
                        fk.foreign_key_name, 
                        STUFF(p.columnlist.value(N'(text()[1])', N'NVARCHAR(MAX)'), 1, 2, N'') AS parent_columnlist,
                        STUFF(c.columnlist.value(N'(text()[1])', N'NVARCHAR(MAX)'), 1, 2, N'') AS child_columnlist,
                        fk.update_action, 
                        fk.delete_action,
                        N'L' AS status_code,
                        fk.precheck
        FROM            cteForeignKeys AS cte
        INNER JOIN      (
                                SELECT  object_id AS foreign_key_id,
                                        name COLLATE DATABASE_DEFAULT AS foreign_key_name,
                                        CASE
                                                WHEN delete_referential_action = 1 THEN N'ON DELETE CASCADE'
                                                WHEN delete_referential_action = 2 THEN N'ON DELETE SET NULL'
                                                WHEN delete_referential_action = 3 THEN N'ON DELETE SET DEFAULT'
                                                ELSE N'ON DELETE NO ACTION'
                                        END AS delete_action,
                                        CASE
                                                WHEN update_referential_action = 1 THEN N'ON UPDATE CASCADE'
                                                WHEN update_referential_action = 2 THEN N'ON UPDATE SET NULL'
                                                WHEN update_referential_action = 3 THEN N'ON UPDATE SET DEFAULT'
                                                ELSE N'ON UPDATE NO ACTION'
                                        END AS update_action,
                                        referenced_object_id AS parent_table_id,
                                        parent_object_id AS child_table_id,
                                        CONCAT(N'IF OBJECT_ID(N', QUOTENAME(name COLLATE DATABASE_DEFAULT, N''''), N', ''F'') IS ') AS precheck
                                FROM    sys.foreign_keys
                        ) AS fk ON fk.foreign_key_id = cte.foreign_key_id
        CROSS APPLY     (
                                SELECT          CONCAT(N', ', QUOTENAME(COALESCE(p.new_column_name, col.name COLLATE DATABASE_DEFAULT)))
                                FROM            sys.foreign_key_columns AS pfk
                                INNER JOIN      sys.columns AS col ON col.object_id = pfk.referenced_object_id
                                                        AND col.column_id = pfk.referenced_column_id
                                LEFT JOIN       #settings AS p ON p.table_id = col.object_id
                                                        AND p.column_id = col.column_id
                                WHERE           pfk.constraint_object_id = fk.foreign_key_id
                                                AND pfk.referenced_object_id = fk.parent_table_id
                                ORDER BY        pfk.constraint_column_id
                                FOR XML         PATH(N''),
                                                TYPE
                        ) AS p(columnlist)
        CROSS APPLY     (
                                SELECT          CONCAT(N', ', QUOTENAME(COALESCE(c.new_column_name, col.name COLLATE DATABASE_DEFAULT)))
                                FROM            sys.foreign_key_columns AS cfk
                                INNER JOIN      sys.columns AS col ON col.object_id = cfk.parent_object_id
                                                        AND col.column_id = cfk.parent_column_id
                                LEFT JOIN       #settings AS c ON c.table_id = col.object_id
                                                        AND c.column_id = col.column_id
                                WHERE           cfk.constraint_object_id = fk.foreign_key_id
                                                AND cfk.parent_object_id = fk.child_table_id
                                ORDER BY        cfk.constraint_column_id
                                FOR XML         PATH(N''),
                                                TYPE
                        ) AS c(columnlist)
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          act.entity,
                act.action_code,
                cte.status_code,
                CASE
                        WHEN act.action_code = N'crfk' THEN CONCAT(cte.precheck, N'NULL ', act.sql_text)
                        ELSE CONCAT(cte.precheck, N'NOT NULL ', act.sql_text)
                END AS sql_text,
                act.sort_order,
                act.phase
FROM            cteReferences AS cte
CROSS APPLY     (
                        VALUES  (
                                        CONCAT(QUOTENAME(cte.child_schema_name), N'.', QUOTENAME(cte.child_table_name)),
                                        N'drfk',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.child_schema_name), N'.', QUOTENAME(cte.child_table_name), N' DROP CONSTRAINT ', QUOTENAME(cte.foreign_key_name), N';'),
                                        30,
                                        2
                                ),
                                (
                                        CONCAT(QUOTENAME(cte.parent_schema_name), N'.', QUOTENAME(cte.parent_table_name)),
                                        N'drfk',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.child_schema_name), N'.', QUOTENAME(cte.child_table_name), N' DROP CONSTRAINT ', QUOTENAME(cte.foreign_key_name), N';'),
                                        30,
                                        2
                                ),
                                (
                                        CONCAT(QUOTENAME(cte.child_schema_name), N'.', QUOTENAME(cte.child_table_name)),
                                        N'crfk',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.child_schema_name), N'.', QUOTENAME(cte.child_table_name), N' WITH CHECK ADD CONSTRAINT ', QUOTENAME(cte.foreign_key_name), N' FOREIGN KEY (', cte.child_columnlist, N') REFERENCES ', QUOTENAME(cte.parent_schema_name), N'.', QUOTENAME(cte.parent_table_name), N' (', cte.parent_columnlist, N') ', cte.update_action, N' ', cte.delete_action, N';'),
                                        200,
                                        3
                                ),
                                (
                                        CONCAT(QUOTENAME(cte.parent_schema_name), N'.', QUOTENAME(cte.parent_table_name)),
                                        N'crfk',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.child_schema_name), N'.', QUOTENAME(cte.child_table_name), N' WITH CHECK ADD CONSTRAINT ', QUOTENAME(cte.foreign_key_name), N' FOREIGN KEY (', cte.child_columnlist, N') REFERENCES ', QUOTENAME(cte.parent_schema_name), N'.', QUOTENAME(cte.parent_table_name), N' (', cte.parent_columnlist, N') ', cte.update_action, N' ', cte.delete_action, N';'),
                                        200,
                                        3
                                )
                ) AS act(entity, action_code, sql_text, sort_order, phase);

-- Add index statements to the queue
CREATE TABLE    #indexes
                (
                        schema_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        table_id INT NOT NULL,
                        table_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        index_id INT NOT NULL,
                        PRIMARY KEY CLUSTERED
                        (
                                table_id,
                                index_id
                        ),
                        index_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        type_desc NVARCHAR(60) COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        is_unique BIT NOT NULL DEFAULT (0),  
                        is_primary_key BIT NOT NULL DEFAULT (0), 
                        is_unique_constraint BIT NOT NULL DEFAULT (0),
                        ignore_dup_key NVARCHAR(32) COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        data_space_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        allow_row_locks NVARCHAR(32) COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        allow_page_locks NVARCHAR(32) COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        fill_factor NVARCHAR(32) COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        is_padded NVARCHAR(32) COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        filter_definition NVARCHAR(MAX) COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        key_columns NVARCHAR(MAX) COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        include_columns NVARCHAR(MAX) COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        partition_columns NVARCHAR(MAX) COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N''),
                        compression_data NVARCHAR(MAX)  COLLATE DATABASE_DEFAULT NOT NULL DEFAULT (N'')
                );

INSERT          #indexes
                (
                        schema_name,
                        table_id,
                        table_name,
                        index_id
                )
SELECT DISTINCT cfg.schema_name,
                cfg.table_id,
                cfg.table_name,
                ic.index_id
FROM            #settings AS cfg
LEFT JOIN       sys.index_columns AS ic ON ic.object_id = cfg.table_id          -- Key column, included column or partition column
                        AND ic.column_id = cfg.column_id
LEFT JOIN       sys.indexes AS ind ON ind.object_id = cfg.table_id
                        AND ind.has_filter = CAST(1 AS BIT)                     -- Filter definition
WHERE           ic.object_id IS NOT NULL
                OR CHARINDEX(ind.filter_definition COLLATE DATABASE_DEFAULT, QUOTENAME(cfg.column_name)) >= 1;

UPDATE          i
SET             i.index_name = ind.name,
                i.type_desc = ind.type_desc,
                i.is_unique = ind.is_unique,
                i.is_primary_key = ind.is_primary_key,
                i.is_unique_constraint = ind.is_unique_constraint,
                i.ignore_dup_key = CONCAT(N'IGNORE_DUP_KEY = ', CASE WHEN ind.ignore_dup_key = 1 THEN N'ON' ELSE N'OFF' END),
                i.data_space_name = ds.name,
                i.allow_row_locks = CONCAT(N'ALLOW_ROW_LOCKS = ', CASE WHEN ind.allow_row_locks = 1 THEN N'ON' ELSE N'OFF' END),
                i.allow_page_locks = CONCAT(N'ALLOW_PAGE_LOCKS = ', CASE WHEN ind.allow_page_locks = 1 THEN N'ON' ELSE N'OFF' END),
                i.fill_factor = CONCAT(N'FILLFACTOR = ', CASE WHEN ind.fill_factor = 0 THEN COALESCE(CONVERT(NVARCHAR(3), cfg.maximum), N'100') ELSE CAST(ind.fill_factor AS NVARCHAR(3)) END),
                i.is_padded = CONCAT(N'PAD_INDEX = ', CASE WHEN ind.is_padded = 1 THEN N'ON' ELSE N'OFF' END),
                i.filter_definition =   CASE
                                                WHEN ind.filter_definition IS NULL THEN N''
                                                ELSE N' WHERE ' + ind.filter_definition
                                        END
FROM            #indexes AS i
INNER JOIN      sys.indexes AS ind ON ind.object_id = i.table_id
                        AND ind.index_id = i.index_id
INNER JOIN      sys.data_spaces AS ds ON ds.data_space_id = ind.data_space_id
LEFT JOIN       sys.configurations AS cfg ON cfg.configuration_id = 109;                -- Default Fill factor %

UPDATE          ind
SET             ind.key_columns = CONCAT(N' (', STUFF(keys.content.value(N'(.)[1]', N'NVARCHAR(MAX)'), 1, 2, N''), N')')
FROM            #indexes AS ind
CROSS APPLY     (
                        SELECT          CONCAT(N', ', QUOTENAME(COALESCE(s.new_column_name, col.name COLLATE DATABASE_DEFAULT)), CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N' ASC' END)
                        FROM            sys.index_columns AS ic
                        INNER JOIN      sys.columns AS col ON col.object_id = ic.object_id
                                                AND col.column_id = ic.column_id
                        LEFT JOIN       #settings AS s ON s.table_id = col.object_id
                                                AND s.column_id = col.column_id
                        WHERE           ic.object_id = ind.table_id
                                        AND ic.index_id = ind.index_id
                                        AND ic.key_ordinal >= 1
                        ORDER BY        ic.key_ordinal
                        FOR XML         PATH(N''),
                                        TYPE
                ) AS keys(content);

UPDATE          ind
SET             ind.include_columns =   CASE
                                                WHEN included.content IS NULL THEN N''
                                                ELSE CONCAT(N' INCLUDE (', STUFF(included.content.value(N'(.)[1]', N'NVARCHAR(MAX)'), 1, 2, N''), N')')
                                        END
FROM            #indexes AS ind
CROSS APPLY     (
                                SELECT          CONCAT(N', ', QUOTENAME(COALESCE(s.new_column_name, col.name COLLATE DATABASE_DEFAULT)))
                                FROM            sys.index_columns AS ic
                                INNER JOIN      sys.columns AS col ON col.object_id = ic.object_id
                                                        AND col.column_id = ic.column_id
                                LEFT JOIN       #settings AS s ON s.table_id = col.object_id
                                                        AND s.column_id = col.column_id
                                WHERE           ic.object_id = ind.table_id
                                                AND ic.index_id = ind.index_id
                                                AND ic.is_included_column = 1
                                ORDER BY        ic.index_column_id
                                FOR XML         PATH(N''),
                                                TYPE
                ) AS included(content);

UPDATE          ind
SET             ind.partition_columns = CASE
                                                WHEN partitions.content IS NULL THEN N''
                                                ELSE CONCAT(N'(', STUFF(partitions.content.value(N'(.)[1]', N'NVARCHAR(MAX)'), 1, 2, N''), N')')
                                        END
FROM            #indexes AS ind
CROSS APPLY     (
                                SELECT          CONCAT(N', ', QUOTENAME(COALESCE(s.new_column_name, col.name COLLATE DATABASE_DEFAULT)))
                                FROM            sys.index_columns AS ic
                                INNER JOIN      sys.columns AS col ON col.object_id = ic.object_id
                                                        AND col.column_id = ic.column_id
                                LEFT JOIN       #settings AS s ON s.table_id = col.object_id
                                                        AND s.column_id = col.column_id
                                WHERE           ic.object_id = ind.table_id
                                                AND ic.index_id = ind.index_id
                                                AND ic.partition_ordinal >= 1
                                ORDER BY        ic.partition_ordinal
                                FOR XML         PATH(N''),
                                                TYPE
                ) AS partitions(content);

UPDATE          ind
SET             ind.compression_data =  CASE
                                                WHEN compression.content IS NULL THEN N''
                                                ELSE STUFF(compression.content.value(N'(.)[1]', N'NVARCHAR(MAX)'), 1, 2, N'')
                                        END
FROM            #indexes AS ind
CROSS APPLY     (
                                SELECT          CONCAT(N', DATA_COMPRESSION = ', par.data_compression_desc COLLATE DATABASE_DEFAULT, N' ON PARTITIONS (', par.partition_number, N')')
                                FROM            (
                                                        SELECT  par.data_compression_desc,
                                                                par.partition_number,
                                                                MAX(par.partition_number) OVER () AS partition_count
                                                        FROM    sys.partitions AS par
                                                        WHERE   par.object_id = ind.table_id
                                                                AND par.index_id = ind.index_id
                                                ) AS par
                                WHERE           par.partition_count >= 2
                                ORDER BY        par.partition_number
                                FOR XML         PATH(N''),
                                                TYPE
                ) AS compression(content);

INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(ind.schema_name, N'.', ind.table_name) AS entity,
                act.action_code,
                N'L' AS status_code, 
                act.sql_text,
                act.sort_order,
                act.phase
FROM            #indexes AS ind
CROSS APPLY     (
                        VALUES  (
                                        N'drix',
                                        CASE
                                                WHEN 1 IN (ind.is_primary_key, ind.is_unique_constraint) THEN CONCAT(N'ALTER TABLE ', QUOTENAME(ind.schema_name), N'.', QUOTENAME(ind.table_name), N' DROP CONSTRAINT ', QUOTENAME(ind.index_name), N' WITH (ONLINE = OFF);')
                                                ELSE CONCAT(N'DROP INDEX ', QUOTENAME(ind.index_name), N' ON ', QUOTENAME(ind.schema_name), N'.', QUOTENAME(ind.table_name), N' WITH (ONLINE = OFF);')
                                        END,
                                        40,
                                        2
                                ),
                                (
                                        N'crix',
                                        CASE
                                                WHEN ind.is_primary_key = 1 THEN CONCAT(N'ALTER TABLE ', QUOTENAME(ind.schema_name), N'.', QUOTENAME(ind.table_name), N' ADD CONSTRAINT ', QUOTENAME(ind.index_name), N' PRIMARY KEY ', ind.type_desc)
                                                WHEN ind.is_unique_constraint = 1 THEN CONCAT(N'ALTER TABLE ', QUOTENAME(ind.schema_name), N'.', QUOTENAME(ind.table_name), N' ADD CONSTRAINT ', QUOTENAME(ind.index_name), N' UNIQUE ', ind.type_desc)
                                                ELSE CONCAT(N'CREATE', CASE WHEN ind.is_unique = 1 THEN N' UNIQUE' ELSE N'' END, ind.type_desc, N' INDEX ', QUOTENAME(ind.index_name), N' ON ', QUOTENAME(ind.schema_name), N'.', QUOTENAME(ind.table_name))
                                        END 
                                        + ind.key_columns
                                        + ind.include_columns
                                        + ind.filter_definition
                                        + CONCAT(N' WITH (', ind.is_padded, N', STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = ON, ', ind.ignore_dup_key, N', ONLINE = OFF, ', ind.allow_row_locks, N', ', ind.allow_page_locks, N', ', ind.fill_factor, ind.compression_data, N') ON ', QUOTENAME(ind.data_space_name))
                                        + ind.partition_columns
                                        + N';',
                                        190,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase)
ORDER BY        CASE
                        WHEN act.action_code = N'drix' AND ind.type_desc = N'CLUSTERED' THEN 1  -- Drop clustered last
                        WHEN act.action_code = N'drix' THEN 0
                        WHEN act.action_code = N'crix' AND ind.type_desc = N'CLUSTERED' THEN 0  -- Create clustered first
                        ELSE 1
                END;

DROP TABLE      #indexes;

-- Add table check constraint statements to the queue
WITH cteCheckConstraints(schema_name, table_name, check_constraint_name, check_definition)
AS (
        SELECT DISTINCT cfg.schema_name,
                        cfg.table_name,
                        cc.name COLLATE DATABASE_DEFAULT AS check_constraint_name,
                        CASE
                                WHEN cfg.new_column_name IS NULL THEN cc.definition COLLATE DATABASE_DEFAULT
                                ELSE REPLACE(cc.definition COLLATE DATABASE_DEFAULT, QUOTENAME(cfg.column_name), QUOTENAME(cfg.new_column_name))
                        END AS check_definition
        FROM            #settings AS cfg
        INNER JOIN      sys.check_constraints AS cc ON cc.parent_object_id = cfg.table_id
        WHERE           cfg.column_id = cc.parent_column_id 
                        OR CHARINDEX(QUOTENAME(cfg.column_name), cc.definition COLLATE DATABASE_DEFAULT) >= 1
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity, 
                act.action_code, 
                N'L' AS status_code, 
                act.sql_text, 
                act.sort_order,
                act.phase
FROM            cteCheckConstraints AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'drck',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' DROP CONSTRAINT ', QUOTENAME(cte.check_constraint_name), N';'),
                                        50,
                                        2
                                ),
                                (
                                        N'crck',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' WITH CHECK ADD CONSTRAINT ', QUOTENAME(cte.check_constraint_name), N' CHECK ', cte.check_definition, N';'),
                                        180,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Add table default constraint statements to the queue
WITH cteDefaultConstraints(schema_name, table_name, column_name, default_constraint_name, default_definition)
AS (
        SELECT DISTINCT cfg.schema_name,
                        cfg.table_name,
                        cfg.column_name,
                        dc.name COLLATE DATABASE_DEFAULT AS default_constraint_name,
                        dc.definition COLLATE DATABASE_DEFAULT AS default_definition
        FROM            #settings AS cfg
        INNER JOIN      sys.default_constraints AS dc ON dc.parent_object_id = cfg.table_id
                                AND dc.parent_column_id = cfg.column_id
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity, 
                act.action_code, 
                N'L' AS status_code, 
                act.sql_text, 
                act.sort_order,
                act.phase
FROM            cteDefaultConstraints AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'drdk',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' DROP CONSTRAINT ', QUOTENAME(cte.default_constraint_name), N';'),
                                        60,
                                        2
                                ),
                                (
                                        N'crdk',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' ADD CONSTRAINT ', QUOTENAME(cte.default_constraint_name), N' DEFAULT ', cte.default_definition, N' FOR ', QUOTENAME(cte.column_name), N';'),
                                        170,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Add computed column statements to the queue
WITH cteComputedColumns(schema_name, table_name, computed_column_name, computed_column_definition, persist_definition)
AS (
        SELECT DISTINCT cfg.schema_name,
                        cfg.table_name,
                        cc.name COLLATE DATABASE_DEFAULT AS computed_column_name,
                        CASE
                                WHEN cfg.new_column_name IS NULL THEN cc.definition COLLATE DATABASE_DEFAULT
                                ELSE REPLACE(cc.definition COLLATE DATABASE_DEFAULT, QUOTENAME(cfg.column_name), QUOTENAME(cfg.new_column_name))
                        END AS computed_column_definition,
                        CASE
                                WHEN cc.is_persisted = 1 THEN N' PERSISTED' 
                                ELSE N'' 
                        END AS persist_definition
        FROM            #settings AS cfg
        INNER JOIN      sys.computed_columns AS cc ON cc.object_id = cfg.table_id
        WHERE           cfg.column_id = cc.column_id 
                        OR CHARINDEX(QUOTENAME(cfg.column_name), cc.definition COLLATE DATABASE_DEFAULT) >= 1
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity, 
                act.action_code, 
                N'L' AS status_code, 
                act.sql_text, 
                act.sort_order,
                act.phase
FROM            cteComputedColumns AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'drcc',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' DROP COLUMN ', QUOTENAME(cte.computed_column_name), N';'),
                                        70,
                                        2
                                ),
                                (
                                        N'crcc',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' ADD ', QUOTENAME(cte.computed_column_name), N' AS ', cte.computed_column_definition, cte.persist_definition, N';'),
                                        160,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Add datatype default statements to the queue
WITH cteDefaults(schema_name, table_name, column_name, new_column_name, datatype_default_name, old_default_name)
AS (
        SELECT DISTINCT cfg.schema_name,
                        cfg.table_name,
                        cfg.column_name,
                        cfg.new_column_name,
                        cfg.datatype_default_name,
                        def.name COLLATE DATABASE_DEFAULT AS old_default_name
        FROM            #settings AS cfg
        INNER JOIN      sys.columns AS col ON col.object_id = cfg.table_id
                                AND col.column_id = cfg.column_id
        INNER JOIN      sys.types AS usr ON usr.user_type_id = col.user_type_id
        LEFT JOIN       sys.objects AS def ON def.object_id = usr.default_object_id
        WHERE           cfg.datatype_default_name <> def.name COLLATE DATABASE_DEFAULT
                        OR cfg.datatype_default_name > N'' AND def.name IS NULL
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity,
                act.action_code,
                N'L' AS status_code,
                act.sql_text,
                act.sort_order,
                act.phase
FROM            cteDefaults AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'undf',
                                        CASE
                                                WHEN cte.old_default_name > N'' THEN CONCAT(N'EXEC sp_unbinddefault @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.table_name) + N'.' + QUOTENAME(cte.column_name), N'''', N''''''), N''';') 
                                                ELSE NULL
                                        END,
                                        80,
                                        2
                                ),
                                (
                                        N'bidf',
                                        CASE 
                                                WHEN cte.datatype_default_name > N'' THEN CONCAT(N'EXEC sp_binddefault @rulename = N', QUOTENAME(cte.datatype_default_name, N''''), N', @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.table_name) + N'.' + QUOTENAME(COALESCE(cte.new_column_name, cte.column_name)), N'''', N''''''), N''';')
                                                ELSE NULL
                                        END,
                                        150,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase)
WHERE           act.sql_text IS NOT NULL;

-- Add datatype rule statements to the queue
WITH cteRules(schema_name, table_name, column_name, new_column_name, datatype_rule_name, old_rule_name)
AS (
        SELECT DISTINCT cfg.schema_name,
                        cfg.table_name,
                        cfg.column_name,
                        cfg.new_column_name,
                        cfg.datatype_rule_name,
                        rul.name COLLATE DATABASE_DEFAULT AS old_rule_name
        FROM            #settings AS cfg
        INNER JOIN      sys.columns AS col ON col.object_id = cfg.table_id
                                AND col.column_id = cfg.column_id
        INNER JOIN      sys.types AS usr ON usr.user_type_id = col.user_type_id
        LEFT JOIN       sys.objects AS rul ON rul.object_id = usr.default_object_id
        WHERE           cfg.datatype_rule_name <> rul.name COLLATE DATABASE_DEFAULT
                        OR cfg.datatype_rule_name > N'' AND rul.name IS NULL
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity,
                act.action_code,
                N'L' AS status_code,
                act.sql_text,
                act.sort_order,
                act.phase
FROM            cteRules AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'unru',
                                        CASE
                                                WHEN cte.old_rule_name > N'' THEN CONCAT(N'EXEC sp_unbindrule @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.table_name) + N'.' + QUOTENAME(cte.column_name), N'''', N''''''), N''';') 
                                                ELSE NULL
                                        END,
                                        90,
                                        2
                                ),
                                (
                                        N'biru',
                                        CASE 
                                                WHEN cte.datatype_rule_name > N'' THEN CONCAT(N'EXEC sp_bindrule @rulename = N', QUOTENAME(cte.datatype_rule_name, N''''), N', @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.table_name) + N'.' + QUOTENAME(COALESCE(cte.new_column_name, cte.column_name)), N'''', N''''''), N''';')
                                                ELSE NULL
                                        END,
                                        10,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase)
WHERE           act.sql_text IS NOT NULL;

-- Add alter table alter column statements to the queue
WITH cteAlterColumn(schema_name, table_name, column_name, datatype_name, max_length, precision_and_scale, collation_name, xml_collection_name, is_nullable)
AS (
        SELECT DISTINCT schema_name,
                        table_name,
                        column_name,
                        QUOTENAME(datatype_name) AS datatype_name,
                        CASE
                                WHEN max_length IS NULL THEN N''
                                ELSE CONCAT(N'(', max_length, N')')
                        END AS max_length,
                        CASE
                                WHEN precision IS NOT NULL AND scale IS NOT NULL THEN CONCAT(N'(', precision, N', ', scale, N')')
                                WHEN scale IS NOT NULL THEN CONCAT(N'(', scale, N')')
                                ELSE N''
                        END AS precision_and_scale,
                        CASE
                                WHEN collation_name IS NULL THEN N''
                                ELSE CONCAT(' COLLATE ', collation_name)
                        END AS collation_name,
                        CASE
                                WHEN xml_collection_name IS NULL THEN N''
                                ELSE CONCAT(N'(', QUOTENAME(xml_collection_name), N')')
                        END AS xml_collection_name,
                        CASE
                                WHEN is_nullable = N'yes' THEN N' NULL' 
                                ELSE N' NOT NULL' 
                        END AS is_nullable
        FROM            #settings
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity,
                act.action_code,
                N'L' AS status_code,
                act.sql_text,
                act.sort_order,
                act.phase
FROM            cteAlterColumn AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'alco',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' ALTER COLUMN ', QUOTENAME(cte.column_name), QUOTENAME(cte.datatype_name), cte.max_length, cte.precision_and_scale, cte.collation_name, QUOTENAME(cte.xml_collection_name), cte.is_nullable, N';'),
                                        110,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Add rename column statements to the queue
WITH cteColumns(schema_name, table_name, column_name, new_column_name)
AS (
        SELECT DISTINCT schema_name,
                        table_name,
                        column_name,
                        new_column_name
        FROM            #settings
        WHERE           new_column_name > N''
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity,
                act.action_code,
                N'L' AS status_code,
                act.sql_text,
                act.sort_order,
                act.phase
FROM            cteColumns AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'reco',
                                        CONCAT(N'EXEC sp_rename @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.table_name) + N'.' + QUOTENAME(cte.column_name), N'''', N''''''), N''', @newname = N', QUOTENAME(cte.new_column_name, N''''), N', @objtype = N''COLUMN'';'),
                                        130,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Add sp_refreshsqlmodule statements to the queue
WITH cteModules(schema_name, object_name, object_type)
AS (
        SELECT          s.name COLLATE DATABASE_DEFAULT AS schema_name,
                        p.name COLLATE DATABASE_DEFAULT AS object_name,
                        p.type COLLATE DATABASE_DEFAULT AS object_type
        FROM            #settings AS cfg
        INNER JOIN      sys.sql_expression_dependencies AS sed ON sed.referenced_id = cfg.table_id
                                AND sed.referencing_class_desc = N'OBJECT_OR_COLUMN'
        INNER JOIN      sys.procedures AS p ON p.object_id = sed.referencing_id
        INNER JOIN      sys.schemas AS s ON s.schema_id = p.schema_id

        UNION

        SELECT          s.name COLLATE DATABASE_DEFAULT AS schema_name,
                        v.name COLLATE DATABASE_DEFAULT AS object_name,
                        v.type COLLATE DATABASE_DEFAULT AS object_type
        FROM            #settings AS cfg
        INNER JOIN      sys.sql_expression_dependencies AS sed ON sed.referenced_id = cfg.table_id
                                AND sed.referencing_class_desc = N'OBJECT_OR_COLUMN'
        INNER JOIN      sys.views AS v ON v.object_id = sed.referencing_id
        INNER JOIN      sys.schemas AS s ON s.schema_id = v.schema_id
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT DISTINCT CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.object_name)) AS entity,
                act.action_code,
                N'L' AS status_code,
                act.sql_text,
                act.sort_order,
                act.phase
FROM            cteModules AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'remo',
                                        CONCAT(N'EXEC sys.sp_refreshsqlmodule @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.object_name), N'''', N''''''), N';'),
                                        220,
                                        3
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Cleanup
DROP TABLE      #settings;

-- Update duplicate status_code
WITH cteDuplicates(status_code, rnk)
AS (
        SELECT  status_code,
                ROW_NUMBER() OVER (PARTITION BY entity, sql_text ORDER BY queue_id) AS rnk
        FROM    dbo.atac_queue
)
UPDATE  cteDuplicates
SET     status_code = N'D'
WHERE   rnk >= 2;

-- Sort statements in correct processing order
WITH cteSort(statement_id, rnk)
AS (
        SELECT  statement_id,
                ROW_NUMBER() OVER (ORDER BY sort_order, entity) AS rnk
        FROM    dbo.atac_queue
)
UPDATE  cteSort
SET     statement_id = rnk
WHERE   statement_id <> rnk;

-- Release first statement
WITH cteReleases(status_code, rnk)
AS (
        SELECT  status_code, 
                DENSE_RANK() OVER (ORDER BY phase, entity, statement_id) AS rnk
        FROM    dbo.atac_queue
)
UPDATE  cteReleases
SET     status_code = N'R'
WHERE   rnk = 1;
GO
