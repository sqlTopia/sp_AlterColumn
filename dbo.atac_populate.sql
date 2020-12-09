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

IF EXISTS (SELECT * FROM dbo.atac_configuration WHERE log_code = N'E')
        BEGIN
                RAISERROR(N'There at least one error in the configurations.', 16, 1);
                
                RETURN  -1000;
        END;

-- Get current configurations
CREATE TABLE    #settings
                (
                        status_code NCHAR(1) COLLATE DATABASE_DEFAULT NOT NULL,
                        schema_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        table_id INT NULL,
                        table_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        column_id INT NULL,
                        column_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        tag NVARCHAR(36) COLLATE DATABASE_DEFAULT NOT NULL,
                        new_column_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        datatype_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        max_length NVARCHAR(4) COLLATE DATABASE_DEFAULT NULL,
                        precision TINYINT NULL,
                        scale TINYINT NULL,
                        collation_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        is_nullable NVARCHAR(3) COLLATE DATABASE_DEFAULT NOT NULL,
                        xml_collection_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        default_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        rule_name SYSNAME COLLATE DATABASE_DEFAULT NULL
                );

INSERT          #settings
                (
                        status_code,
                        schema_name,
                        table_id,
                        table_name,
                        column_id,
                        column_name,
                        tag,
                        new_column_name,
                        datatype_name,
                        max_length,
                        precision,
                        scale,
                        collation_name,
                        is_nullable,
                        xml_collection_name,
                        default_name,
                        rule_name
                )
SELECT          CASE
                        WHEN acm.column_id IS NULL THEN N'E'                            -- Error (column could not be found)
                        WHEN EXISTS     (
                                                SELECT  cfg.datatype_name, 
                                                        cfg.max_length,
                                                        cfg.precision,
                                                        cfg.scale,
                                                        cfg.collation_name,
                                                        cfg.is_nullable,
                                                        cfg.xml_collection_name,
                                                        cfg.default_name,
                                                        cfg.rule_name

                                                INTERSECT

                                                SELECT  acm.datatype_name, 
                                                        acm.max_length,
                                                        acm.precision,
                                                        acm.scale,
                                                        acm.collation_name,
                                                        acm.is_nullable,
                                                        acm.xml_collection_name,
                                                        acm.default_name,
                                                        acm.rule_name
                                        ) THEN N'I'                                     -- Ignored (no change in column metadata)
                        ELSE N'L'                                                       -- Locked (prepared but not available yet)
                END AS status_code,
                cfg.schema_name,
                acm.table_id,
                cfg.table_name,
                acm.column_id,
                cfg.column_name,
                cfg.tag,
                cfg.new_column_name,
                cfg.datatype_name,
                cfg.max_length,
                cfg.precision,
                cfg.scale,
                cfg.collation_name,
                cfg.is_nullable,
                cfg.xml_collection_name,
                cfg.default_name,
                cfg.rule_name
FROM            dbo.atac_configuration AS cfg
LEFT JOIN       (
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
                                        def.name COLLATE DATABASE_DEFAULT AS default_name,
                                        rul.name COLLATE DATABASE_DEFAULT AS rule_name
                        FROM            sys.schemas AS sch
                        INNER JOIN      sys.tables AS tbl ON tbl.schema_id = sch.schema_id
                                                AND tbl.type COLLATE DATABASE_DEFAULT = 'U'
                        INNER JOIN      sys.columns AS col ON col.object_id = tbl.object_id
                        INNER JOIN      sys.types AS usr ON usr.user_type_id = col.user_type_id
                        LEFT JOIN       sys.xml_schema_collections AS xsc ON xsc.xml_collection_id = col.xml_collection_id
                        LEFT JOIN       sys.objects AS def ON def.object_id = col.default_object_id
                        LEFT JOIN       sys.objects AS rul ON rul.object_id = col.rule_object_id
                ) AS acm ON acm.schema_name = cfg.schema_name
                        AND acm.table_name = cfg.table_name
                        AND acm.column_name = cfg.column_name;

-- Add database trigger statements to the queue
INSERT  dbo.atac_queue
        (
                entity,
                action_code,
                sql_text,
                tag,
                sort_order,
                phase
        )
SELECT  N'' AS entity,
        action_code,
        sql_text,
        N'' AS tag,
        sort_order,
        phase
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
                                210, 
                                5
                        )
        ) AS act(action_code, sql_text, sort_order, phase);

-- Add table trigger statements to the queue
WITH cteTriggers(schema_name, table_name, status_code, tag)
AS (
        SELECT DISTINCT schema_name,
                        table_name,
                        status_code,
                        tag
        FROM            #settings
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        tag,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity,
                act.action_code,
                cte.status_code,
                act.sql_text,
                cte.tag,
                act.sort_order,
                act.phase
FROM            cteTriggers AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'ditg',
                                        CONCAT(N'DISABLE TRIGGER ALL ON ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N';'),
                                        20,
                                        2
                                ),
                                (
                                        N'entg',
                                        CONCAT(N'ENABLE TRIGGER ALL ON ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N';'),
                                        200,
                                        4
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Add foreign key statements to the queue
WITH cteForeignKeys(parent_schema_name, parent_table_name, child_schema_name, child_table_name, foreign_key_id, tag, status_code)
AS (
        -- Parent columns
        SELECT          p.schema_name AS parent_schema_name,
                        p.table_name AS parent_table_name,
                        c.schema_name AS child_schema_name,
                        c.table_name AS child_table_name,
                        fkc.constraint_object_id AS foreign_key_id,
                        p.tag,
                        p.status_code
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
                        fkc.constraint_object_id AS foreign_key_id,
                        c.tag,
                        c.status_code
        FROM            sys.foreign_key_columns AS fkc
        INNER JOIN      #settings AS p ON p.table_id = fkc.referenced_object_id
                                AND p.column_id = fkc.referenced_column_id
        INNER JOIN      #settings AS c ON c.table_id = fkc.parent_object_id
                                AND c.column_id = fkc.parent_column_id
), cteReferences(parent_schema_name, parent_table_name, child_schema_name, child_table_name, foreign_key_name, parent_columnlist, child_columnlist, update_action, delete_action, tag, status_code, precheck)
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
                        cte.tag,
                        cte.status_code,
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
                                SELECT          CONCAT(N', ', QUOTENAME(col.name COLLATE DATABASE_DEFAULT))
                                FROM            sys.foreign_key_columns AS pfk
                                INNER JOIN      sys.columns AS col ON col.object_id = pfk.referenced_object_id
                                                        AND col.column_id = pfk.referenced_column_id
                                WHERE           pfk.constraint_object_id = fk.foreign_key_id
                                                AND pfk.referenced_object_id = fk.parent_table_id
                                ORDER BY        pfk.constraint_column_id
                                FOR XML         PATH(N''),
                                                TYPE
                        ) AS p(columnlist)
        CROSS APPLY     (
                                SELECT          CONCAT(N', ', QUOTENAME(col.name COLLATE DATABASE_DEFAULT))
                                FROM            sys.foreign_key_columns AS cfk
                                INNER JOIN      sys.columns AS col ON col.object_id = cfk.parent_object_id
                                                        AND col.column_id = cfk.parent_column_id
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
                        tag,
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
                cte.tag,
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
                                        190,
                                        3
                                ),
                                (
                                        CONCAT(QUOTENAME(cte.parent_schema_name), N'.', QUOTENAME(cte.parent_table_name)),
                                        N'crfk',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.child_schema_name), N'.', QUOTENAME(cte.child_table_name), N' WITH CHECK ADD CONSTRAINT ', QUOTENAME(cte.foreign_key_name), N' FOREIGN KEY (', cte.child_columnlist, N') REFERENCES ', QUOTENAME(cte.parent_schema_name), N'.', QUOTENAME(cte.parent_table_name), N' (', cte.parent_columnlist, N') ', cte.update_action, N' ', cte.delete_action, N';'),
                                        190,
                                        3
                                )
                ) AS act(entity, action_code, sql_text, sort_order, phase);

-- Add index statements to the queue
WITH cteIndexes(schema_name, table_id, table_name, index_id, tag, status_code)
AS (
        SELECT DISTINCT cfg.schema_name,
                        cfg.table_id,
                        cfg.table_name,
                        ic.index_id,
                        cfg.tag,
                        cfg.status_code
        FROM            #settings AS cfg
        INNER JOIN      sys.index_columns AS ic ON ic.object_id = cfg.table_id
                                AND ic.column_id = cfg.column_id
), cteReferences(schema_name, table_id, table_name, index_id, index_name, is_unique, is_primary_key, is_unique_constraint, type_desc, filter_definition, with_clause, on_clause, key_columns, include_columns, partition_columns, status_code, tag) 
AS (
        SELECT          cte.schema_name, 
                        cte.table_id, 
                        cte.table_name, 
                        cte.index_id,
                        ind.index_name,
                        ind.is_unique,
                        ind.is_primary_key,
                        ind.is_unique_constraint,
                        ind.type_desc,
                        ind.filter_definition,
                        CONCAT(N'WITH (PAD_INDEX = ' + CASE WHEN ind.is_padded = 1 THEN N'ON' ELSE N'OFF' END, N', STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = ON, IGNORE_DUP_KEY = ', CASE WHEN ind.ignore_dup_key = 1 THEN N'ON' ELSE N'OFF' END, N', ONLINE = OFF, ALLOW_ROW_LOCKS = ', CASE WHEN ind.allow_row_locks = 1 THEN N'ON' ELSE N'OFF' END, N', ALLOW_PAGE_LOCKS = ', CASE WHEN ind.allow_page_locks = 1 THEN N'ON' ELSE N'OFF' END, N', FILLFACTOR = ', CASE WHEN ind.fill_factor = 0 THEN N'100' ELSE CAST(ind.fill_factor AS NVARCHAR(3)) END, comp.content.value(N'(.)[1]', N'NVARCHAR(MAX)'), N')') AS with_clause,
                        CONCAT(N'ON ', QUOTENAME(ds.data_space_name)) AS on_clause,
                        STUFF(k.content.value(N'(.)[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS key_columns,
                        STUFF(i.content.value(N'(.)[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS include_columns,
                        STUFF(p.content.value(N'(.)[1]', N'NVARCHAR(MAX)'), 1, 2, N'') AS partition_columns,
                        cte.status_code,
                        cte.tag
        FROM            cteIndexes AS cte
        INNER JOIN      (
                                SELECT  object_id AS table_id,
                                        index_id,
                                        name COLLATE DATABASE_DEFAULT AS index_name,
                                        type_desc,
                                        is_unique, 
                                        is_primary_key, 
                                        is_unique_constraint,
                                        ignore_dup_key,
                                        data_space_id,
                                        allow_row_locks,
                                        allow_page_locks,
                                        fill_factor,
                                        is_padded,
                                        filter_definition COLLATE DATABASE_DEFAULT AS filter_definition
                                FROM    sys.indexes
                                WHERE   index_id >= 1
                        ) AS ind ON ind.table_id = cte.table_id
                                AND ind.index_id = cte.index_id
        INNER JOIN      (
                                SELECT  data_space_id,
                                        name COLLATE DATABASE_DEFAULT AS data_space_name
                                FROM    sys.data_spaces 
                        ) AS ds ON ds.data_space_id = ind.data_space_id
        OUTER APPLY     (
                                SELECT          CONCAT(N', ', QUOTENAME(col.name COLLATE DATABASE_DEFAULT), CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N' ASC' END)
                                FROM            sys.index_columns AS ic
                                INNER JOIN      sys.columns AS col ON col.object_id = ic.object_id
                                                        AND col.column_id = ic.column_id
                                WHERE           ic.object_id = ind.table_id
                                                AND ic.index_id = ind.index_id
                                                AND ic.key_ordinal >= 1
                                ORDER BY        ic.key_ordinal
                                FOR XML         PATH(N''),
                                                TYPE
                        ) AS k(content)
        OUTER APPLY     (
                                SELECT          CONCAT(N', ', QUOTENAME(col.name COLLATE DATABASE_DEFAULT))
                                FROM            sys.index_columns AS ic
                                INNER JOIN      sys.columns AS col ON col.object_id = ic.object_id
                                                        AND col.column_id = ic.column_id
                                WHERE           ic.object_id = ind.table_id
                                                AND ic.index_id = ind.index_id
                                                AND ic.is_included_column = 1
                                ORDER BY        ic.index_column_id
                                FOR XML         PATH(N''),
                                                TYPE
                        ) AS i(content)
        OUTER APPLY     (
                                SELECT          CONCAT(N', ', QUOTENAME(col.name COLLATE DATABASE_DEFAULT))
                                FROM            sys.index_columns AS ic
                                INNER JOIN      sys.columns AS col ON col.object_id = ic.object_id
                                                        AND col.column_id = ic.column_id
                                WHERE           ic.object_id = ind.table_id
                                                AND ic.index_id = ind.index_id
                                                AND ic.partition_ordinal >= 1
                                ORDER BY        ic.partition_ordinal
                                FOR XML         PATH(N''),
                                                TYPE
                        ) AS p(content)
        OUTER APPLY     (
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
                        ) AS comp(content)
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        tag,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity,
                act.action_code,
                cte.status_code, 
                act.sql_text,
                cte.tag,
                act.sort_order,
                act.phase
FROM            cteReferences AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'drix',
                                        CASE
                                                WHEN cte.is_primary_key = 1 OR cte.is_unique_constraint = 1 THEN CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' DROP CONSTRAINT ', QUOTENAME(cte.index_name), N' WITH (ONLINE = OFF);')
                                                ELSE CONCAT(N'DROP INDEX ', QUOTENAME(cte.index_name), N' ON ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' WITH (ONLINE = OFF);')
                                        END,
                                        40,
                                        2
                                ),
                                (
                                        N'crix',
                                        CASE
                                                WHEN cte.is_primary_key = 1 THEN CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' ADD CONSTRAINT ', QUOTENAME(cte.index_name), N' PRIMARY KEY ', CASE WHEN cte.type_desc = 'CLUSTERED' THEN N'CLUSTERED' ELSE N'NONCLUSTERED' END)
                                                WHEN cte.is_unique_constraint = 1 THEN CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' ADD CONSTRAINT ', QUOTENAME(cte.index_name), N' UNIQUE ', CASE WHEN cte.type_desc = 'CLUSTERED' THEN N'CLUSTERED' ELSE N'NONCLUSTERED' END)
                                                ELSE CONCAT(N'CREATE ', CASE WHEN cte.is_unique = 1 THEN N'UNIQUE ' ELSE N'' END, CASE WHEN cte.type_desc = 'CLUSTERED' THEN N'CLUSTERED ' ELSE N'NONCLUSTERED ' END, N'INDEX ', QUOTENAME(cte.index_name), N' ON ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name))
                                        END 
                                        + CONCAT(N' (', cte.key_columns, N')', CASE WHEN cte.include_columns IS NULL THEN N'' ELSE N' INCLUDE (' + cte.include_columns + N')' END, CASE WHEN cte.filter_definition IS NULL THEN N'' ELSE N' WHERE ' + cte.filter_definition END)
                                        + CASE WHEN cte.with_clause >= N'' THEN cte.with_clause ELSE N'' END
                                        + CASE WHEN cte.partition_columns > N'' THEN CONCAT(N' ', cte.partition_columns, N');') ELSE N'' END,
                                        180,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase)
ORDER BY        CASE
                        WHEN act.action_code = N'drix' AND cte.type_desc = N'CLUSTERED' THEN 1  -- Drop clustered last
                        WHEN act.action_code = N'drix' THEN 0
                        WHEN act.action_code = N'crix' AND cte.type_desc = N'CLUSTERED' THEN 0  -- Create clustered first
                        ELSE 1
                END;

-- Add check constraint statements to the queue
WITH cteCheckConstraints(schema_name, table_name, check_constraint_name, check_definition, status_code, tag)
AS (
        SELECT DISTINCT cfg.schema_name,
                        cfg.table_name,
                        cc.check_constraint_name,
                        cc.check_definition,
                        cfg.status_code,
                        cfg.tag
        FROM            #settings AS cfg
        INNER JOIN      (
                                SELECT  parent_object_id AS table_id,
                                        parent_column_id AS column_id,
                                        name COLLATE DATABASE_DEFAULT AS check_constraint_name,
                                        definition COLLATE DATABASE_DEFAULT AS check_definition
                                FROM    sys.check_constraints
                        ) AS cc ON cc.table_id = cfg.table_id
        WHERE           cfg.column_id = cc.column_id 
                        OR CHARINDEX(QUOTENAME(cfg.column_name), cc.check_definition) >= 1
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        tag,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity, 
                act.action_code, 
                cte.status_code, 
                act.sql_text, 
                cte.tag,
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
                                        170,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Add default constraint statements to the queue
WITH cteDefaultConstraints(schema_name, table_name, check_constraint_name, check_definition, status_code, tag)
AS (
        SELECT          cfg.schema_name,
                        cfg.table_name,
                        dc.default_constraint_name,
                        dc.default_definition,
                        cfg.status_code,
                        cfg.tag
        FROM            #settings AS cfg
        INNER JOIN      (
                                SELECT  parent_object_id AS table_id,
                                        parent_column_id AS column_id,
                                        name COLLATE DATABASE_DEFAULT AS default_constraint_name,
                                        definition COLLATE DATABASE_DEFAULT AS default_definition
                                FROM    sys.default_constraints
                        ) AS dc ON dc.table_id = cfg.table_id
                                AND dc.column_id = cfg.column_id
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        tag,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity, 
                act.action_code, 
                cte.status_code, 
                act.sql_text, 
                cte.tag,
                act.sort_order,
                act.phase
FROM            cteDefaultConstraints AS cte
CROSS APPLY     (
                        VALUES  (
                                        N'drdk',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' DROP CONSTRAINT ', QUOTENAME(cte.check_constraint_name), N';'),
                                        60,
                                        2
                                ),
                                (
                                        N'crdk',
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' WITH CHECK ADD CONSTRAINT ', QUOTENAME(cte.check_constraint_name), N' CHECK ', cte.check_definition, N';'),
                                        160,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Add computed column statements to the queue
WITH cteComputedColumns(schema_name, table_name, computed_column_name, computed_column_definition, status_code, tag, is_persisted)
AS (
        SELECT DISTINCT cfg.schema_name,
                        cfg.table_name,
                        cc.computed_column_name,
                        cc.computed_column_definition,
                        cfg.status_code,
                        cfg.tag,
                        cc.is_persisted
        FROM            #settings AS cfg
        INNER JOIN      (
                                SELECT  object_id AS table_id,
                                        column_id AS column_id,
                                        definition COLLATE DATABASE_DEFAULT AS computed_column_definition,
                                        name COLLATE DATABASE_DEFAULT AS computed_column_name,
                                        is_persisted
                                FROM    sys.computed_columns
                        ) AS cc ON cc.table_id = cfg.table_id
        WHERE           cfg.column_id = cc.column_id 
                        OR CHARINDEX(QUOTENAME(cfg.column_name), cc.computed_column_definition) >= 1
)
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        tag,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name)) AS entity, 
                act.action_code, 
                cte.status_code, 
                act.sql_text, 
                cte.tag,
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
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' ADD ', QUOTENAME(cte.computed_column_name), N' AS ', cte.computed_column_definition, CASE WHEN cte.is_persisted = 1 THEN N' PERSISTED;' ELSE N';' END),
                                        150,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase);

-- Add datatype default statements to the queue
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        tag,
                        sort_order,
                        phase
                )
SELECT DISTINCT CONCAT(QUOTENAME(cfg.schema_name), N'.', QUOTENAME(cfg.table_name)) AS entity,
                act.action_code,
                cfg.status_code,
                act.sql_text,
                cfg.tag,
                act.sort_order,
                act.phase
FROM            #settings AS cfg
INNER JOIN      sys.columns AS col ON col.object_id = cfg.table_id
                        AND col.column_id = cfg.column_id
LEFT JOIN       (
                        SELECT  object_id AS default_object_id,
                                name COLLATE DATABASE_DEFAULT AS default_name
                        FROM    sys.objects
                ) AS def ON def.default_object_id = col.default_object_id
CROSS APPLY     (
                        VALUES  (
                                        N'undf',
                                        CASE
                                                WHEN cfg.default_name <> def.default_name THEN CONCAT(N'EXEC sp_unbinddefault @objname = N''', REPLACE(QUOTENAME(cfg.schema_name) + N'.' + QUOTENAME(cfg.table_name) + N'.' + QUOTENAME(cfg.column_name), N'''', N''''''), N''';') 
                                                ELSE NULL
                                        END,
                                        80,
                                        2
                                ),
                                (
                                        N'bidf',
                                        CASE 
                                                WHEN cfg.default_name > N'' AND (cfg.default_name <> def.default_name OR def.default_name IS NULL) THEN CONCAT(N'EXEC sp_binddefault @rulename = N', QUOTENAME(cfg.default_name, N''''), N', @objname = N''', REPLACE(QUOTENAME(cfg.schema_name) + N'.' + QUOTENAME(cfg.table_name) + N'.' + QUOTENAME(cfg.column_name), N'''', N''''''), N''';')
                                                ELSE NULL
                                        END,
                                        140,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase)
WHERE           act.sql_text IS NOT NULL;

-- Add datatype rule statements to the queue
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        tag,
                        sort_order,
                        phase
                )
SELECT DISTINCT CONCAT(QUOTENAME(cfg.schema_name), N'.', QUOTENAME(cfg.table_name)) AS entity,
                act.action_code,
                cfg.status_code,
                act.sql_text,
                cfg.tag,
                act.sort_order,
                act.phase
FROM            #settings AS cfg
INNER JOIN      sys.columns AS col ON col.object_id = cfg.table_id
                        AND col.column_id = cfg.column_id
LEFT JOIN       (
                        SELECT  object_id AS rule_object_id,
                                name COLLATE DATABASE_DEFAULT AS rule_name
                        FROM    sys.objects
                ) AS rul ON rul.rule_object_id = col.rule_object_id
CROSS APPLY     (
                        VALUES  (
                                        N'unru',
                                        CASE
                                                WHEN cfg.rule_name <> rul.rule_name THEN CONCAT(N'EXEC sp_unbindrule @objname = N''', REPLACE(QUOTENAME(cfg.schema_name) + N'.' + QUOTENAME(cfg.table_name) + N'.' + QUOTENAME(cfg.column_name), N'''', N''''''), N''';') 
                                                ELSE NULL
                                        END,
                                        90,
                                        2
                                ),
                                (
                                        N'biru',
                                        CASE 
                                                WHEN cfg.rule_name > N'' AND (cfg.rule_name <> rul.rule_name OR rul.rule_name IS NULL) THEN CONCAT(N'EXEC sp_bindrule @rulename = N', QUOTENAME(cfg.rule_name, N''''), N', @objname = N''', REPLACE(QUOTENAME(cfg.schema_name) + N'.' + QUOTENAME(cfg.table_name) + N'.' + QUOTENAME(cfg.column_name), N'''', N''''''), N''';')
                                                ELSE NULL
                                        END,
                                        130,
                                        2
                                )
                ) AS act(action_code, sql_text, sort_order, phase)
WHERE           act.sql_text IS NOT NULL;

-- Add alter table alter column statements to the queue
WITH cteAlterColumn(entity, action_code, status_code, tag, command, datatype_name, max_length, precision_and_scale, collation_name, xml_collection_name, is_nullable, sort_order, phase)
AS (
        SELECT DISTINCT CONCAT(QUOTENAME(schema_name), N'.', QUOTENAME(table_name)) AS entity,
                        N'alco' AS action_code,
                        status_code,
                        tag,
                        CONCAT(N'ALTER TABLE ', QUOTENAME(schema_name), N'.', QUOTENAME(table_name), N' ALTER COLUMN ', QUOTENAME(column_name)) AS command,
                        CONCAT(N' ', QUOTENAME(datatype_name)) AS datatype_name,
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
                        END AS is_nullable,
                        110 AS sort_order,
                        2 AS phase
        FROM            #settings
)
INSERT  dbo.atac_queue
        (
                entity,
                action_code,
                status_code,
                sql_text,
                tag,
                sort_order,
                phase
        )
SELECT  entity,
        action_code,
        status_code,
        CONCAT(command, datatype_name, max_length, precision_and_scale, collation_name, xml_collection_name, is_nullable, N';') AS sql_text,
        tag,
        sort_order,
        phase
FROM    cteAlterColumn;

-- Cleanup
DROP TABLE      #settings;

-- Update duplicate status_code
WITH cteDuplicates(status_code, rnk)
AS (
        SELECT  status_code,
                ROW_NUMBER() OVER (PARTITION BY action_code, entity, sql_text ORDER BY queue_id) AS rnk
        FROM    dbo.atac_queue
        WHERE   status_code = N'L'
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

-- Release Disable database trigger
UPDATE  dbo.atac_queue
SET     status_code = N'R'
WHERE   action_code = N'didt'
        AND status_code = N'L';
GO
