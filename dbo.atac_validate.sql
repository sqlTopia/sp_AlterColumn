IF OBJECT_ID(N'dbo.atac_validate', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE dbo.atac_validate AS');
GO
ALTER PROCEDURE dbo.atac_validate
/*
        atac_validate v21.01.01
        (C) 2009-2021, Peter Larsson
*/
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Exit if no configurations are found
IF NOT EXISTS (SELECT * FROM dbo.atac_configuration)
        BEGIN
                RETURN;
        END;

-- Replenish always
EXEC dbo.atac_replenish;

-- Local helper table
CREATE TABLE    #settings
                (
                        schema_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        table_id INT NOT NULL,
                        table_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        column_id INT NOT NULL,
                        column_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        PRIMARY KEY CLUSTERED
                        (
                                schema_name,
                                table_name,
                                column_name
                        ),
                        new_column_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        user_datatype_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        system_datatype_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        max_length NVARCHAR(4) COLLATE DATABASE_DEFAULT NULL,
                        precision TINYINT NULL,
                        scale TINYINT NULL,
                        collation_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        is_nullable NVARCHAR(3) COLLATE DATABASE_DEFAULT NOT NULL,
                        xml_collection_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        default_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        rule_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        graph_id INT NOT NULL,
                        node_count INT NOT NULL,
                        log_code NCHAR(1) NULL,
                        log_text NVARCHAR(MAX) NULL
                );

CREATE UNIQUE NONCLUSTERED INDEX uix_settings ON #settings (table_id, column_id) INCLUDE (graph_id);

-- Get valid configuration settings
INSERT          #settings
                (
                        schema_name,
                        table_id,
                        table_name,
                        column_id,
                        column_name,
                        user_datatype_name,
                        system_datatype_name,
                        max_length,
                        precision,
                        scale,
                        collation_name,
                        is_nullable,
                        xml_collection_name,
                        default_name,
                        rule_name,
                        graph_id,
                        node_count
                )
SELECT          sch.name COLLATE DATABASE_DEFAULT AS schema_name,
                tbl.object_id AS table_id,
                tbl.name COLLATE DATABASE_DEFAULT AS table_name,
                col.column_id,
                col.name COLLATE DATABASE_DEFAULT AS column_name,
                usr.name COLLATE DATABASE_DEFAULT AS user_datatype_name,
                typ.name COLLATE DATABASE_DEFAULT AS system_datatype_name,
                CASE
                        WHEN typ.name COLLATE DATABASE_DEFAULT IN (N'geography', N'geometry', N'image', N'ntext', N'sysname', N'text', N'xml') THEN cfg.max_length
                        WHEN col.max_length = -1 THEN COALESCE(cfg.max_length, CAST(N'MAX' AS NVARCHAR(4)))
                        WHEN typ.name COLLATE DATABASE_DEFAULT IN (N'nchar', N'nvarchar') THEN COALESCE(cfg.max_length, CAST(col.max_length / 2 AS NVARCHAR(4)))
                        WHEN typ.name COLLATE DATABASE_DEFAULT IN (N'binary', N'char', N'varbinary', N'varchar') THEN COALESCE(cfg.max_length, CAST(col.max_length AS NVARCHAR(4)))
                        ELSE cfg.max_length
                END AS max_length,
                CASE 
                        WHEN typ.name COLLATE DATABASE_DEFAULT IN (N'decimal', N'numeric') THEN COALESCE(cfg.precision, col.precision)
                        ELSE cfg.precision
                END AS precision,
                CASE 
                        WHEN typ.name COLLATE DATABASE_DEFAULT IN (N'datetime2', N'datetimeoffset', N'decimal', N'numeric', N'time') THEN COALESCE(cfg.scale, col.scale)
                        ELSE cfg.scale
                END AS scale,
                COALESCE(cfg.collation_name, col.name COLLATE DATABASE_DEFAULT) AS collation_name,
                CASE
                        WHEN col.is_nullable = 1 THEN COALESCE(cfg.is_nullable, CAST(N'yes' AS NVARCHAR(3)))
                        ELSE COALESCE(cfg.is_nullable, CAST(N'no' AS NVARCHAR(3)))
                END AS is_nullable,
                COALESCE(cfg.xml_collection_name, xsc.name COLLATE DATABASE_DEFAULT) AS xml_collection_name,
                COALESCE(cfg.default_name, def.name COLLATE DATABASE_DEFAULT) AS default_name,
                COALESCE(cfg.rule_name, rul.name COLLATE DATABASE_DEFAULT) AS rule_name,
                DENSE_RANK() OVER (ORDER BY col.object_id, col.column_id) AS graph_id,
                1 AS node_count
FROM            dbo.atac_configuration AS cfg
INNER JOIN      sys.schemas AS sch ON sch.name COLLATE DATABASE_DEFAULT = cfg.schema_name
INNER JOIN      sys.tables AS tbl ON tbl.name COLLATE DATABASE_DEFAULT = cfg.table_name
                        AND tbl.type COLLATE DATABASE_DEFAULT = 'U'
INNER JOIN      sys.columns AS col ON col.object_id = tbl.object_id
                        AND col.name COLLATE DATABASE_DEFAULT = cfg.column_name
INNER JOIN      sys.types AS usr ON usr.user_type_id = col.user_type_id
INNER JOIN      sys.types AS typ ON typ.user_type_id = col.system_type_id
LEFT JOIN       sys.xml_schema_collections AS xsc ON xsc.xml_collection_id = col.xml_collection_id
LEFT JOIN       sys.objects AS def ON def.object_id = col.default_object_id
LEFT JOIN       sys.objects AS rul ON rul.object_id = col.rule_object_id;

-- Loop until no more columns are found with foreign keys
WHILE ROWCOUNT_BIG() >= 1
        BEGIN
                WITH cteGraphs(table_id, column_id, graph_id)
                AS (
                        SELECT          fkc.referenced_object_id AS table_id,
                                        fkc.referenced_column_id AS column_id,
                                        cfg.graph_id
                        FROM            #settings AS cfg
                        INNER JOIN      sys.foreign_key_columns AS fkc ON fkc.parent_object_id = cfg.table_id
                                                AND fkc.parent_column_id = cfg.column_id

                        UNION

                        SELECT          fkc.parent_object_id AS table_id,
                                        fkc.parent_column_id AS column_id,
                                        cfg.graph_id
                        FROM            #settings AS cfg
                        INNER JOIN      sys.foreign_key_columns AS fkc ON fkc.referenced_object_id = cfg.table_id
                                                AND fkc.referenced_column_id = cfg.column_id
                )
                UPDATE          cfg
                SET             cfg.graph_id = cte.graph_id
                FROM            #settings AS cfg
                INNER JOIN      cteGraphs AS cte ON cte.table_id = cfg.table_id
                                        AND cte.column_id = cfg.column_id
                                        AND cte.graph_id < cfg.graph_id;
        END;

-- Calculate node count
WITH cteNodes(node_count, cnt)
AS (
        SELECT  node_count,
                COUNT(*) OVER (PARTITION BY graph_id) AS cnt
        FROM    #settings
)
UPDATE  cteNodes
SET     node_count = cnt
WHERE   cnt >= 2;

-- Always convert deprecated datatypes
UPDATE  #settings
SET     user_datatype_name =    CASE
                                        WHEN system_datatype_name = N'image' THEN N'varbinary'
                                        WHEN system_datatype_name = N'ntext' THEN N'nvarchar'
                                        ELSE N'varchar'
                                END,
        max_length = N'MAX',
        precision = NULL,
        scale = NULL,
        xml_collection_name = NULL,
        log_code = N'W',
        log_text = CONCAT(N'Configuration is changed from ', user_datatype_name, ' to ', system_datatype_name, '.')
WHERE   system_datatype_name IN (N'image', N'text', N'ntext');

-- Validate configurations regarding datatype, collation, xml collection, default and rule are having valid names
WITH cteInvalid(log_code, log_text, information)
AS (
        SELECT          cfg.log_code,
                        cfg.log_text,
                        CASE
                                WHEN typ.name IS NULL THEN N'Datatype name is invalid.'
                                WHEN hcl.name IS NULL AND cfg.collation_name > N'' THEN N'Collation name is invalid.'           -- Empty space will remove collation name
                                WHEN xml.name IS NULL AND cfg.xml_collection_name > N'' THEN N'XML collection name is invalid.' -- Empty space will remove xml collection name
                                WHEN def.name IS NULL AND cfg.default_name > N'' THEN N'Default name is invalid.'               -- Empty space will remove default name
                                WHEN rul.name IS NULL AND cfg.rule_name > N'' THEN N'Rule name is invalid.'                     -- Empty space will remove rule name
                                ELSE NULL
                        END AS information
        FROM            #settings AS cfg
        LEFT JOIN       sys.types AS typ ON typ.name COLLATE DATABASE_DEFAULT = cfg.user_datatype_name
        LEFT JOIN       sys.fn_helpcollations() AS hcl ON hcl.name COLLATE DATABASE_DEFAULT = cfg.collation_name
        LEFT JOIN       sys.xml_schema_collections AS xml ON xml.name COLLATE DATABASE_DEFAULT = cfg.xml_collection_name
        LEFT JOIN       sys.objects AS def ON def.name COLLATE DATABASE_DEFAULT = cfg.default_name
                                AND def.type COLLATE DATABASE_DEFAULT = 'D'
        LEFT JOIN       sys.objects AS rul ON rul.name COLLATE DATABASE_DEFAULT = cfg.rule_name
                                AND rul.type COLLATE DATABASE_DEFAULT = 'R'
        WHERE           cfg.log_code IS NULL
)
UPDATE  cteInvalid
SET     log_code = N'E',
        log_text = information
WHERE   log_code IS NULL
        AND information IS NOT NULL;

-- Check if new column name already exists in current table
UPDATE          cfg
SET             cfg.log_code = N'E',
                cfg.log_text = CONCAT(N'Column name ', cfg.new_column_name, N' conflicts with existing column in table ', cfg.schema_name, N'.', cfg.table_name, N'.')
FROM            #settings AS cfg
INNER JOIN      sys.columns AS col ON col.object_id = cfg.table_id
                        AND col.name COLLATE DATABASE_DEFAULT = cfg.new_column_name
WHERE           cfg.new_column_name > N''
                AND cfg.log_code IS NULL;

-- Validate fixed length datatypes
UPDATE          cfg
SET             cfg.max_length = NULL,
                cfg.precision = NULL,
                cfg.scale = NULL,
                cfg.collation_name = NULL,
                cfg.xml_collection_name =       CASE
                                                        WHEN cfg.system_datatype_name = N'xml' THEN cfg.xml_collection_name
                                                        ELSE NULL
                                                END
FROM            #settings AS cfg
WHERE           cfg.system_datatype_name IN (N'bigint', N'bit', N'date', N'datetime', N'float', N'geography', N'geometry', N'hierarchyid', N'int', N'money', N'real', N'smalldatetime', N'smallint', N'smallmoney', N'tinyint', N'sql_variant', N'sysname', N'timestamp', N'uniqueidentifier', N'xml')
                AND cfg.log_code IS NULL;

-- Validate datatypes with max_length only
UPDATE          cfg
SET             cfg.precision = NULL,
                cfg.scale = NULL,
                cfg.xml_collection_name = NULL,
                cfg.log_code =  CASE
                                        WHEN inf.msg IS NULL THEN NULL
                                        ELSE N'E'
                                END,
                cfg.log_text = inf.msg
FROM            #settings AS cfg
CROSS APPLY     (
                        SELECT  CASE
                                        WHEN cfg.system_datatype_name IN (N'binary', N'char') AND (cfg.max_length LIKE N'[1-9]' OR cfg.max_length LIKE N'[1-9][0-9]' OR cfg.max_length LIKE N'[1-9][0-9][0-9]' OR cfg.max_length LIKE N'[1-7][0-9][0-9][0-9]' OR cfg.max_length = N'8000') THEN NULL
                                        WHEN cfg.system_datatype_name = N'nchar' AND (cfg.max_length LIKE N'[1-9]' OR cfg.max_length LIKE N'[1-9][0-9]' OR cfg.max_length LIKE N'[1-9][0-9][0-9]' OR cfg.max_length LIKE N'[1-3][0-9][0-9][0-9]' OR cfg.max_length = N'4000') THEN NULL
                                        WHEN cfg.system_datatype_name = N'nvarchar' AND (cfg.max_length = N'MAX' OR cfg.max_length LIKE N'[1-9]' OR cfg.max_length LIKE N'[1-9][0-9]' OR cfg.max_length LIKE N'[1-9][0-9][0-9]' OR cfg.max_length LIKE N'[1-3][0-9][0-9][0-9]' OR cfg.max_length = N'4000') THEN NULL
                                        WHEN cfg.system_datatype_name IN (N'varbinary', N'varchar') AND (cfg.max_length = N'MAX' OR cfg.max_length LIKE N'[1-9]' OR cfg.max_length LIKE N'[1-9][0-9]' OR cfg.max_length LIKE N'[1-9][0-9][0-9]' OR cfg.max_length LIKE N'[1-7][0-9][0-9][0-9]' OR cfg.max_length = N'8000') THEN NULL
                                ELSE N'Invalid max_length.'
                        END 
                ) AS inf(msg)
WHERE           cfg.system_datatype_name IN (N'binary', N'char', N'nchar', N'nvarchar', N'varbinary', N'varchar')
                AND cfg.log_code IS NULL;

-- Validate datatypes with scale only
UPDATE          cfg
SET             cfg.max_length = NULL,
                cfg.precision = NULL,
                cfg.collation_name = NULL,
                cfg.xml_collection_name = NULL,
                cfg.log_code =  CASE
                                        WHEN cfg.scale <= 7 THEN NULL
                                        ELSE N'E'
                                END,
                cfg.log_text = inf.msg
FROM            #settings AS cfg
CROSS APPLY     (
                        SELECT  CASE
                                        WHEN cfg.scale <= 7 THEN NULL
                                        ELSE N'Invalid scale.'
                                END
                ) AS inf(msg)
WHERE           cfg.system_datatype_name IN (N'datetime2', N'datetimeoffset', N'time')
                AND cfg.log_code IS NULL;

-- Check datatypes with precision and scale only
UPDATE          cfg
SET             cfg.max_length = NULL,
                cfg.collation_name = NULL,
                cfg.xml_collection_name = NULL,
                cfg.log_code =  CASE
                                        WHEN inf.msg IS NULL THEN NULL
                                        ELSE N'E'
                                END,
                cfg.log_text = inf.msg
FROM            #settings AS cfg
CROSS APPLY     (
                        SELECT  CASE
                                        WHEN cfg.precision >= 1 AND precision <= 38 AND precision >= scale THEN NULL
                                        ELSE N'Invalid precision and scale.'
                                END
                ) AS inf(msg)
WHERE           cfg.system_datatype_name IN (N'decimal', N'numeric')
                AND cfg.log_code IS NULL;

-- Check indeterministic datatype name
WITH cteConfiguration(log_code, log_text, mi, mx, graph_id)
AS (
        SELECT  log_code,
                log_text,
                MIN(COALESCE(user_datatype_name, N'')) OVER (PARTITION BY graph_id) AS mi,
                MAX(COALESCE(user_datatype_name, N'')) OVER (PARTITION BY graph_id) AS mx,
                graph_id
        FROM    #settings
        WHERE   node_count >= 2
)
UPDATE  cteConfiguration
SET     log_code = N'W',
        log_text = CONCAT(N'(#', graph_id, N') Multiple datatype names within same foreign key chain.')
WHERE   mi < mx
        AND log_code IS NULL;

-- Check indeterministic max_length
WITH cteConfiguration(log_code, log_text, mi, mx, graph_id)
AS (
        SELECT  log_code,
                log_text,
                MIN(COALESCE(max_length, N'')) OVER (PARTITION BY graph_id) AS mi,
                MAX(COALESCE(max_length, N'')) OVER (PARTITION BY graph_id) AS mx,
                graph_id
        FROM    #settings
        WHERE   node_count >= 2
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = CONCAT(N'(#', graph_id, N') Multiple max_lengths within same foreign key chain.') 
WHERE   mi < mx
        AND log_code IS NULL;

-- Check indeterministic precision
WITH cteConfiguration(log_code, log_text, mi, mx, graph_id)
AS (
        SELECT  log_code,
                log_text,
                MIN(COALESCE(precision, -1)) OVER (PARTITION BY graph_id) AS mi,
                MAX(COALESCE(precision, -1)) OVER (PARTITION BY graph_id) AS mx,
                graph_id
        FROM    #settings
        WHERE   node_count >= 2
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = CONCAT(N'(#', graph_id, N') Multiple precisions within same foreign key chain.')
WHERE   mi < mx
        AND log_code IS NULL;

-- Check indeterministic scale
WITH cteConfiguration(log_code, log_text, mi, mx, graph_id)
AS (
        SELECT  log_code,
                log_text,
                MAX(COALESCE(scale, -1)) OVER (PARTITION BY graph_id) AS mx,
                MIN(COALESCE(scale, -1)) OVER (PARTITION BY graph_id) AS mi,
                graph_id
        FROM    #settings
        WHERE   node_count >= 2
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = CONCAT(N'(#', graph_id, N') Multiple scales within same foreign key chain.')
WHERE   mi < mx
        AND log_code IS NULL;

-- Check indeterministic collation name
WITH cteConfiguration(log_code, log_text, mi, mx, graph_id)
AS (
        SELECT  log_code,
                log_text,
                MIN(COALESCE(collation_name, N'')) OVER (PARTITION BY graph_id) AS mi,
                MAX(COALESCE(collation_name, N'')) OVER (PARTITION BY graph_id) AS mx,
                graph_id
        FROM    #settings
        WHERE   node_count >= 2
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = CONCAT(N'(#', graph_id, N') Multiple collation names within same foreign key chain.')
WHERE   mi < mx
        AND log_code IS NULL;

-- Check indeterministic xml collection name
WITH cteConfiguration(log_code, log_text, mi, mx, graph_id)
AS (
        SELECT  log_code,
                log_text,
                MIN(COALESCE(xml_collection_name, N'')) OVER (PARTITION BY graph_id) AS mi,
                MAX(COALESCE(xml_collection_name, N'')) OVER (PARTITION BY graph_id) AS mx,
                graph_id
        FROM    #settings
        WHERE   node_count >= 2
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = CONCAT(N'(#', graph_id, N') Multiple xml collection names within same foreign key chain.')
WHERE   mi < mx
        AND log_code IS NULL;

-- Update configurations settings
MERGE   dbo.atac_configuration AS tgt
USING   #settings AS src ON src.schema_name = tgt.schema_name
                AND src.table_name = tgt.table_name
                AND src.column_name = tgt.column_name
WHEN    MATCHED
        THEN    UPDATE
                SET     tgt.max_length = src.max_length,
                        tgt.precision = src.precision,
                        tgt.scale = src.scale,
                        tgt.collation_name = src.collation_name,
                        tgt.is_nullable = src.is_nullable,
                        tgt.xml_collection_name = src.xml_collection_name,
                        tgt.default_name = src.default_name,
                        tgt.rule_name = src.rule_name,
                        tgt.log_code = src.log_code,
                        tgt.log_text = src.log_text
WHEN    NOT MATCHED BY SOURCE
        THEN    UPDATE
                SET     tgt.log_code = N'W',
                        tgt.log_text = N'Configuration could not be validated.';

-- Clean up
DROP TABLE      #settings;
GO
