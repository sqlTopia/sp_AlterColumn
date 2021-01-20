IF OBJECT_ID(N'dbo.atac_validate', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE dbo.atac_validate AS');
GO
ALTER PROCEDURE dbo.atac_validate
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Exit if no configurations are found
IF NOT EXISTS (SELECT * FROM dbo.atac_configuration)
        BEGIN
                RETURN;
        END;

-- Clear logging
UPDATE  dbo.atac_configuration
SET     log_code = NULL,
        log_text = NULL;

-- Always replenish
EXEC    dbo.atac_replenish;

-- Local helper table
CREATE TABLE    #settings
                (
                        schema_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        table_id INT NOT NULL,
                        table_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        column_id INT NOT NULL,
                        column_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        tag NVARCHAR(36) COLLATE DATABASE_DEFAULT NOT NULL,
                        PRIMARY KEY CLUSTERED
                        (
                                table_id,
                                column_id,
                                tag
                        ),
                        new_column_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        is_user_defined BIT NOT NULL,
                        datatype_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        system_datatype_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                        max_length NVARCHAR(4) COLLATE DATABASE_DEFAULT NULL,
                        precision TINYINT NULL,
                        scale TINYINT NULL,
                        collation_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        is_nullable NVARCHAR(3) COLLATE DATABASE_DEFAULT NOT NULL,
                        xml_collection_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        datatype_default_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        datatype_rule_name SYSNAME COLLATE DATABASE_DEFAULT NULL,
                        graph_id INT NOT NULL,
                        node_count INT NOT NULL,
                        log_code NCHAR(1) NULL,
                        log_text NVARCHAR(MAX) NULL
                );

-- Get current configuration settings
INSERT          #settings
                (
                        schema_name,
                        table_id,
                        table_name,
                        column_id,
                        column_name,
                        tag,
                        is_user_defined,
                        datatype_name,
                        system_datatype_name,
                        max_length,
                        precision,
                        scale,
                        collation_name,
                        is_nullable,
                        xml_collection_name,
                        datatype_default_name,
                        datatype_rule_name,
                        graph_id,
                        node_count
                )
SELECT          cfg.schema_name,
                tbl.object_id AS table_id,
                cfg.table_name,
                col.column_id,
                cfg.column_name,
                cfg.tag,
                usr.is_user_defined,
                usr.name COLLATE DATABASE_DEFAULT AS datatype_name,
                typ.name COLLATE DATABASE_DEFAULT AS system_datatype_name,
                CASE
                        WHEN usr.is_user_defined = 1 THEN NULL
                        WHEN usr.name COLLATE DATABASE_DEFAULT IN (N'nvarchar', N'varbinary', N'varchar') AND col.max_length = -1 THEN N'MAX'
                        WHEN usr.name COLLATE DATABASE_DEFAULT IN (N'binary', N'char', N'varbinary', N'varchar') THEN col.max_length
                        WHEN usr.name COLLATE DATABASE_DEFAULT IN (N'nchar', N'nvarchar') THEN col.max_length / 2
                        ELSE NULL
                END AS max_length,
                CASE 
                        WHEN usr.is_user_defined = 1 THEN NULL
                        WHEN usr.name COLLATE DATABASE_DEFAULT IN (N'decimal', N'numeric') THEN col.precision
                        ELSE NULL
                END AS precision,
                CASE 
                        WHEN usr.is_user_defined = 1 THEN NULL
                        WHEN usr.name COLLATE DATABASE_DEFAULT IN (N'datetime2', N'datetimeoffset', N'decimal', N'numeric', N'time') THEN col.scale
                        ELSE NULL
                END AS scale,
                col.collation_name AS collation_name,
                CASE
                        WHEN col.is_nullable = 1 THEN N'yes'
                        ELSE N'no'
                END AS is_nullable,
                xsc.name COLLATE DATABASE_DEFAULT AS xml_collection_name,
                def.name COLLATE DATABASE_DEFAULT AS datatype_default_name,
                rul.name COLLATE DATABASE_DEFAULT AS datatype_rule_name,
                DENSE_RANK() OVER (ORDER BY col.object_id, col.column_id) AS graph_id,
                1 AS node_count
FROM            dbo.atac_configuration AS cfg
INNER JOIN      sys.schemas AS sch ON sch.name COLLATE DATABASE_DEFAULT = cfg.schema_name
INNER JOIN      sys.tables AS tbl ON tbl.schema_id = sch.schema_id
                        AND tbl.name COLLATE DATABASE_DEFAULT = cfg.table_name
                        AND tbl.type COLLATE DATABASE_DEFAULT = 'U'
INNER JOIN      sys.columns AS col ON col.object_id = tbl.object_id
                        AND col.name COLLATE DATABASE_DEFAULT = cfg.column_name
INNER JOIN      sys.types AS usr ON usr.user_type_id = col.user_type_id
INNER JOIN      sys.types AS typ ON typ.user_type_id = col.system_type_id
LEFT JOIN       sys.xml_schema_collections AS xsc ON xsc.xml_collection_id = col.xml_collection_id
LEFT JOIN       sys.objects AS def ON def.object_id = col.default_object_id
LEFT JOIN       sys.objects AS rul ON rul.object_id = col.rule_object_id;

-- Get wanted configuration settings
UPDATE          tgt
SET             tgt.new_column_name = NULLIF(cfg.new_column_name, cfg.column_name),
                tgt.is_user_defined = COALESCE(usr.is_user_defined, tgt.is_user_defined),
                tgt.datatype_name = COALESCE(usr.name COLLATE DATABASE_DEFAULT, tgt.datatype_name),
                tgt.system_datatype_name = COALESCE(typ.name COLLATE DATABASE_DEFAULT, tgt.system_datatype_name),
                tgt.max_length =        CASE
                                                WHEN usr.is_user_defined = 1 THEN NULL
                                                ELSE cfg.max_length
                                        END,
                tgt.precision = CASE 
                                        WHEN usr.is_user_defined = 1 THEN NULL
                                        ELSE cfg.precision
                                END,
                tgt.scale =     CASE 
                                        WHEN usr.is_user_defined = 1 THEN NULL
                                        ELSE cfg.scale
                                END,
                tgt.collation_name =    CASE
                                                WHEN usr.is_user_defined = 1 THEN NULL
                                                ELSE COALESCE(cfg.collation_name, tgt.collation_name COLLATE DATABASE_DEFAULT)
                                        END,
                tgt.is_nullable = COALESCE(cfg.is_nullable, tgt.is_nullable),
                tgt.xml_collection_name = COALESCE(cfg.xml_collection_name, tgt.xml_collection_name),
                tgt.datatype_default_name = COALESCE(cfg.datatype_default_name, tgt.datatype_default_name),
                tgt.datatype_rule_name = COALESCE(cfg.datatype_rule_name, tgt.datatype_rule_name)
FROM            #settings AS tgt
INNER JOIN      dbo.atac_configuration AS cfg ON cfg.schema_name = tgt.schema_name
                        AND cfg.table_name = tgt.table_name
                        AND cfg.column_name = tgt.column_name
                        AND cfg.tag = tgt.tag
INNER JOIN      sys.columns AS col ON col.object_id = tgt.table_id
                        AND col.column_id = tgt.column_id
LEFT JOIN       sys.types AS usr ON usr.name COLLATE DATABASE_DEFAULT = cfg.datatype_name
LEFT JOIN       sys.types AS typ ON typ.user_type_id = usr.system_type_id

UPDATE  #settings
SET     collation_name = CAST(DATABASEPROPERTYEX(DB_NAME(), N'Collation') AS SYSNAME)
WHERE   collation_name = N'DATABASE_DEFAULT';

-- Loop until no more columns are found with foreign keys
WHILE ROWCOUNT_BIG() >= 1
        BEGIN
                WITH cteGraphs(table_id, column_id, graph_id)
                AS (
                        -- Parent columns
                        SELECT          fkc.referenced_object_id AS table_id,
                                        fkc.referenced_column_id AS column_id,
                                        cfg.graph_id
                        FROM            #settings AS cfg
                        INNER JOIN      sys.foreign_key_columns AS fkc ON fkc.parent_object_id = cfg.table_id
                                                AND fkc.parent_column_id = cfg.column_id

                        -- Take care self-referencing
                        UNION

                        -- Child columns
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

-- Calculate node count (could be duplicated due to tags)
WITH cteNodeCount(graph_id)
AS (
        SELECT          graph_id
        FROM            #settings
        GROUP BY        graph_id,
                        table_id,
                        column_id
), cteGraphCount(graph_id, node_count)
AS (
        SELECT          graph_id,
                        COUNT(*) AS node_count
        FROM            cteNodeCount
        GROUP BY        graph_id
        HAVING          COUNT(*) >= 2
)
UPDATE          cfg
SET             cfg.node_count = cte.node_count
FROM            #settings AS cfg
INNER JOIN      cteGraphCount AS cte ON cte.graph_id = cfg.graph_id;

-- Always convert deprecated datatypes
UPDATE  #settings
SET     datatype_name = CASE
                                WHEN system_datatype_name = N'image' THEN N'varbinary'
                                WHEN system_datatype_name = N'ntext' THEN N'nvarchar'
                                ELSE N'varchar'
                        END,
        system_datatype_name =  CASE
                                        WHEN system_datatype_name = N'image' THEN N'varbinary'
                                        WHEN system_datatype_name = N'ntext' THEN N'nvarchar'
                                        ELSE N'varchar'
                                END,
        max_length = N'MAX',
        precision = NULL,
        scale = NULL,
        collation_name =        CASE
                                        WHEN system_datatype_name = N'image' THEN NULL
                                        ELSE collation_name
                                END,
        xml_collection_name = NULL,
        log_code = N'W',
        log_text = CONCAT(N'Configuration is changed from ', system_datatype_name, ' to ', datatype_name, '(max).')
WHERE   system_datatype_name IN (N'image', N'text', N'ntext');

-- Adjust fixed length datatypes
UPDATE  #settings
SET     max_length = NULL,
        precision = NULL,
        scale = NULL,
        collation_name =        CASE
                                        WHEN datatype_name = N'sysname' THEN collation_name
                                        ELSE NULL
                                END,
        xml_collection_name =   CASE
                                        WHEN system_datatype_name = N'xml' THEN xml_collection_name
                                        ELSE NULL
                                END
WHERE   system_datatype_name IN (N'bigint', N'bit', N'date', N'datetime', N'float', N'geography', N'geometry', N'hierarchyid', N'int', N'money', N'real', N'smalldatetime', N'smallint', N'smallmoney', N'tinyint', N'sql_variant', N'timestamp', N'uniqueidentifier', N'xml')
        OR datatype_name = N'sysname';

-- Validate configurations regarding datatype, collation, xml collection, default and rule are having valid names
WITH cteInvalid(log_code, log_text, msgtxt)
AS (
        SELECT          cfg.log_code,
                        cfg.log_text,
                        CASE
                                WHEN typ.name IS NULL THEN N'Datatype name is invalid.'
                                WHEN hcl.name IS NULL AND cfg.collation_name > N'' THEN N'Collation name is invalid.'                   -- Empty space will remove collation name
                                WHEN xml.name IS NULL AND cfg.xml_collection_name > N'' THEN N'XML collection name is invalid.'         -- Empty space will remove xml collection name
                                WHEN def.name IS NULL AND cfg.datatype_default_name > N'' THEN N'Datatype default name is invalid.'     -- Empty space will remove default name
                                WHEN rul.name IS NULL AND cfg.datatype_rule_name > N'' THEN N'Datatype rule name is invalid.'           -- Empty space will remove rule name
                                ELSE NULL
                        END AS msgtxt
        FROM            #settings AS cfg
        LEFT JOIN       sys.types AS typ ON typ.name COLLATE DATABASE_DEFAULT = cfg.datatype_name
        LEFT JOIN       sys.fn_helpcollations() AS hcl ON hcl.name COLLATE DATABASE_DEFAULT = cfg.collation_name
        LEFT JOIN       sys.xml_schema_collections AS xml ON xml.name COLLATE DATABASE_DEFAULT = cfg.xml_collection_name
        LEFT JOIN       sys.objects AS def ON def.name COLLATE DATABASE_DEFAULT = cfg.datatype_default_name
                                AND def.type COLLATE DATABASE_DEFAULT = 'D'
        LEFT JOIN       sys.objects AS rul ON rul.name COLLATE DATABASE_DEFAULT = cfg.datatype_rule_name
                                AND rul.type COLLATE DATABASE_DEFAULT = 'R'
)
UPDATE  cteInvalid
SET     log_code = N'E',
        log_text = msgtxt
WHERE   msgtxt IS NOT NULL;

-- Check if new column name already exists in current table
UPDATE          cfg
SET             cfg.log_code = N'E',
                cfg.log_text = CONCAT(N'Column name ', cfg.new_column_name, N' conflicts with existing column in table ', cfg.schema_name, N'.', cfg.table_name, N'.')
FROM            #settings AS cfg
INNER JOIN      sys.columns AS col ON col.object_id = cfg.table_id
                        AND col.name COLLATE DATABASE_DEFAULT = cfg.new_column_name
WHERE           cfg.new_column_name > N'';

-- Adjust and validate datatypes with max_length only
UPDATE          cfg
SET             cfg.max_length =        CASE
                                                WHEN cfg.is_user_defined = 1 THEN NULL
                                                WHEN cfg.datatype_name = N'sysname' THEN NULL
                                                ELSE cfg.max_length
                                        END,
                cfg.precision = NULL,
                cfg.scale = NULL,
                cfg.xml_collection_name = NULL,
                cfg.log_code =  CASE
                                        WHEN cfg.is_user_defined = 1 THEN cfg.log_code
                                        WHEN cfg.datatype_name = N'sysname' THEN cfg.log_code
                                        WHEN inf.msgtxt IS NULL THEN cfg.log_code
                                        ELSE N'E'
                                END,
                cfg.log_text =  CASE
                                        WHEN cfg.is_user_defined = 1 THEN cfg.log_text
                                        WHEN cfg.datatype_name = N'sysname' THEN cfg.log_text
                                        WHEN inf.msgtxt IS NULL THEN cfg.log_text
                                        ELSE inf.msgtxt
                                END
FROM            #settings AS cfg
CROSS APPLY     (
                        SELECT  CASE
                                        WHEN cfg.datatype_name IN (N'binary', N'char') AND (cfg.max_length LIKE N'[1-9]' OR cfg.max_length LIKE N'[1-9][0-9]' OR cfg.max_length LIKE N'[1-9][0-9][0-9]' OR cfg.max_length LIKE N'[1-7][0-9][0-9][0-9]' OR cfg.max_length = N'8000') THEN NULL
                                        WHEN cfg.datatype_name = N'nchar' AND (cfg.max_length LIKE N'[1-9]' OR cfg.max_length LIKE N'[1-9][0-9]' OR cfg.max_length LIKE N'[1-9][0-9][0-9]' OR cfg.max_length LIKE N'[1-3][0-9][0-9][0-9]' OR cfg.max_length = N'4000') THEN NULL
                                        WHEN cfg.datatype_name = N'nvarchar' AND (cfg.max_length LIKE N'[1-9]' OR cfg.max_length LIKE N'[1-9][0-9]' OR cfg.max_length LIKE N'[1-9][0-9][0-9]' OR cfg.max_length LIKE N'[1-3][0-9][0-9][0-9]' OR cfg.max_length = N'4000' OR cfg.max_length = N'MAX') THEN NULL
                                        WHEN cfg.datatype_name IN (N'varbinary', N'varchar') AND (cfg.max_length LIKE N'[1-9]' OR cfg.max_length LIKE N'[1-9][0-9]' OR cfg.max_length LIKE N'[1-9][0-9][0-9]' OR cfg.max_length LIKE N'[1-7][0-9][0-9][0-9]' OR cfg.max_length = N'8000' OR cfg.max_length = N'MAX') THEN NULL
                                ELSE N'Invalid max_length.'
                        END 
                ) AS inf(msgtxt)
WHERE           cfg.system_datatype_name IN (N'binary', N'char', N'nchar', N'nvarchar', N'varbinary', N'varchar');

-- Adjust and validate datatypes with precision and scale only
UPDATE  #settings
SET     max_length = NULL,
        precision =     CASE
                                WHEN is_user_defined = 1 THEN NULL
                                ELSE precision
                        END,
        scale = CASE
                        WHEN is_user_defined = 1 THEN NULL
                        ELSE scale
                END,
        collation_name = NULL,
        xml_collection_name = NULL,
        log_code =      CASE
                                WHEN is_user_defined = 1 THEN log_code
                                WHEN precision IS NULL OR scale IS NULL THEN N'E'
                                ELSE log_code
                        END,
        log_text =      CASE
                                WHEN is_user_defined = 1 THEN log_text
                                WHEN precision IS NULL OR scale IS NULL THEN N'Invalid precision and scale.'
                                ELSE log_text
                        END
WHERE   system_datatype_name IN (N'decimal', N'numeric');

-- Adjust and validate datatypes with scale only
UPDATE  #settings
SET     max_length = NULL,
        precision = NULL,
        scale = CASE
                        WHEN is_user_defined = 1 THEN NULL
                        ELSE scale
                END,
        collation_name = NULL,
        xml_collection_name = NULL,
        log_code =      CASE
                                WHEN is_user_defined = 1 THEN log_code
                                WHEN scale <= 7 THEN log_code
                                ELSE N'E'
                        END,
        log_text =      CASE
                                WHEN is_user_defined = 1 THEN log_text
                                WHEN scale <= 7 THEN log_text
                                ELSE N'Invalid scale.'
                        END
WHERE   system_datatype_name IN (N'datetime2', N'datetimeoffset', N'time');

-- Check indeterministic new_column_name
WITH cteConfiguration(log_code, log_text, mi, mx)
AS (
        SELECT  log_code,
                log_text,
                MIN(new_column_name) OVER (PARTITION BY table_id, column_id) AS mi,
                MAX(new_column_name) OVER (PARTITION BY table_id, column_id) AS mx
        FROM    #settings
        WHERE   new_column_name IS NOT NULL
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = N'Configuration has multiple new column name in table.'
WHERE   mi < mx;

WITH cteConfiguration(log_code, log_text, mi, mx)
AS (
        SELECT  log_code,
                log_text,
                MIN(column_id) OVER (PARTITION BY table_id, new_column_name) AS mi,
                MAX(column_id) OVER (PARTITION BY table_id, new_column_name) AS mx
        FROM    #settings
        WHERE   new_column_name IS NOT NULL
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = N'Configuration has same new column name for multiple columns in table.'
WHERE   mi < mx;

-- Check indeterministic datatype name
WITH cteConfiguration(log_code, log_text, mi, mx, graph_id)
AS (
        SELECT  log_code,
                log_text,
                MIN(COALESCE(datatype_name, N'')) OVER (PARTITION BY graph_id, tag) AS mi,
                MAX(COALESCE(datatype_name, N'')) OVER (PARTITION BY graph_id, tag) AS mx,
                graph_id
        FROM    #settings
        WHERE   node_count >= 2
)
UPDATE  cteConfiguration
SET     log_code = N'W',
        log_text = CONCAT(N'(#', graph_id, N') Multiple datatype names within same foreign key chain.')
WHERE   mi < mx
        AND log_code IS NULL;

WITH cteConfiguration(log_code, log_text, mi, mx, graph_id, tag)
AS (
        SELECT  log_code,
                log_text,
                MIN(COALESCE(datatype_name, N'')) OVER (PARTITION BY graph_id) AS mi,
                MAX(COALESCE(datatype_name, N'')) OVER (PARTITION BY graph_id) AS mx,
                graph_id,
                tag
        FROM    #settings
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = CONCAT(N'(#', graph_id, CASE WHEN tag > N'' THEN N'/' + tag ELSE N'' END, N') Multiple datatype names between configuration tags.')
WHERE   mi < mx;

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
WHERE   mi < mx;

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
WHERE   mi < mx;

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
WHERE   mi < mx;

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
WHERE   mi < mx;

-- Check indeterministic xml collection name
WITH cteConfiguration(log_code, log_text, mi, mx, graph_id)
AS (
        SELECT  log_code,
                log_text,
                MIN(COALESCE(xml_collection_name, N'')) OVER (PARTITION BY table_id, column_id) AS mi,
                MAX(COALESCE(xml_collection_name, N'')) OVER (PARTITION BY table_id, column_id) AS mx,
                graph_id
        FROM    #settings
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = CONCAT(N'(#', graph_id, N') Multiple xml collection names for same column.')
WHERE   mi < mx;

-- Check indeterministic datatype_default_name
WITH cteConfiguration(log_code, log_text, mi, mx)
AS (
        SELECT  log_code,
                log_text,
                MIN(datatype_default_name) OVER (PARTITION BY table_id, column_id) AS mi,
                MAX(datatype_default_name) OVER (PARTITION BY table_id, column_id) AS mx
        FROM    #settings
        WHERE   datatype_default_name IS NOT NULL
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = N'Configuration has multiple datatype default name on column.'
WHERE   mi < mx;

-- Check indeterministic datatype_rule_name
WITH cteConfiguration(log_code, log_text, mi, mx)
AS (
        SELECT  log_code,
                log_text,
                MIN(datatype_rule_name) OVER (PARTITION BY table_id, column_id) AS mi,
                MAX(datatype_rule_name) OVER (PARTITION BY table_id, column_id) AS mx
        FROM    #settings
        WHERE   datatype_rule_name IS NOT NULL
)
UPDATE  cteConfiguration
SET     log_code = N'E',
        log_text = N'Configuration has multiple datatype rule name on column.'
WHERE   mi < mx;

-- Update configurations settings
MERGE   dbo.atac_configuration AS tgt
USING   #settings AS src ON src.schema_name = tgt.schema_name
                AND src.table_name = tgt.table_name
                AND src.column_name = tgt.column_name
WHEN    MATCHED
        THEN    UPDATE
                SET     tgt.new_column_name = src.new_column_name,
                        tgt.datatype_name = src.datatype_name,
                        tgt.max_length = src.max_length,
                        tgt.precision = src.precision,
                        tgt.scale = src.scale,
                        tgt.collation_name = src.collation_name,
                        tgt.is_nullable = src.is_nullable,
                        tgt.xml_collection_name = src.xml_collection_name,
                        tgt.datatype_default_name = src.datatype_default_name,
                        tgt.datatype_rule_name = src.datatype_rule_name,
                        tgt.log_code = src.log_code,
                        tgt.log_text = src.log_text
WHEN    NOT MATCHED BY SOURCE
        THEN    UPDATE
                SET     tgt.log_code = N'M',
                        tgt.log_text = N'Could not validate configuration due to missing schema name or table name or column name.';

-- Clean up
DROP TABLE      #settings;
GO
