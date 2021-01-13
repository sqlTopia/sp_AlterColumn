IF OBJECT_ID(N'dbo.atac_populate', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE dbo.atac_populate AS');
GO
ALTER PROCEDURE dbo.atac_populate
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

IF EXISTS (SELECT * FROM dbo.atac_queue WHERE status_code <> N'L')
        BEGIN
                RAISERROR(N'Processing has already begun.', 16, 1);
                
                RETURN  -1000;
        END;
ELSE IF NOT EXISTS (SELECT * FROM dbo.atac_configuration)
        BEGIN
                RETURN  0;
        END;

-- Always validate
EXEC    dbo.atac_validate;

IF EXISTS (SELECT * FROM dbo.atac_configuration WHERE log_code = N'E')
        BEGIN
                RAISERROR(N'There at least one error in the configurations.', 16, 1);

                RETURN  -1100;
        END;

-- Get current configurations
DECLARE @settings TABLE
        (        
                schema_name SYSNAME NOT NULL,
                table_id INT NULL,
                table_name SYSNAME NOT NULL,
                column_id INT NULL,
                column_name SYSNAME NOT NULL,
                PRIMARY KEY CLUSTERED
                (
                        schema_name,
                        table_name,
                        column_name
                ),
                UNIQUE
                (
                        table_id,
                        column_id
                ),
                new_column_name SYSNAME NULL,
                datatype_name SYSNAME NOT NULL,
                max_length NVARCHAR(4) NULL,
                precision TINYINT NULL,
                scale TINYINT NULL,
                collation_name SYSNAME NULL,
                is_nullable NVARCHAR(3) NOT NULL,
                xml_collection_name SYSNAME NULL,
                datatype_default_name SYSNAME NULL,
                datatype_rule_name SYSNAME NULL
        );

INSERT          @settings
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
CROSS APPLY     dbo.sqltopia_column_metadata(cfg.schema_name, cfg.table_name, cfg.column_name) AS acm
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
                                )
OPTION          (RECOMPILE);

-- No changes detected
IF NOT EXISTS (SELECT * FROM @settings)
        BEGIN
                RETURN  0;
        END;

/*
        Start populating atac_queue 
*/

-- endt = Enable database triggers
-- didt = Disable database triggers
RAISERROR(N'Adding database trigger statements to atac_queue...', 10, 1) WITH NOWAIT;

INSERT  dbo.atac_queue
        (
                entity,
                action_code,
                status_code,
                sql_text,
                sort_order,
                phase
        )
SELECT  N'DATABASE' AS entity,
        trg.action_code,
        N'L' AS status_code,
        trg.sql_text,
        CASE
                WHEN trg.action_code = N'endt' THEN 250
                ELSE 0
        END AS sort_order,
        CASE
                WHEN trg.action_code = N'endt' THEN 6
                ELSE 1
        END AS phase
FROM    dbo.sqltopia_database_triggers() AS trg
WHERE   trg.action_code IN (N'endt', N'didt')
        AND trg.is_disabled = 0
        AND trg.is_ms_shipped = 0
        AND trg.sql_text > N''
OPTION  (RECOMPILE);

-- entg = Enable table triggers
-- ditg = Disable table triggers
RAISERROR(N'Adding table trigger statements to atac_queue...', 10, 1) WITH NOWAIT;

INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT DISTINCT CONCAT(QUOTENAME(trg.schema_name), N'.', QUOTENAME(trg.table_name)) AS entity,
                trg.action_code,
                N'L' AS status_code,
                trg.sql_text,
                CASE
                        WHEN trg.action_code = N'entg' THEN 230
                        ELSE 20
                END AS sort_order,
                CASE
                        WHEN trg.action_code = N'entg' THEN 5
                        ELSE 3
                END AS phase
FROM            @settings AS cfg        
CROSS APPLY     dbo.sqltopia_table_triggers(cfg.schema_name, cfg.table_name) AS trg
WHERE           trg.action_code IN (N'entg', N'ditg')
                AND trg.is_disabled = 0
                AND trg.is_ms_shipped = 0
                AND trg.sql_text > N''
OPTION          (RECOMPILE);

-- crfk = Create foreign keys
-- drfk = Drop foreign keys
-- difk = Disable foreign keys
RAISERROR(N'Adding foreign key statements to atac_queue...', 10, 1) WITH NOWAIT;

INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT DISTINCT CONCAT(QUOTENAME(fk.child_schema_name), N'.', QUOTENAME(fk.child_table_name)) AS entity,
                fk.action_code,
                N'L' AS status_code,
                fk.sql_text,
                CASE
                        WHEN fk.action_code = N'crfk' THEN 210
                        WHEN fk.action_code = N'drfk' THEN 10
                        ELSE 220
                END AS sort_order,
                CASE
                        WHEN fk.action_code = N'crfk' THEN 4
                        WHEN fk.action_code = N'drfk' THEN 2
                        ELSE 4
                END AS phase
FROM            @settings AS cfg
CROSS APPLY     dbo.sqltopia_foreign_keys(cfg.schema_name, cfg.table_name, cfg.column_name, cfg.new_column_name) AS fk
WHERE           (
                        fk.action_code IN (N'crfk', N'drfk')
                        OR fk.action_code = N'difk' AND fk.is_disabled = 1
                )
                AND fk.sql_text > N''
                AND fk.is_ms_shipped = 0
OPTION          (RECOMPILE);

-- crix = Create index
-- drix = Drop index
-- diix = Disable index
RAISERROR(N'Adding index statements to atac_queue...', 10, 1) WITH NOWAIT;

INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT DISTINCT CONCAT(QUOTENAME(ind.schema_name), N'.', QUOTENAME(ind.table_name)) AS entity,
                ind.action_code,
                N'L' AS status_code,
                ind.sql_text,
                CASE
                        WHEN ind.action_code = N'crix' THEN 190
                        WHEN ind.action_code = N'drix' THEN 30
                        ELSE 200
                END AS sort_order,
                3 AS phase
FROM            @settings AS cfg
CROSS APPLY     dbo.sqltopia_indexes(cfg.schema_name, cfg.table_name, cfg.column_name, cfg.new_column_name) AS ind
WHERE           (
                        ind.action_code IN (N'crix', N'drix')
                        OR ind.action_code = N'diix' AND ind.is_disabled = 1
                )
                AND ind.sql_text > N''
OPTION          (RECOMPILE);

-- crck = Create table check constraint
-- drck = Drop table check constraint
-- dick = Disable table check constraint
RAISERROR(N'Adding table check constraint statements to atac_queue...', 10, 1) WITH NOWAIT;

INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT DISTINCT CONCAT(QUOTENAME(chc.schema_name), N'.', QUOTENAME(chc.table_name)) AS entity,
                chc.action_code,
                N'L' AS status_code,
                chc.sql_text,
                CASE
                        WHEN chc.action_code = N'crck' THEN 170
                        WHEN chc.action_code = N'drck' THEN 40
                        ELSE 180
                END AS sort_order,
                3 AS phase
FROM            @settings AS cfg
CROSS APPLY     dbo.sqltopia_check_constraints(cfg.schema_name, cfg.table_name, cfg.column_name, cfg.new_column_name) AS chc
WHERE           (
                        chc.action_code IN (N'crck', N'drck')
                        OR chc.action_code IN (N'crck', N'drck') AND chc.is_disabled = 1
                )
                AND chc.sql_text > N''
OPTION          (RECOMPILE);

-- crdk = Create table default constraint
-- drdk = Drop table default constraint
RAISERROR(N'Adding table default constraint statements to atac_queue...', 10, 1) WITH NOWAIT;

INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(dfc.schema_name), N'.', QUOTENAME(dfc.table_name)) AS entity,
                dfc.action_code,
                N'L' AS status_code,
                dfc.sql_text,
                CASE
                        WHEN dfc.action_code = N'crdk' THEN 160
                        ELSE 50
                END AS sort_order,
                3 AS phase
FROM            @settings AS cfg
CROSS APPLY     dbo.sqltopia_default_constraints(cfg.schema_name, cfg.table_name, cfg.column_name, cfg.new_column_name) AS dfc
WHERE           dfc.action_code IN (N'crdk', N'drdk')
                AND dfc.sql_text > N''
OPTION          (RECOMPILE);

-- undf = Unbind column default
-- bidf = Bind column default
RAISERROR(N'Adding datatype column default statements to atac_queue...', 10, 1) WITH NOWAIT;

INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(def.schema_name), N'.', QUOTENAME(def.table_name)) AS entity,
                def.action_code,
                N'L' AS status_code,
                def.sql_text,
                CASE
                        WHEN def.action_code = N'bidf' THEN 140
                        ELSE 70
                END AS sort_order,
                3 AS phase
FROM            @settings AS cfg
CROSS APPLY     dbo.sqltopia_datatype_defaults(cfg.schema_name, cfg.table_name, cfg.column_name, cfg.new_column_name) AS def
WHERE           def.sql_text > N''
                AND     (
                                def.action_code = N'bidf' AND cfg.datatype_default_name > N''
                                OR def.action_code = N'undf' AND cfg.datatype_default_name >= N'' AND def.default_name > N''
                        )
OPTION          (RECOMPILE);

-- unru = Unbind column rule
-- biru = Bind column rule
RAISERROR(N'Adding datatype column rule statements to atac_queue...', 10, 1) WITH NOWAIT;

INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(rul.schema_name), N'.', QUOTENAME(rul.table_name)) AS entity,
                rul.action_code,
                N'L' AS status_code,
                rul.sql_text,
                CASE
                        WHEN rul.action_code = N'biru' THEN 130
                        ELSE 80
                END AS sort_order,
                3 AS phase
FROM            @settings AS cfg
CROSS APPLY     dbo.sqltopia_datatype_rules(cfg.schema_name, cfg.table_name, cfg.column_name, cfg.new_column_name) AS rul
WHERE           rul.sql_text > N''
                AND     (
                                rul.action_code = N'biru' AND cfg.datatype_rule_name > N''
                                OR rul.action_code = N'unru' AND cfg.datatype_rule_name >= N'' AND rul.rule_name > N''
                        )
OPTION          (RECOMPILE);

-- alco = Alter column
RAISERROR(N'Adding alter column statements to atac_queue...', 10, 1) WITH NOWAIT;

WITH cteColumn(schema_name, table_name, column_name, datatype_name, max_length, precision_and_scale, collation_name, xml_collection_name, is_nullable)
AS (
        SELECT  schema_name,
                table_name,
                column_name,
                datatype_name,
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
        FROM    @settings
)
INSERT  dbo.atac_queue
        (
                entity,
                action_code,
                status_code,
                sql_text,
                sort_order,
                phase
        )
SELECT  CONCAT(QUOTENAME(schema_name), N'.', QUOTENAME(table_name)) AS entity,
        N'alco' AS action_code,
        N'L' AS status_code,
        CONCAT(N'ALTER TABLE ', QUOTENAME(schema_name), N'.', QUOTENAME(table_name), N' ALTER COLUMN ', QUOTENAME(column_name), N' ', QUOTENAME(datatype_name), max_length, precision_and_scale, collation_name, xml_collection_name, is_nullable, N';') AS sql_text,
        100 AS sort_order,
        3 AS phase
FROM    cteColumn
OPTION  (RECOMPILE);

-- reco = Rename a column
RAISERROR(N'Adding column rename statements to atac_queue...', 10, 1) WITH NOWAIT;

INSERT  dbo.atac_queue
        (
                entity,
                action_code,
                status_code,
                sql_text,
                sort_order,
                phase
        )
SELECT  CONCAT(QUOTENAME(schema_name), N'.', QUOTENAME(table_name)) AS entity,
        N'reco' AS action_code,
        N'L' AS status_code,
        CONCAT(N'EXEC sys.sp_rename @objname = N''', REPLACE(QUOTENAME(schema_name) + N'.' + QUOTENAME(table_name) + N'.' + QUOTENAME(column_name), N'''', N''''''), N''', @newname = N', QUOTENAME(new_column_name, N''''), N', @objtype = N''COLUMN'';') AS sql_text,
        120 sort_order,
        3 AS phase
FROM    @settings
WHERE   new_column_name > N''
OPTION  (RECOMPILE);

-- remo = Refresh modules
RAISERROR(N'Adding module refresh statements to atac_queue...', 10, 1) WITH NOWAIT;

WITH cteModules(schema_name, module_name)
AS (
        SELECT          s.name COLLATE DATABASE_DEFAULT AS schema_name,
                        p.name COLLATE DATABASE_DEFAULT AS module_name
        FROM            @settings AS cfg
        INNER JOIN      sys.sql_expression_dependencies AS sed ON sed.referenced_id = cfg.table_id
                                AND sed.referencing_class_desc = N'OBJECT_OR_COLUMN'
        INNER JOIN      sys.procedures AS p ON p.object_id = sed.referencing_id
        INNER JOIN      sys.schemas AS s ON s.schema_id = p.schema_id

        UNION

        SELECT          s.name COLLATE DATABASE_DEFAULT AS schema_name,
                        v.name COLLATE DATABASE_DEFAULT AS module_name
        FROM            @settings AS cfg
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
SELECT DISTINCT CONCAT(QUOTENAME(schema_name), N'.', QUOTENAME(module_name)) AS entity,
                N'remo' AS action_code,
                N'L' AS status_code,
                CONCAT(N'EXEC sys.sp_refreshsqlmodule @name = N''', REPLACE(QUOTENAME(schema_name) + N'.' + QUOTENAME(module_name), N'''', N''''''), N''';') AS sql_text,
                240 AS sort_order,
                5 AS phase
FROM            cteModules
OPTION          (RECOMPILE);

-- Sort statements in correct processing order
WITH cteSort(statement_id, rnk)
AS (
        SELECT  statement_id,
                ROW_NUMBER() OVER (ORDER BY sort_order, entity, queue_id) AS rnk
        FROM    dbo.atac_queue
)
UPDATE  cteSort
SET     statement_id = rnk;

-- Release first statements in first phase
WITH cteReady(status_code, rnk)
AS (
        SELECT  status_code, 
                ROW_NUMBER() OVER (PARTITION BY entity ORDER BY phase, statement_id) AS rn
        FROM    dbo.atac_queue
)
UPDATE  cteReady
SET     status_code = N'R'
WHERE   rnk = 1;
GO
