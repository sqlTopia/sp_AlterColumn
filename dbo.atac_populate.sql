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
                schema_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                table_id INT NULL,
                table_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
                column_id INT NULL,
                column_name SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
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
                                );

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
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT DISTINCT N'' AS entity,
                trg.action_code,
                N'L' AS status_code,
                trg.sql_text,
                CASE
                        WHEN trg.action_code = N'endt' THEN 230
                        ELSE 10
                END AS sort_order,
                CASE
                        WHEN trg.action_code = N'endt' THEN 4
                        ELSE 1
                END AS phase
FROM            dbo.sqltopia_database_triggers() AS trg
WHERE           trg.action_code IN (N'endt', N'didt')
                AND trg.is_disabled = 0
                AND trg.is_ms_shipped = 0
                AND trg.sql_text > N'';

-- entg = Enable table triggers
-- ditg = Disable table triggers
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
                        WHEN trg.action_code = N'entg' THEN 210
                        ELSE 20
                END AS sort_order,
                CASE
                        WHEN trg.action_code = N'entg' THEN 3
                        ELSE 2
                END AS phase
FROM            @settings AS cfg        
CROSS APPLY     dbo.sqltopia_table_triggers(cfg.schema_name, cfg.table_name) AS trg
WHERE           trg.action_code IN (N'entg', N'ditg')
                AND trg.is_disabled = 0
                AND trg.is_ms_shipped = 0
                AND trg.sql_text > N'';

/*
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

        -- Take care of self-referencing
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
*/

-- crix = Create index
-- drix = Drop index
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT          CONCAT(QUOTENAME(ind.schema_name), N'.', QUOTENAME(ind.table_name)) AS entity,
                ind.action_code,
                N'L' AS status_code,
                ind.sql_text,
                CASE
                        WHEN ind.action_code = N'crix' THEN 190
                        ELSE 40
                END AS sort_order,
                CASE
                        WHEN ind.action_code = N'crix' THEN 2
                        ELSE 2
                END AS phase
FROM            @settings AS cfg
CROSS APPLY     dbo.sqltopia_indexes(cfg.schema_name, cfg.table_name, cfg.column_name, cfg.new_column_name) AS ind
WHERE           ind.action_code IN (N'crix', N'drix');

-- crck = Create table check constraint
-- drck = Drop table check constraint
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
                        WHEN chc.action_code = N'crck' THEN 180
                        ELSE 50
                END AS sort_order,
                CASE
                        WHEN chc.action_code = N'crck' THEN 2
                        ELSE 2
                END AS phase
FROM            @settings AS cfg
CROSS APPLY     dbo.sqltopia_check_constraints(cfg.schema_name, cfg.table_name, cfg.column_name, cfg.new_column_name) AS chc
WHERE           chc.action_code IN (N'crck', N'drck')
                AND chc.sql_text > N'';

-- crdk = Create table default constraint
-- drdk = Drop table default constraint
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT DISTINCT CONCAT(QUOTENAME(dfc.schema_name), N'.', QUOTENAME(dfc.table_name)) AS entity,
                dfc.action_code,
                N'L' AS status_code,
                dfc.sql_text,
                CASE
                        WHEN dfc.action_code = N'crdk' THEN 170
                        ELSE 60
                END AS sort_order,
                CASE
                        WHEN dfc.action_code = N'crdk' THEN 2
                        ELSE 2
                END AS phase
FROM            @settings AS cfg
CROSS APPLY     dbo.sqltopia_check_constraints(cfg.schema_name, cfg.table_name, cfg.column_name, cfg.new_column_name) AS dfc
WHERE           dfc.action_code IN (N'crdk', N'drdk')
                AND dfc.sql_text > N'';


-- Add computed column statements to the queue
-- crdk = Create table default constraint
-- drdk = Drop table default constraint
INSERT          dbo.atac_queue
                (
                        entity,
                        action_code,
                        status_code,
                        sql_text,
                        sort_order,
                        phase
                )
SELECT DISTINCT CONCAT(QUOTENAME(col.schema_name), N'.', QUOTENAME(col.table_name)) AS entity,
                col.action_code,
                N'L' AS status_code,
                col.sql_text,
                CASE
                        WHEN col.action_code = N'crcc' THEN 160
                        ELSE 70
                END AS sort_order,
                CASE
                        WHEN col.action_code = N'crcc' THEN 2
                        ELSE 2
                END AS phase
FROM            @settings AS cfg
CROSS APPLY     dbo.sqltopia_computed_columns(cfg.schema_name, cfg.table_name, cfg.column_name, cfg.new_column_name) AS col
WHERE           col.action_code IN (N'crcc', N'drcc')
                AND col.sql_text > N'';

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
                                                WHEN cte.old_default_name > N'' THEN CONCAT(N'EXEC sys.sp_unbinddefault @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.table_name) + N'.' + QUOTENAME(cte.column_name), N'''', N''''''), N''';') 
                                                ELSE NULL
                                        END,
                                        80,
                                        2
                                ),
                                (
                                        N'bidf',
                                        CASE 
                                                WHEN cte.datatype_default_name > N'' THEN CONCAT(N'EXEC sys.sp_binddefault @rulename = N', QUOTENAME(cte.datatype_default_name, N''''), N', @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.table_name) + N'.' + QUOTENAME(COALESCE(cte.new_column_name, cte.column_name)), N'''', N''''''), N''';')
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
        LEFT JOIN       sys.objects AS rul ON rul.object_id = usr.rule_object_id
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
                                                WHEN cte.old_rule_name > N'' THEN CONCAT(N'EXEC sys.sp_unbindrule @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.table_name) + N'.' + QUOTENAME(cte.column_name), N'''', N''''''), N''';') 
                                                ELSE NULL
                                        END,
                                        90,
                                        2
                                ),
                                (
                                        N'biru',
                                        CASE 
                                                WHEN cte.datatype_rule_name > N'' THEN CONCAT(N'EXEC sys.sp_bindrule @rulename = N', QUOTENAME(cte.datatype_rule_name, N''''), N', @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.table_name) + N'.' + QUOTENAME(COALESCE(cte.new_column_name, cte.column_name)), N'''', N''''''), N''';')
                                                ELSE NULL
                                        END,
                                        140,
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
                                        CONCAT(N'ALTER TABLE ', QUOTENAME(cte.schema_name), N'.', QUOTENAME(cte.table_name), N' ALTER COLUMN ', QUOTENAME(cte.column_name), N' ', QUOTENAME(cte.datatype_name), cte.max_length, cte.precision_and_scale, cte.collation_name, cte.xml_collection_name, cte.is_nullable, N';'),
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
                                        CONCAT(N'EXEC sys.sp_rename @objname = N''', REPLACE(QUOTENAME(cte.schema_name) + N'.' + QUOTENAME(cte.table_name) + N'.' + QUOTENAME(cte.column_name), N'''', N''''''), N''', @newname = N', QUOTENAME(cte.new_column_name, N''''), N', @objtype = N''COLUMN'';'),
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
                ROW_NUMBER() OVER (ORDER BY sort_order, entity, queue_id) AS rnk
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
END;
GO
