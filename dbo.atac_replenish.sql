IF OBJECT_ID(N'dbo.atac_replenish', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE dbo.atac_replenish AS');
GO
ALTER PROCEDURE [dbo].[atac_replenish]
/*
        atac_replenish v21.01.01
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

-- Local helper variable
DECLARE @iteration INT = 0;

-- Local helper table
CREATE TABLE    #graphs
                (
                        table_id INT NOT NULL,
                        column_id INT NOT NULL,
                        tag NVARCHAR(36) COLLATE DATABASE_DEFAULT NOT NULL,
                        PRIMARY KEY CLUSTERED
                        (
                                table_id,
                                column_id,
                                tag
                        ),
                        iteration INT NOT NULL
                );

CREATE NONCLUSTERED INDEX ix_graphs ON #graphs (iteration);

-- Get all valid columns from configurations
INSERT          #graphs
                (
                        table_id,
                        column_id,
                        tag,
                        iteration
                )
SELECT          col.object_id AS table_id,
                col.column_id,
                cfg.tag,
                0 AS iteration
FROM            dbo.atac_configuration AS cfg
INNER JOIN      sys.schemas AS sch ON sch.name COLLATE DATABASE_DEFAULT = cfg.schema_name
INNER JOIN      sys.tables AS tbl ON tbl.schema_id = sch.schema_id
                        AND tbl.name COLLATE DATABASE_DEFAULT = cfg.table_name
                        AND tbl.type COLLATE DATABASE_DEFAULT = 'U'             -- Only normal tables allowed
INNER JOIN      sys.columns AS col ON col.object_id = tbl.object_id
                        AND col.name COLLATE DATABASE_DEFAULT = cfg.column_name;

-- Find all connected columns
WHILE ROWCOUNT_BIG() >= 1
        BEGIN
                SET     @iteration += 1;

                WITH cteGraphs(table_id, column_id, tag, iteration)
                AS (
                        -- Parent columns
                        SELECT          fkc.referenced_object_id AS table_id,
                                        fkc.referenced_column_id AS column_id,
                                        grp.tag,
                                        @iteration AS iteration
                        FROM            #graphs AS grp
                        INNER JOIN      sys.foreign_key_columns AS fkc ON fkc.parent_object_id = grp.table_id
                                                AND fkc.parent_column_id = grp.column_id
                        WHERE           grp.iteration = @iteration - 1

                        -- Take care of table self-referencing
                        UNION

                        -- Child columns
                        SELECT          fkc.parent_object_id AS table_id,
                                        fkc.parent_column_id AS column_id,
                                        grp.tag,
                                        @iteration AS iteration
                        FROM            #graphs AS grp
                        INNER JOIN      sys.foreign_key_columns AS fkc ON fkc.referenced_object_id = grp.table_id
                                                AND fkc.referenced_column_id = grp.column_id
                        WHERE           grp.iteration = @iteration - 1
                )
                MERGE   #graphs AS tgt
                USING   cteGraphs AS src ON src.table_id = tgt.table_id
                                AND src.column_id = tgt.column_id
                                AND src.tag = tgt.tag
                WHEN    NOT MATCHED BY TARGET
                        THEN    INSERT  (
                                                table_id,
                                                column_id,
                                                tag,
                                                iteration
                                        )
                                VALUES  (
                                                src.table_id,
                                                src.column_id,
                                                src.tag,
                                                src.iteration
                                        );
        END;

-- Replish configurations
WITH cteMetadata(schema_name, table_name, column_name, tag, datatype_name, max_length, precision, scale, collation_name, is_nullable, xml_collection_name, default_name, rule_name, log_code, log_text)
AS (
        SELECT          sch.name COLLATE DATABASE_DEFAULT AS schema_name,
                        tbl.name COLLATE DATABASE_DEFAULT AS table_name,
                        col.name COLLATE DATABASE_DEFAULT AS column_name,
                        grp.tag,
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
                        rul.name COLLATE DATABASE_DEFAULT AS rule_name,
                        N'W' AS log_code,
                        N'Configuration was automatically replenished.' AS log_text
        FROM            #graphs AS grp
        INNER JOIN      sys.columns AS col ON col.object_id = grp.table_id
                                AND col.column_id = grp.column_id
        INNER JOIN      sys.tables AS tbl ON tbl.object_id = col.object_id
        INNER JOIN      sys.schemas AS sch ON sch.schema_id = tbl.schema_id
        INNER JOIN      sys.types AS usr ON usr.user_type_id = col.user_type_id
        INNER JOIN      sys.types AS typ ON typ.user_type_id = col.system_type_id
        LEFT JOIN       sys.xml_schema_collections AS xsc ON xsc.xml_collection_id = col.xml_collection_id
        LEFT JOIN       sys.objects AS def ON def.object_id = col.default_object_id
        LEFT JOIN       sys.objects AS rul ON rul.object_id = col.rule_object_id
)
MERGE   dbo.atac_configuration AS tgt
USING   cteMetadata AS src ON src.schema_name = tgt.schema_name
                AND src.table_name = tgt.table_name
                AND src.column_name = tgt.column_name
                AND src.tag = tgt.tag
WHEN    NOT MATCHED BY TARGET
        THEN    INSERT  (
                                schema_name,
                                table_name,
                                column_name,
                                tag,
                                datatype_name,
                                max_length,
                                precision,
                                scale,
                                collation_name,
                                is_nullable,
                                xml_collection_name,
                                default_name,
                                rule_name,
                                log_code,
                                log_text
                        )
                VALUES  (
                                src.schema_name,
                                src.table_name,
                                src.column_name,
                                src.tag,
                                src.datatype_name,
                                src.max_length,
                                src.precision,
                                src.scale,
                                src.collation_name,
                                src.is_nullable,
                                src.xml_collection_name,
                                src.default_name,
                                src.rule_name,
                                src.log_code,
                                src.log_text
                        );

-- Clean up
DROP TABLE      #graphs;
GO
