IF OBJECT_ID(N'dbo.atac_configurations', 'U') IS NOT NULL
        DROP TABLE dbo.atac_configurations;
GO
CREATE TABLE    atac_configurations
                (
                        tag VARCHAR(36) NOT NULL CONSTRAINT df_atac_configurations_tag DEFAULT (''),
                        table_name VARCHAR(257) NOT NULL CONSTRAINT ck_atac_configurations_table_name CHECK (table_name > ''),
                        column_name VARCHAR(128) NOT NULL CONSTRAINT ck_atac_configurations_column_name CHECK (column_name > ''),
                        new_column_name VARCHAR(128) NULL CONSTRAINT ck_atac_configurations_new_column_name CHECK       (
                                                                                                                                   new_column_name IS NULL      -- Inherit current column setting
                                                                                                                                OR new_column_name > ''         -- Change column name 
                                                                                                                        ),
                        is_nullable VARCHAR(5) NULL CONSTRAINT ck_atac_configurations_is_nullable CHECK (
                                                                                                                   is_nullable IS NULL          -- Inherit current setting
                                                                                                                OR is_nullable = 'true'         -- Set column nullable
                                                                                                                OR is_nullable = 'false'        -- Set column non-nullable
                                                                                                        ),
                        datatype_name VARCHAR(128) NULL CONSTRAINT ck_atac_configurations_datatype_name CHECK   (
                                                                                                                           datatype_name IS NULL        -- Inherit current column setting
                                                                                                                        OR datatype_name > ''           -- Set new datatype name
                                                                                                                ),
                        max_length VARCHAR(4) NULL CONSTRAINT ck_atac_configurations_max_length CHECK   (
                                                                                                                   max_length IS NULL                           -- Inherit current column setting
                                                                                                                OR max_length LIKE '[1-9]'                      -- Set new max_length
                                                                                                                OR max_length LIKE '[1-9][0-9]'                 -- Set new max_length
                                                                                                                OR max_length LIKE '[1-9][0-9][0-9]'            -- Set new max_length
                                                                                                                OR max_length LIKE '[1-7][0-9][0-9][0-9]'       -- Set new max_length
                                                                                                                OR max_length = '8000'                          -- Set new max_length
                                                                                                                OR max_length = 'MAX'                           -- Set new max_length
                                                                                                        ),
                        precision TINYINT NULL CONSTRAINT ck_atac_configurations_precision CHECK        (
                                                                                                                   precision IS NULL                    -- Inherit current column setting
                                                                                                                OR precision >= 1 AND precision <= 38   -- Set new precision
                                                                                                        ),
                        scale TINYINT NULL CONSTRAINT ck_atac_configurations_scale CHECK        (
                                                                                                           scale IS NULL        -- Inherit current column setting
                                                                                                        OR scale <= 38          -- Set new scale
                                                                                                ),
                        collation_name VARCHAR(128) NULL CONSTRAINT ck_atac_configurations_collation_name CHECK (
                                                                                                                           collation_name IS NULL       -- Inherit current setting
                                                                                                                        OR collation_name > ''          -- Set new collation name
                                                                                                                ),
                        xml_collection_name VARCHAR(257) NULL CONSTRAINT ck_atac_configurations_xml_collection_name CHECK       (
                                                                                                                                           xml_collection_name IS NULL  -- Inherit current setting
                                                                                                                                        OR xml_collection_name = ''     -- Remove current setting
                                                                                                                                        OR xml_collection_name > ''     -- Set new xml collection name with 2-part naming
                                                                                                                                ),
                        datatype_default_name VARCHAR(128) NULL CONSTRAINT ck_atac_configurations_default_name CHECK    (
                                                                                                                                   datatype_default_name IS NULL        -- Inherit current setting
                                                                                                                                OR datatype_default_name = ''           -- Remove current setting
                                                                                                                                OR datatype_default_name > ''           -- Set new default name
                                                                                                                        ),
                        datatype_rule_name VARCHAR(128) NULL CONSTRAINT ck_atac_configurations_rule_name CHECK  (
                                                                                                                           datatype_rule_name IS NULL   -- Inherit current setting
                                                                                                                        OR datatype_rule_name = ''      -- Remove current setting
                                                                                                                        OR datatype_rule_name > ''      -- Set new rule name
                                                                                                                ),
                        log_text VARCHAR(MAX) NULL CONSTRAINT ck_atac_configurations_log_text CHECK (log_text > ''),
                        CONSTRAINT ck_atac_configurations_table_name_column_name CHECK (COLUMNPROPERTY(OBJECT_ID(table_name, 'U'), column_name, 'ColumnId') IS NOT NULL),
                        CONSTRAINT ck_atac_configurations_datatype_name_max_length_precision_scale CHECK        (
                                                                                                                           datatype_name IN ('binary', 'char') AND (max_length IS NULL OR max_length LIKE '[1-9]' OR max_length LIKE '[1-9][0-9]' OR max_length LIKE '[1-9][0-9][0-9]' OR max_length LIKE '[1-7][0-9][0-9][0-9]' OR max_length = '8000') AND precision IS NULL AND scale IS NULL
                                                                                                                        OR datatype_name IN ('datetime2', 'datetimeoffset', 'time') AND max_length IS NULL AND precision IS NULL AND (scale IS NULL OR scale BETWEEN 0 AND 7)
                                                                                                                        OR datatype_name IN ('decimal', 'numeric') AND max_length IS NULL AND (precision IS NULL OR precision <= 38) AND (scale IS NULL OR scale <= precision)
                                                                                                                        OR datatype_name = 'nchar' AND (max_length IS NULL OR max_length LIKE '[1-9]' OR max_length LIKE '[1-9][0-9]' OR max_length LIKE '[1-9][0-9][0-9]' OR max_length LIKE '[1-3][0-9][0-9][0-9]' OR max_length = '4000') AND precision IS NULL AND scale IS NULL
                                                                                                                        OR datatype_name = 'nvarchar' AND (max_length IS NULL OR max_length LIKE '[1-9]' OR max_length LIKE '[1-9][0-9]' OR max_length LIKE '[1-9][0-9][0-9]' OR max_length LIKE '[1-3][0-9][0-9][0-9]' OR max_length = '4000' OR max_length = 'MAX') AND precision IS NULL AND scale IS NULL
                                                                                                                        OR datatype_name IN ('varbinary', 'varchar') AND (max_length IS NULL OR max_length LIKE '[1-9]' OR max_length LIKE '[1-9][0-9]' OR max_length LIKE '[1-9][0-9][0-9]' OR max_length LIKE '[1-7][0-9][0-9][0-9]' OR max_length = '8000' OR max_length = 'MAX') AND precision IS NULL AND scale IS NULL
                                                                                                                        OR max_length IS NULL AND precision IS NULL AND scale IS NULL
                                                                                                                ),
                        CONSTRAINT ck_atac_configurations_datatype_name_collation_name CHECK    (
                                                                                                           datatype_name IS NULL AND collation_name IS NULL
                                                                                                        OR datatype_name IN ('bigint', 'binary', 'bit', 'date', 'datetime', 'datetime2', 'datetimeoffset', 'decimal', 'float', 'geography', 'geometry', 'hierarchyid', 'image', 'int', 'money', 'numeric', 'real', 'smalldatetime', 'smallint', 'smallmoney', 'sql_variant', 'time', 'timestamp', 'tinyint', 'uniqueidentifier', 'varbinary', 'xml') AND collation_name IS NULL
                                                                                                        OR datatype_name NOT IN ('bigint', 'binary', 'bit', 'date', 'datetime', 'datetime2', 'datetimeoffset', 'decimal', 'float', 'geography', 'geometry', 'hierarchyid', 'image', 'int', 'money', 'numeric', 'real', 'smalldatetime', 'smallint', 'smallmoney', 'sql_variant', 'time', 'timestamp', 'tinyint', 'uniqueidentifier', 'varbinary', 'xml')
                                                                                                ),
                        CONSTRAINT ck_atac_configurations_datatype_name_xml_collection_name CHECK       (
                                                                                                                   datatype_name IS NULL AND xml_collection_name IS NULL
                                                                                                                OR datatype_name = 'xml'
                                                                                                                OR datatype_name != 'xml' AND xml_collection_name IS NULL
                                                                                                        ),
                        CONSTRAINT pk_atac_configurations PRIMARY KEY CLUSTERED (table_name, column_name, tag)
                );
GO
