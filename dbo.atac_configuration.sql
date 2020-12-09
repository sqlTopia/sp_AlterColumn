IF OBJECT_ID(N'dbo.atac_configuration', 'U') IS NOT NULL
        DROP TABLE dbo.atac_configuration;
GO
CREATE TABLE    dbo.atac_configuration
                (
                        schema_name SYSNAME NOT NULL CONSTRAINT ck_atac_configuration_schema_name CHECK (schema_name > N''),
                        table_name SYSNAME NOT NULL CONSTRAINT ck_atac_configuration_table_name CHECK (table_name > N''),
                        column_name SYSNAME NOT NULL CONSTRAINT ck_atac_configuration_column_name CHECK (column_name > N''),
                        tag NVARCHAR(36) NOT NULL CONSTRAINT df_atac_configuration_tag DEFAULT (N''),
                        CONSTRAINT pk_atac_configuration PRIMARY KEY CLUSTERED
                        (
                                schema_name,
                                table_name,
                                column_name,
                                tag
                        ),
                        new_column_name SYSNAME NULL CONSTRAINT ck_atac_configuration_new_column_name CHECK     (
                                                                                                                        new_column_name IS NULL         -- Inherit current column setting
                                                                                                                        OR new_column_name > N''        -- Change column name 
                                                                                                                ),
                        datatype_name SYSNAME NULL CONSTRAINT ck_atac_configuration_datatype_name CHECK (
                                                                                                                datatype_name IS NULL   -- Inherit current column setting
                                                                                                                OR datatype_name > N''  -- Set new datatype name
                                                                                                        ),
                        max_length NVARCHAR(4) NULL CONSTRAINT ck_atac_configuration_max_length CHECK   (
                                                                                                                max_length IS NULL                              -- Inherit current column setting
                                                                                                                OR max_length LIKE N'[1-9]'                     -- Set new max_length
                                                                                                                OR max_length LIKE N'[1-9][0-9]'                -- Set new max_length
                                                                                                                OR max_length LIKE N'[1-9][0-9][0-9]'           -- Set new max_length
                                                                                                                OR max_length LIKE N'[1-7][0-9][0-9][0-9]'      -- Set new max_length
                                                                                                                OR max_length = N'8000'                         -- Set new max_length
                                                                                                                OR max_length = N'MAX'                          -- Set new max_length
                                                                                                        ),
                        precision TINYINT NULL CONSTRAINT ck_atac_configuration_precision CHECK (
                                                                                                        precision IS NULL                       -- Inherit current column setting
                                                                                                        OR precision >= 1 AND precision <= 38   -- Set new precision
                                                                                                ),
                        scale TINYINT NULL CONSTRAINT ck_atac_configuration_scale CHECK (
                                                                                                scale IS NULL   -- Inherit current column setting
                                                                                                OR scale <= 38  -- Set new scale
                                                                                        ),
                        collation_name SYSNAME NULL CONSTRAINT ck_atac_configuration_collation_name CHECK       (
                                                                                                                        collation_name IS NULL  -- Inherit current setting
                                                                                                                        OR collation_name = N'' -- Remove current setting
                                                                                                                        OR collation_name > N'' -- Set new collation name
                                                                                                                ),
                        is_nullable NVARCHAR(3) NULL CONSTRAINT ck_atac_configuration_is_nullable CHECK (
                                                                                                                is_nullable IS NULL     -- Inherit current setting
                                                                                                                OR is_nullable = N'yes' -- Set column nullable
                                                                                                                OR is_nullable = N'no'  -- Set column non-nullable
                                                                                                        ),
                        xml_collection_name SYSNAME NULL CONSTRAINT ck_atac_configuration_xml_collection_name CHECK     (
                                                                                                                                xml_collection_name IS NULL     -- Inherit current setting
                                                                                                                                OR xml_collection_name = N''    -- Remove current setting
                                                                                                                                OR xml_collection_name > N''    -- Set new xml collection name
                                                                                                                        ),
                        default_name SYSNAME NULL CONSTRAINT ck_atac_configuration_default_name CHECK   (
                                                                                                                default_name IS NULL    -- Inherit current setting
                                                                                                                OR default_name = N''   -- Remove current setting
                                                                                                                OR default_name > N''   -- Set new default name
                                                                                                        ),
                        rule_name SYSNAME NULL CONSTRAINT ck_atac_configuration_rule_name CHECK (
                                                                                                        rule_name IS NULL       -- Inherit current setting
                                                                                                        OR rule_name = N''      -- Remove current setting
                                                                                                        OR rule_name > N''      -- Set new rule name
                                                                                                ),
                        log_code NCHAR(1) NULL CONSTRAINT ck_atac_configuration_log_code CHECK  (
                                                                                                        log_code IS NULL
                                                                                                        OR log_code = N'M'      -- Missing
                                                                                                        OR log_code = N'W'      -- Warning
                                                                                                        OR log_code = N'E'      -- Error
                                                                                                ),
                        log_text NVARCHAR(MAX) NULL,
                        CONSTRAINT ck_atac_configuration_precision_scale CHECK  (
                                                                                        precision IS NULL AND scale IS NULL     -- Other datatypes including user defined
                                                                                        OR precision IS NULL AND scale <= 7     -- Datetime2, DatetimeOffset and Time
                                                                                        OR precision >= scale                   -- Decimal and numeric
                                                                                ),
                        CONSTRAINT ck_atac_configuration_logtext CHECK  (
                                                                                log_code IS NULL AND log_text IS NULL
                                                                                OR log_code IS NOT NULL AND log_text > N''
                                                                        )
                );
GO
