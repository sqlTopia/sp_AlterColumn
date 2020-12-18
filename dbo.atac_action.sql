IF OBJECT_ID(N'fk_atac_queue_atac_action', 'F') IS NOT NULL
        BEGIN
                ALTER TABLE dbo.atac_queue DROP CONSTRAINT fk_atac_queue_atac_action;
                
                TRUNCATE TABLE  dbo.atac_queue;
        END;
GO
IF OBJECT_ID(N'dbo.atac_action', 'U') IS NOT NULL
        DROP TABLE dbo.atac_action;
GO
CREATE TABLE    dbo.atac_action
                (
                        code NCHAR(4) NOT NULL CONSTRAINT pk_atac_action PRIMARY KEY CLUSTERED CONSTRAINT ck_atac_action_code CHECK (code LIKE N'[a-z][a-z][a-z][a-z]'),
                        sort TINYINT NOT NULL,
                        description NVARCHAR(32) NOT NULL CONSTRAINT ck_atac_action_desciption CHECK(description > N''),
                        CONSTRAINT uq_atac_action UNIQUE (sort)
                );
GO
IF OBJECT_ID(N'dbo.atac_queue', 'U') IS NOT NULL
        BEGIN
                ALTER TABLE dbo.atac_queue WITH CHECK ADD CONSTRAINT fk_atac_queue_atac_action FOREIGN KEY (action_code) REFERENCES dbo.atac_action (code);
        END;
GO
