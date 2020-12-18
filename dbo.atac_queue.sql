IF OBJECT_ID(N'dbo.atac_queue', 'U') IS NOT NULL
        DROP TABLE dbo.atac_queue;
GO
CREATE TABLE    dbo.atac_queue
                (
                        statement_id INT NOT NULL CONSTRAINT df_atac_queue_statement_id DEFAULT (1) CONSTRAINT ck_atac_queue_statement_id CHECK (statement_id >= 1),
                        action_code NCHAR(4) NOT NULL,
                        session_id SMALLINT NULL,
                        status_code NCHAR(1) NOT NULL CONSTRAINT ck_atac_queue_status_code CHECK        (
                                                                                                                   status_code = N'E'   -- Error
                                                                                                                OR status_code = N'F'   -- Finished
                                                                                                                OR status_code = N'W'   -- Working
                                                                                                                OR status_code = N'L'   -- Locked
                                                                                                                OR status_code = N'R'   -- Ready
                                                                                                        ),
                        statement_start DATETIME2(3) NULL,
                        statement_end DATETIME2(3) NULL,
                        statement_time AS (DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, statement_start, COALESCE(statement_end, SYSDATETIME())), CAST('00:00:00' AS TIME(3)))),
                        log_text NVARCHAR(MAX) NULL CONSTRAINT ck_atac_queue_log_text CHECK (log_text IS NULL OR log_text > N''),
                        queue_id INT IDENTITY(1, 1) NOT NULL CONSTRAINT pk_atac_queue PRIMARY KEY NONCLUSTERED,
                        sort_order TINYINT NOT NULL,
                        entity NVARCHAR(392) NOT NULL,
                        phase TINYINT NOT NULL,
                        sql_text NVARCHAR(MAX) NOT NULL CONSTRAINT ck_atac_queue_sql_text CHECK (sql_text > N''),
                        INDEX cx_atac_queue CLUSTERED (phase, entity, statement_id),
                        CONSTRAINT ck_atac_queue_time CHECK     (
                                                                           statement_start IS NULL AND statement_end IS NULL
                                                                        OR statement_start IS NOT NULL AND statement_end IS NULL
                                                                        OR statement_start <= statement_end
                                                                ),
                        CONSTRAINT ck_atac_queue_action_code_sort_order_phase CHECK     (
                                                                                                   action_code = N'didt' AND sort_order =  10 AND phase = 1     -- Disable database triggers
                                                                                                OR action_code = N'ditg' AND sort_order =  20 AND phase = 2     -- Disable table triggers
                                                                                                OR action_code = N'drfk' AND sort_order =  30 AND phase = 2     -- Drop foreign key
                                                                                                OR action_code = N'drix' AND sort_order =  40 AND phase = 2     -- Drop index
                                                                                                OR action_code = N'drck' AND sort_order =  50 AND phase = 2     -- Drop table check constraint
                                                                                                OR action_code = N'drdk' AND sort_order =  60 AND phase = 2     -- Drop table default constraint
                                                                                                OR action_code = N'drcc' AND sort_order =  70 AND phase = 2     -- Drop computed column
                                                                                                OR action_code = N'undf' AND sort_order =  80 AND phase = 2     -- Unbind column default
                                                                                                OR action_code = N'unru' AND sort_order =  90 AND phase = 2     -- Unbind column rule
                                                                                                OR action_code = N'prfx' AND sort_order = 100 AND phase = 2     -- Any statement request by user to be run before alter column
                                                                                                OR action_code = N'alco' AND sort_order = 110 AND phase = 2     -- Alter column
                                                                                                OR action_code = N'sffx' AND sort_order = 120 AND phase = 2     -- Any statement request by user to be run after alter column
                                                                                                OR action_code = N'reco' AND sort_order = 130 AND phase = 2     -- Rename a column
                                                                                                OR action_code = N'biru' AND sort_order = 140 AND phase = 2     -- Bind column rule
                                                                                                OR action_code = N'bidf' AND sort_order = 150 AND phase = 2     -- Bind column default
                                                                                                OR action_code = N'crcc' AND sort_order = 160 AND phase = 2     -- Create computed column
                                                                                                OR action_code = N'crdk' AND sort_order = 170 AND phase = 2     -- Create table default constraint
                                                                                                OR action_code = N'crck' AND sort_order = 180 AND phase = 2     -- Create table check constraint
                                                                                                OR action_code = N'crix' AND sort_order = 190 AND phase = 2     -- Create index
                                                                                                OR action_code = N'crfk' AND sort_order = 200 AND phase = 3     -- Create foreign key
                                                                                                OR action_code = N'entg' AND sort_order = 210 AND phase = 3     -- Enable table triggers
                                                                                                OR action_code = N'remo' AND sort_order = 220 AND phase = 3     -- Refresh modules
                                                                                                OR action_code = N'endt' AND sort_order = 230 AND phase = 4     -- Enable database triggers
                                                                                        ),
                );
GO
