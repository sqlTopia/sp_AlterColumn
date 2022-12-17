IF OBJECT_ID(N'dbo.atac_queue', 'U') IS NOT NULL
        DROP TABLE dbo.atac_queue;
GO
CREATE TABLE    dbo.atac_queue
                (
                        statement_id INT NOT NULL CONSTRAINT df_atac_queue_statement_id DEFAULT (1) CONSTRAINT ck_atac_queue_statement_id CHECK (statement_id >= 1),
                        action_code CHAR(4) NOT NULL,
                        session_id SMALLINT NULL,
                        status_code CHAR(1) NOT NULL CONSTRAINT ck_atac_queue_status_code CHECK (
                                                                                                           status_code = 'E'    -- Error
                                                                                                        OR status_code = 'F'    -- Finished
                                                                                                        OR status_code = 'L'    -- Locked
                                                                                                        OR status_code = 'R'    -- Ready
                                                                                                        OR status_code = 'W'    -- Working
                                                                                                ),
                        statement_start DATETIME2(3) NULL,
                        statement_end DATETIME2(3) NULL,
                        statement_time AS (DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, statement_start, COALESCE(statement_end, SYSDATETIME())), CAST('00:00:00.000' AS TIME(3)))),
                        log_text VARCHAR(MAX) NULL CONSTRAINT ck_atac_queue_log_text CHECK (log_text > ''),
                        queue_id INT IDENTITY(1, 1) NOT NULL,
                        sort_order SMALLINT NOT NULL,
                        entity VARCHAR(257) NOT NULL,
                        phase TINYINT NOT NULL,
                        sql_text VARCHAR(MAX) NOT NULL CONSTRAINT ck_atac_queue_sql_text CHECK (sql_text > ''),
                        CONSTRAINT ck_atac_queue_time CHECK     (
                                                                           statement_start IS NULL AND statement_end IS NULL
                                                                        OR statement_start IS NOT NULL AND statement_end IS NULL
                                                                        OR statement_start <= statement_end
                                                                ),
                        CONSTRAINT ck_atac_queue_action_code_sort_order_phase CHECK     (
                                                                                                   action_code = 'didt' AND sort_order =   0 AND phase = 0      -- Disable database triggers
                                                                                                OR action_code = 'drdt' AND sort_order =  10 AND phase = 1      -- Drop database triggers
                                                                                                OR action_code = 'ditg' AND sort_order =  20 AND phase = 1      -- Disable table triggers
                                                                                                OR action_code = 'drtg' AND sort_order =  30 AND phase = 1      -- Drop table triggers
                                                                                                OR action_code = 'difk' AND sort_order =  40 AND phase = 1      -- Disable foreign key
                                                                                                OR action_code = 'drfk' AND sort_order =  50 AND phase = 1      -- Drop foreign key
                                                                                                OR action_code = 'diix' AND sort_order =  60 AND phase = 1      -- Disable index
                                                                                                OR action_code = 'drix' AND sort_order =  70 AND phase = 1      -- Drop index
                                                                                                OR action_code = 'drvw' AND sort_order =  80 AND phase = 1      -- Drop view
                                                                                                OR action_code = 'drfn' AND sort_order =  90 AND phase = 1      -- Drop function
                                                                                                OR action_code = 'dick' AND sort_order = 100 AND phase = 1      -- Disable table check constraint
                                                                                                OR action_code = 'drck' AND sort_order = 110 AND phase = 1      -- Drop table check constraint
                                                                                                OR action_code = 'drdk' AND sort_order = 120 AND phase = 1      -- Drop table default constraint
                                                                                                OR action_code = 'drcc' AND sort_order = 130 AND phase = 1      -- Drop computed column
                                                                                                OR action_code = 'undf' AND sort_order = 140 AND phase = 1      -- Unbind column default
                                                                                                OR action_code = 'drdf' AND sort_order = 150 AND phase = 1      -- Drop column default
                                                                                                OR action_code = 'unru' AND sort_order = 160 AND phase = 1      -- Unbind column rule
                                                                                                OR action_code = 'drru' AND sort_order = 170 AND phase = 1      -- Drop column rule
                                                                                                OR action_code = 'aldb' AND sort_order = 180 AND phase = 2      -- Alter database
                                                                                                OR action_code = 'alco' AND sort_order = 190 AND phase = 3      -- Alter column
                                                                                                OR action_code = 'reco' AND sort_order = 200 AND phase = 4      -- Rename a column
                                                                                                OR action_code = 'tsql' AND sort_order = 210 AND phase = 5      -- User supplied t-sql statements
                                                                                                OR action_code = 'crru' AND sort_order = 220 AND phase = 6      -- Create column rule
                                                                                                OR action_code = 'biru' AND sort_order = 230 AND phase = 6      -- Bind column rule
                                                                                                OR action_code = 'crdf' AND sort_order = 240 AND phase = 6      -- Create column default
                                                                                                OR action_code = 'bidf' AND sort_order = 250 AND phase = 6      -- Bind column default
                                                                                                OR action_code = 'crcc' AND sort_order = 260 AND phase = 6      -- Create computed column
                                                                                                OR action_code = 'crdk' AND sort_order = 270 AND phase = 6      -- Create table default constraint
                                                                                                OR action_code = 'crck' AND sort_order = 280 AND phase = 6      -- Create table check constraint
                                                                                                OR action_code = 'enck' AND sort_order = 290 AND phase = 6      -- Enable table check constraint
                                                                                                OR action_code = 'crfn' AND sort_order = 300 AND phase = 6      -- Create function
                                                                                                OR action_code = 'crvw' AND sort_order = 310 AND phase = 6      -- Create view
                                                                                                OR action_code = 'crix' AND sort_order = 320 AND phase = 6      -- Create index
                                                                                                OR action_code = 'enix' AND sort_order = 330 AND phase = 6      -- Enable index
                                                                                                OR action_code = 'crfk' AND sort_order = 340 AND phase = 6      -- Create foreign key
                                                                                                OR action_code = 'enfk' AND sort_order = 350 AND phase = 6      -- Enable foreign key
                                                                                                OR action_code = 'crtg' AND sort_order = 360 AND phase = 6      -- Create table triggers
                                                                                                OR action_code = 'entg' AND sort_order = 370 AND phase = 6      -- Enable table triggers
                                                                                                OR action_code = 'cltb' AND sort_order = 380 AND phase = 7      -- Clean tables
                                                                                                OR action_code = 'remo' AND sort_order = 390 AND phase = 7      -- Refresh modules
                                                                                                OR action_code = 'crdt' AND sort_order = 400 AND phase = 8      -- Create database triggers
                                                                                                OR action_code = 'endt' AND sort_order = 410 AND phase = 9      -- Enable database triggers
                                                                                        ),
                        CONSTRAINT pk_atac_queue PRIMARY KEY NONCLUSTERED (queue_id),
                );
GO
CREATE NONCLUSTERED INDEX ix_atac_queue_status_code ON dbo.atac_queue (status_code, phase)
INCLUDE(session_id, action_code, statement_start, statement_end, sql_text, log_text, entity)
WHERE status_code IN ('E', 'L', 'R', 'W');
GO
CREATE NONCLUSTERED INDEX ix_atac_queue_phase ON dbo.atac_queue (phase)
GO
CREATE CLUSTERED INDEX ix_atac_queue_statement_id ON dbo.atac_queue (statement_id);
GO
