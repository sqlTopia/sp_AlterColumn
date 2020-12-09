IF OBJECT_ID(N'dbo.atac_queue', 'U') IS NOT NULL
        DROP TABLE dbo.atac_queue;
GO
CREATE TABLE    dbo.atac_queue
                /*
                        atac_queue v21.01.01
                        (C) 2009-2021, Peter Larsson
                */
                (
                        statement_id INT NOT NULL CONSTRAINT df_atac_queue_statement_id DEFAULT (0) CONSTRAINT ck_atac_queue_statement_id CHECK (statement_id >= 0),
                        action_code NCHAR(4) NOT NULL CONSTRAINT ck_atac_queue_action_code CHECK (action_code > N''),
                        session_id SMALLINT NULL,
                        status_code NCHAR(1) NOT NULL CONSTRAINT df_atac_queue_status_code DEFAULT (N'L') CONSTRAINT ck_atac_queue_status_code CHECK    (
                                                                                                                                                                status_code = N'E'      -- Error in configuration (column not found or column is in table type)
                                                                                                                                                                OR status_code = N'D'   -- Duplicate configuration (generated statement for the configuration is a duplicate)
                                                                                                                                                                OR status_code = N'I'   -- Ignored configuration (configuration has no change for column metadata)
                                                                                                                                                                OR status_code = N'F'   -- Finished (statement is executed ok)
                                                                                                                                                                OR status_code = N'W'   -- Working (statement is being executed)
                                                                                                                                                                OR status_code = N'L'   -- Locked (statement is not available at this time)
                                                                                                                                                                OR status_code = N'R'   -- Ready (statement is ready to be executed)
                                                                                                                                                        ),
                        tag NVARCHAR(36) NOT NULL,
                        statement_start DATETIME2(3) NULL,
                        statement_end DATETIME2(3) NULL,
                        statement_time AS (DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, statement_start, statement_end), CAST('00:00:00' AS TIME(3)))),
                        log_text NVARCHAR(MAX) NULL CONSTRAINT ck_atac_queue_log_text CHECK (log_text IS NULL OR log_text > N''),
                        queue_id INT IDENTITY(1, 1) NOT NULL,
                        sort_order INT NOT NULL,
                        entity NVARCHAR(392) NOT NULL,
                        sql_text NVARCHAR(MAX) NOT NULL CONSTRAINT ck_atac_queue_sql_text CHECK (sql_text > N''),
                        CONSTRAINT bk_atac_queue PRIMARY KEY CLUSTERED (statement_id, queue_id),
                        CONSTRAINT ck_atac_queue_time CHECK     (
                                                                        statement_start <= statement_end
                                                                        OR statement_start IS NOT NULL AND statement_end IS NULL
                                                                        OR statement_start IS NULL AND statement_end IS NULL
                                                                ),
                        CONSTRAINT ck_atac_queue_action_code_sort_order CHECK   (
                                                                                        action_code = N'didt' AND sort_order = 10       -- Disable database triggers
                                                                                        OR action_code = N'ditg' AND sort_order = 20    -- Disable table triggers
                                                                                        OR action_code = N'drfk' AND sort_order = 30    -- Drop foreign key
                                                                                        OR action_code = N'drix' AND sort_order = 40    -- Drop index
                                                                                        OR action_code = N'drck' AND sort_order = 50    -- Drop table check constraint
                                                                                        OR action_code = N'drdk' AND sort_order = 60    -- Drop table default constraint
                                                                                        OR action_code = N'drcc' AND sort_order = 70    -- Drop computed column
                                                                                        OR action_code = N'undf' AND sort_order = 80    -- Unbind column default
                                                                                        OR action_code = N'unru' AND sort_order = 90    -- Unbind column rule
                                                                                        OR action_code = N'pref' AND sort_order = 100   -- Any statement request by user to be run before alter column
                                                                                        OR action_code = N'alco' AND sort_order = 110   -- Alter column
                                                                                        OR action_code = N'suff' AND sort_order = 120   -- Any statement request by user to be run after alter column
                                                                                        OR action_code = N'biru' AND sort_order = 130   -- Bind column rule
                                                                                        OR action_code = N'bidf' AND sort_order = 140   -- Bind column default
                                                                                        OR action_code = N'crcc' AND sort_order = 150   -- Create computed column
                                                                                        OR action_code = N'crdk' AND sort_order = 160   -- Create table default constraint
                                                                                        OR action_code = N'crck' AND sort_order = 170   -- Create table check constraint
                                                                                        OR action_code = N'crix' AND sort_order = 180   -- Create index
                                                                                        OR action_code = N'crfk' AND sort_order = 190   -- Create foreign key
                                                                                        OR action_code = N'entg' AND sort_order = 200   -- Enable table triggers
                                                                                        OR action_code = N'reco' AND sort_order = 210   -- Rename column
                                                                                        OR action_code = N'endt' AND sort_order = 220   -- Enable database triggers
                                                                                )
                );
GO
