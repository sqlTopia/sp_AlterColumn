IF SCHEMA_ID(N'tools') IS NULL
        EXEC(N'CREATE SCHEMA tools;');
GO
IF OBJECT_ID(N'tools.usp_atac_process', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE tools.usp_atac_process AS');
GO
SET ANSI_NULL, QUOTED_IDENTIFIER ON;
GO
ALTER PROCEDURE tools.usp_atac_process
(
        @process_statements INT = 2147483647,
        @maximum_retry_count TINYINT = 100,
        @wait_time TIME(3) = '00:00:00.250'
)
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Local helper variables
DECLARE @statement_id INT,
        @sql_text VARCHAR(MAX),
        @entity VARCHAR(257) = '',
        @current_phase TINYINT = 0,
        @max_phase TINYINT,
        @action_code CHAR(4),
        @current_retry_count TINYINT = 0,
        @error_number INT,
        @start_time DATETIME2(3),
        @end_time DATETIME2(3),
        @elapsed INT,
        @delay CHAR(12) = @wait_time,
        @is_main_task_done BIT;

-- Elevate permissions
IF HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'ALTER') IS NULL OR HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'ALTER') = 0
        BEGIN
                RAISERROR('You are not allowed to alter database.', 18, 1);

                RETURN  -1000;
        END;

-- Validate user supplied input parameters
IF @process_statements = 0
        BEGIN
                RAISERROR('You opted out by chosing @process_statements = 0.', 10, 1);

                RETURN;
        END;
ELSE IF @process_statements IS NULL
        BEGIN
                SET     @process_statements = 2147483647;
        END;
ELSE IF @process_statements < 0
        BEGIN
                RAISERROR('Number of process statements must be between 1 and 2147383647.', 16, 1, @process_statements);

                RETURN  -1100;
        END;

IF @maximum_retry_count IS NULL OR @maximum_retry_count > 100
        BEGIN
                SET     @maximum_retry_count = 100;
        END;

-- Local helper table
DECLARE @process TABLE
        (
                statement_id INT NOT NULL PRIMARY KEY CLUSTERED,
                entity VARCHAR(257) NOT NULL,
                action_code CHAR(4) NOT NULL,
                sql_text VARCHAR(MAX) NOT NULL
        );

BEGIN TRY
        -- Get current phase
        SELECT TOP(1)   @current_phase = taq.phase
        FROM            tools.atac_queue AS taq
        WHERE           taq.status_code = 'R'
        ORDER BY        taq.statement_id
        OPTION          (MAXDOP 1);

        -- Get current maximum phase
        SELECT TOP(1)   @max_phase = taq.phase
        FROM            tools.atac_queue AS taq
        ORDER BY        taq.phase DESC
        OPTION          (MAXDOP 1);

        -- Keep iterating as long as there are statements to be executed
        WHILE EXISTS(SELECT * FROM tools.atac_queue AS taq WHERE taq.status_code IN ('L', 'R'))
                BEGIN
                        -- Get next statement ordered by phase and statement_id
                        WHILE @current_phase <= @max_phase
                                BEGIN
                                        DELETE  p
                                        FROM    @process AS p;

                                        WITH cte_queue(statement_id, status_code, session_id, entity, action_code, statement_start, statement_end, sql_text, log_text)
                                        AS (
                                                SELECT TOP(1)   taq.statement_id,
                                                                taq.status_code,
                                                                taq.session_id,
                                                                taq.entity,
                                                                taq.action_code,
                                                                taq.statement_start,
                                                                taq.statement_end,
                                                                taq.sql_text,
                                                                taq.log_text
                                                FROM            tools.atac_queue AS taq
                                                WHERE           taq.status_code = 'R'
                                                                AND taq.phase = @current_phase
                                                ORDER BY        taq.statement_id
                                        )
                                        UPDATE  cte
                                        SET     cte.status_code = 'W',
                                                cte.session_id = @@SPID,
                                                cte.statement_start = SYSDATETIME(),
                                                cte.statement_end = NULL,
                                                cte.log_text = NULL
                                        OUTPUT  inserted.statement_id,
                                                inserted.entity,
                                                inserted.action_code,
                                                inserted.sql_text
                                        INTO    @process
                                                (
                                                        statement_id,
                                                        entity,
                                                        action_code,
                                                        sql_text
                                                )
                                        FROM    cte_queue AS cte
                                        OPTION  (MAXDOP 1);

                                        -- Process statement if found
                                        IF @@ROWCOUNT >= 1
                                                BEGIN
                                                        SELECT  @statement_id = statement_id,
                                                                @sql_text = sql_text,
                                                                @entity = entity,
                                                                @action_code = action_code,
                                                                @current_retry_count = 0
                                                        FROM    @process;

                                                        -- Execute statement
                                                        WHILE @current_retry_count <= @maximum_retry_count
                                                                BEGIN
                                                                        SELECT  @error_number = 0,
                                                                                @is_main_task_done = 0;

                                                                        BEGIN TRY
                                                                                -- Excute current statement
                                                                                RAISERROR('ABARUN %s', 10, 1, @sql_text) WITH NOWAIT;

                                                                                IF @is_main_task_done = 0
                                                                                        BEGIN
                                                                                                SET     @start_time = SYSDATETIME();

                                                                                                EXEC    (@sql_text);

                                                                                                SET     @is_main_task_done = 1;

                                                                                                SET     @end_time = SYSDATETIME();

                                                                                                SET     @elapsed = DATEDIFF(SECOND, @start_time, @end_time);

                                                                                                IF @elapsed >= 60
                                                                                                        BEGIN
                                                                                                                RAISERROR(N'Msg 0, Level 8, Elapsed: %i seconds', 10, 1, @elapsed) WITH NOWAIT;
                                                                                                        END;
                                                                                        END;

                                                                                -- Update processed and end time
                                                                                UPDATE  taq
                                                                                SET     taq.status_code = 'F',
                                                                                        taq.statement_end = @end_time,
                                                                                        taq.log_text = NULL
                                                                                FROM    tools.atac_queue AS taq
                                                                                WHERE   taq.statement_id = @statement_id
                                                                                OPTION  (MAXDOP 1);

                                                                                -- Decrease execution counter
                                                                                SET     @process_statements -= 1;

                                                                                -- Exit retry loop
                                                                                BREAK;
                                                                        END TRY
                                                                        BEGIN CATCH
                                                                                SET     @error_number = ERROR_NUMBER();

                                                                                UPDATE  taq 
                                                                                SET     taq.status_code = 'E',
                                                                                        taq.log_text = CONCAT('(', ERROR_NUMBER(), ') ', ERROR_MESSAGE())
                                                                                FROM    tools.atac_queue AS taq
                                                                                WHERE   taq.statement_id = @statement_id
                                                                                OPTION  (MAXDOP 1);

                                                                                IF @action_code NOT IN ('cltb', 'remo')
                                                                                        BEGIN
                                                                                                UPDATE  taq
                                                                                                SET     taq.status_code = 'E',
                                                                                                        taq.log_text = CONCAT('An earlier execution for same entity went wrong (statement #', @statement_id, ').')
                                                                                                FROM    dbo.atac_queue AS taq
                                                                                                WHERE   taq.statement_id > @statement_id
                                                                                                        AND taq.entity = @entity
                                                                                                OPTION  (MAXDOP 1);
                                                                                        END;
                                                                        END CATCH;

                                                                        IF @error_number = 1203                         -- Preemptive unlock.
                                                                                SET     @current_retry_count += 1;
                                                                        ELSE IF @error_number = 1204                    -- SQL Server cannot obtain a lock resource.
                                                                                SET     @current_retry_count += 1;
                                                                        ELSE IF @error_number = 1205                    -- Resources are accessed in conflicting order on separate transactions, causing a deadlock.
                                                                                SET     @current_retry_count += 1;
                                                                        ELSE IF @error_number = 1222                    -- Another transaction held a lock on a required resource longer than this query could wait for it.
                                                                                SET     @current_retry_count += 1;
                                                                        ELSE
                                                                                BEGIN
                                                                                        RAISERROR('Msg 0, Level 16, A new complication has occured. Please report error number to sp_AlterColumn developer.', 10, 1);

                                                                                        BREAK;
                                                                                END;

                                                                        -- Display information about retry attempt
                                                                        RAISERROR('Msg 0, Level 16, Retry attempt %d.', 10, 1, @current_retry_count) WITH NOWAIT;

                                                                        IF @current_retry_count > @maximum_retry_count
                                                                                BEGIN
                                                                                        RAISERROR('Msg 0, Level 16, Maximum retry count %d is reached.', 18, 1, @maximum_retry_count) WITH NOWAIT;

                                                                                        RETURN  -2000;
                                                                                END;

                                                                        WAITFOR DELAY   @delay;
                                                                END;

                                                        -- Exit iteration if no more executions to do
                                                        IF @process_statements = 0
                                                                BEGIN
                                                                        BREAK;
                                                                END;
                                                END;

                                        -- Check if available statements at current phase
                                        IF EXISTS(SELECT * FROM tools.atac_queue AS taq WHERE taq.phase = @current_phase AND taq.status_code IN ('L', 'R', 'W'))
                                                BEGIN
                                                        -- Unlock next statement for specific entity
                                                        WITH cte_phase
                                                        AS (
                                                                SELECT TOP(1)   taq.status_code
                                                                FROM            tools.atac_queue AS taq
                                                                WHERE           taq.phase = @current_phase
                                                                                AND taq.entity = @entity
                                                                                AND taq.status_code IN ('L', 'R')
                                                                ORDER BY        taq.statement_id
                                                        )
                                                        UPDATE  cte
                                                        SET     cte.status_code = 'R'
                                                        FROM    cte_phase AS cte
                                                        WHERE   cte.status_code = 'L'
                                                        OPTION  (MAXDOP 1);

                                                        IF @@ROWCOUNT = 0
                                                                BEGIN
                                                                        WAITFOR DELAY   @delay;
                                                                END;
                                                END;
                                        ELSE
                                                BEGIN
                                                        -- Check for errors in current phase
                                                        IF EXISTS(SELECT * FROM tools.atac_queue AS taq WHERE taq.phase = @current_phase AND taq.status_code = 'E' AND taq.action_code <> 'remo')
                                                                BEGIN
                                                                        -- Not allowed to continue with next phase when errors exist in current
                                                                        SET     @process_statements = 0;

                                                                        BREAK;
                                                                END;

                                                        -- Move on to next phase
                                                        SET     @current_phase += 1;

                                                        -- Exit if no more phases
                                                        IF @current_phase > @max_phase
                                                                BEGIN
                                                                        SET     @process_statements = 0;

                                                                        BREAK;
                                                                END;

                                                        -- Unlock first statement for each entity
                                                        WITH cte_phase
                                                        AS (
                                                                SELECT  taq.status_code,
                                                                        ROW_NUMBER() OVER (PARTITION BY taq.entity ORDER BY taq.statement_id) AS rnk
                                                                FROM    tools.atac_queue AS taq
                                                                WHERE   taq.phase = @current_phase
                                                                        AND taq.status_code IN ('E', 'W', 'L', 'R')
                                                        )
                                                        UPDATE  cte
                                                        SET     cte.status_code = 'R'
                                                        FROM    cte_phase AS cte
                                                        WHERE   cte.rnk = 1
                                                                AND cte.status_code = 'L'
                                                        OPTION  (MAXDOP 1);
                                                END;
                                END;

                        -- Exit iteration if no more executions to do
                        IF @process_statements = 0
                                BEGIN
                                        BREAK;
                                END;
                END;
END TRY
BEGIN CATCH
        THROW;
END CATCH;
GO
