IF OBJECT_ID(N'dbo.atac_process', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE dbo.atac_process AS');
GO
ALTER PROCEDURE dbo.atac_process
(
        @number_of_executions INT = 0,  -- 0 = Loop until no more statements are found
        @waitfor TIME(0) = '00:00:05'
)
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Local helper variables
DECLARE @statement_id INT,
        @sql_text NVARCHAR(MAX),
        @entity NVARCHAR(392),
        @action_code NCHAR(4),
        @delay NCHAR(8) = @waitfor;

-- Validate user supplied input parameters
IF @number_of_executions <= 0 OR @number_of_executions IS NULL
        BEGIN
                SET     @number_of_executions = -1;
        END;

-- Local helper table
DECLARE @process TABLE
        (
                statement_id INT NOT NULL,
                action_code NCHAR(4) NOT NULL,
                entity NVARCHAR(392) NOT NULL,
                sql_text NVARCHAR(MAX) NOT NULL
        );

-- Keep processing as long as there are statements to be processed
WHILE EXISTS (SELECT * FROM dbo.atac_queue WHERE status_code IN (N'W', N'L', N'R')) AND (@number_of_executions = -1 OR @number_of_executions >= 1)
        BEGIN
                -- Prepare iteration
                DELETE
                FROM    @process;

                -- Find next ready row and lock it
                WITH cteProcess(statement_id, action_code, status_code, session_id, entity, statement_start, sql_text, log_text)
                AS (
                        SELECT TOP(1)   statement_id,
                                        action_code,
                                        status_code,
                                        session_id,
                                        entity,
                                        statement_start,
                                        sql_text,
                                        log_text
                        FROM            dbo.atac_queue
                        WHERE           status_code = N'R'
                        ORDER BY        statement_id
                )
                UPDATE  cteProcess
                SET     status_code = N'W',
                        session_id = @@SPID,
                        statement_start = SYSDATETIME(),
                        log_text = NULL
                OUTPUT  inserted.statement_id,
                        inserted.action_code,
                        inserted.entity,
                        inserted.sql_text
                INTO    @process
                        (
                                statement_id,
                                action_code,
                                entity,
                                sql_text
                        );

                -- Get statement to process (aggregation is used to make certain a value is returned even if no rows)
                SELECT  @statement_id = MAX(statement_id),
                        @sql_text = MAX(sql_text),
                        @entity = MAX(entity),
                        @action_code = MAX(action_code)
                FROM    @process;

                -- Statement not available 
                IF @statement_id IS NULL
                        BEGIN
                                IF EXISTS (SELECT * FROM dbo.atac_queue WHERE status_code IN (N'W', N'L', N'R') AND action_code <> N'endt')
                                        BEGIN
                                                -- No more statements ready, wait and try later
                                                WAITFOR DELAY @delay;
                                        END;
                                ELSE
                                        BEGIN
                                                -- Release enable database trigger statement
                                                UPDATE  dbo.atac_queue
                                                SET     status_code = N'R'
                                                WHERE   action_code = N'endt'
                                                        AND status_code = N'L';
                                        END;
                        END;
                ELSE
                        BEGIN
                                BEGIN TRY
                                        -- Excute currenct statement
                                        EXEC    (@sql_text);

                                        -- Update processed and end time
                                        UPDATE  dbo.atac_queue
                                        SET     status_code = N'F',
                                                statement_end = SYSDATETIME()
                                        WHERE   statement_id = @statement_id;

                                        -- Decrease exeuction counter
                                        IF @number_of_executions >= 1
                                                SET     @number_of_executions -= 1;
                                END TRY
                                BEGIN CATCH
                                        UPDATE  dbo.atac_queue
                                        SET     status_code = N'L',
                                                session_id = NULL,
                                                statement_start = NULL,
                                                log_text = CONCAT(N'(', ERROR_NUMBER(), N') ', ERROR_MESSAGE())
                                        WHERE   statement_id = @statement_id;
                                END CATCH;
                        END;

                -- Unlock statements
                IF @action_code = N'didt'
                        BEGIN
                                WITH cteProcess
                                AS (
                                        SELECT  status_code,
                                                ROW_NUMBER() OVER (PARTITION BY entity ORDER BY statement_id) AS rnk
                                        FROM    dbo.atac_queue
                                        WHERE   statement_id >= @statement_id
                                                AND action_code <> N'endt'
                                                AND status_code IN (N'W', N'L', N'R')
                                )
                                UPDATE  cteProcess
                                SET     status_code = N'R'
                                WHERE   rnk = 1
                                        AND status_code IN (N'L', N'R');
                        END;
                ELSE
                        BEGIN
                                WITH cteProcess
                                AS (
                                        SELECT TOP(1)   status_code
                                        FROM            dbo.atac_queue
                                        WHERE           statement_id >= @statement_id
                                                        AND entity = @entity
                                                        AND status_code IN (N'W', N'L', N'R')
                                        ORDER BY        statement_id
                                )
                                UPDATE  cteProcess
                                SET     status_code = N'R'
                                WHERE   status_code IN (N'L', N'R');
                        END;
        END;
GO
