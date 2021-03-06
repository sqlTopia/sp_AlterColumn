IF OBJECT_ID(N'dbo.atac_process', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE dbo.atac_process AS');
GO
ALTER PROCEDURE dbo.atac_process
(
        @maximum_number_of_statements INT = NULL,
        @waitfor TIME(3) = '00:00:00.250'
)
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Local helper variables
DECLARE @statement_id INT,
        @sql_text NVARCHAR(MAX),
        @entity NVARCHAR(392) = N'',
        @current_phase TINYINT = 1,
        @max_phase TINYINT,
        @action_code NCHAR(4);

-- Validate user supplied input parameters
IF @maximum_number_of_statements <= 0
        BEGIN
                RETURN;
        END;

-- Get current max_phase
SELECT TOP(1)   @max_phase = phase
FROM            dbo.atac_queue
ORDER BY        phase DESC;

-- Local helper table
DECLARE @process TABLE
        (
                statement_id INT NOT NULL,
                entity NVARCHAR(392) NOT NULL,
                action_code NCHAR(4) NOT NULL,
                sql_text NVARCHAR(MAX) NOT NULL
        );

-- Keep iterating as long as there are statements to be executed
WHILE EXISTS (SELECT * FROM dbo.atac_queue WHERE status_code IN (N'W', N'L', N'R'))
        BEGIN
                -- Get next statement ordered by phase and statement_id
                WHILE @current_phase <= @max_phase
                        BEGIN
                                DELETE
                                FROM    @process;

                                WITH cteQueue(statement_id, status_code, session_id, entity, action_code, statement_start, sql_text, log_text)
                                AS (
                                        SELECT TOP(1)   statement_id,
                                                        status_code,
                                                        session_id,
                                                        entity,
                                                        action_code,
                                                        statement_start,
                                                        sql_text,
                                                        log_text
                                        FROM            dbo.atac_queue WITH (READPAST)
                                        WHERE           status_code = N'R'
                                                        AND phase = @current_phase
                                        ORDER BY        statement_id
                                )
                                UPDATE  cteQueue
                                SET     status_code = N'W',
                                        session_id = @@SPID,
                                        statement_start = SYSDATETIME(),
                                        log_text = NULL
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
                                        );

                                -- Process statement if found
                                IF ROWCOUNT_BIG() >= 1
                                        BEGIN
                                                SELECT  @statement_id = statement_id,
                                                        @sql_text = sql_text,
                                                        @entity = entity,
                                                        @action_code = action_code
                                                FROM    @process;

                                                -- Execute statement
                                                BEGIN TRY
                                                        -- Excute current statement
                                                        EXEC    dbo.sqltopia_retry      @sql_text = @sql_text,
                                                                                        @waitfor = @waitfor;
                                                                                        
                                                        -- Update processed and end time
                                                        UPDATE  dbo.atac_queue WITH (TABLOCK)
                                                        SET     status_code = N'F',
                                                                statement_end = SYSDATETIME()
                                                        WHERE   statement_id = @statement_id;

                                                        -- Decrease execution counter
                                                        IF @maximum_number_of_statements >= 1
                                                                BEGIN
                                                                        SET     @maximum_number_of_statements -= 1;
                                                                END;

                                                        -- No more executions left to do
                                                        IF @maximum_number_of_statements = 0
                                                                BEGIN
                                                                        RETURN;
                                                                END;
                                                END TRY
                                                BEGIN CATCH
                                                        UPDATE  dbo.atac_queue WITH (TABLOCK)
                                                        SET     statement_start = NULL,
                                                                status_code = N'E',
                                                                log_text = CONCAT(N'(', ERROR_NUMBER(), N') ', ERROR_MESSAGE())
                                                        WHERE   statement_id = @statement_id;

                                                        IF @action_code NOT IN (N'remo')
                                                                BEGIN
                                                                        UPDATE  dbo.atac_queue WITH (TABLOCK)
                                                                        SET     status_code = N'E',
                                                                                log_text = CONCAT(N'An earlier execution for same entity went wrong (statement #', @statement_id, N').')
                                                                        WHERE   statement_id > @statement_id
                                                                                AND entity = @entity;
                                                                END;
                                                END CATCH;
                                        END;

                                -- Check if available statements at current phase
                                IF EXISTS (SELECT * FROM dbo.atac_queue WHERE phase = @current_phase AND status_code IN (N'W', N'L', N'R'))
                                        BEGIN
                                                -- Unlock next statement for specific entity
                                                WITH ctePhase
                                                AS (
                                                        SELECT TOP(1)   status_code
                                                        FROM            dbo.atac_queue WITH (TABLOCK)
                                                        WHERE           phase = @current_phase
                                                                        AND entity = @entity
                                                                        AND status_code IN (N'W', N'L', N'R')
                                                        ORDER BY        statement_id
                                                )
                                                UPDATE  ctePhase
                                                SET     status_code = N'R'
                                                WHERE   status_code = N'L';
                                        END;
                                ELSE
                                        BEGIN
                                                SET     @current_phase += 1;

                                                -- Exit if no more phases
                                                IF @current_phase > @max_phase
                                                        BEGIN
                                                                RETURN;
                                                        END;

                                                -- Unlock first statement for each entity
                                                WITH ctePhase
                                                AS (
                                                        SELECT  status_code,
                                                                ROW_NUMBER() OVER (PARTITION BY entity ORDER BY statement_id) AS rnk
                                                        FROM    dbo.atac_queue WITH (TABLOCK)
                                                        WHERE   phase = @current_phase
                                                                AND status_code IN (N'E', N'W', N'L', N'R')
                                                )
                                                UPDATE  ctePhase
                                                SET     status_code = N'R'
                                                WHERE   rnk = 1
                                                        AND status_code = N'L';
                                        END;
                        END;
        END;
GO
