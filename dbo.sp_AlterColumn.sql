IF OBJECT_ID(N'dbo.sp_AlterColumn', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE dbo.sp_AlterColumn AS');
GO
ALTER PROCEDURE dbo.sp_AlterColumn
(
        @verbose BIT = 1,
        @number_of_executions INT = 0,
        @waitfor TIME(3) = '00:00:00.250'
)
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

BEGIN TRY
        -- Populate statement queue
        EXEC    dbo.atac_populate;

        -- If viewing statements only
        IF @verbose = 1
                BEGIN
                        SELECT          action_code,
                                        status_code,
                                        sql_text
                        FROM            dbo.atac_queue
                        ORDER BY        statement_id;

                        RETURN;
                END;

        -- Process statements
        EXEC    dbo.atac_process        @number_of_executions = @number_of_executions,
                                        @waitfor = @waitfor;
END TRY
BEGIN CATCH
        THROW;

        RETURN  -1000;
END CATCH;
GO
