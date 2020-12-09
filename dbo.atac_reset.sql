IF OBJECT_ID(N'dbo.atac_reset', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE dbo.atac_reset AS');
GO
ALTER PROCEDURE dbo.atac_reset
(
        @tag NVARCHAR(36) = NULL
)
/*
        atac_reset v21.01.01
        (C) 2009-2021, Peter Larsson
*/
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Delete one tag from atac_configuration and atac_queue
IF @tag IS NOT NULL
        BEGIN
                DELETE
                FROM    dbo.atac_queue
                WHERE   tag = @tag
                        AND @tag IS NOT NULL;

                DELETE
                FROM    dbo.atac_configuration
                WHERE   tag = @tag 
                        AND @tag IS NOT NULL;
        END;

-- Delete tags in atac_queue
IF @tag IS NULL OR NOT EXISTS (SELECT * FROM dbo.atac_queue)
        BEGIN
                TRUNCATE TABLE  dbo.atac_queue;
        END;

-- Delete all tags in atac_configuration
IF @tag IS NULL
        BEGIN
                TRUNCATE TABLE  dbo.atac_configuration;
        END;
GO
