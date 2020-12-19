IF OBJECT_ID(N'dbo.atac_reset', 'P') IS NULL
        EXEC(N'CREATE PROCEDURE dbo.atac_reset AS');
GO
ALTER PROCEDURE dbo.atac_reset
AS

-- Prevent unwanted resultsets back to client
SET NOCOUNT ON;

-- Clean up
TRUNCATE TABLE  dbo.atac_queue;
TRUNCATE TABLE  dbo.atac_configuration;
