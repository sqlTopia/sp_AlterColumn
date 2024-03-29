IF OBJECT_ID(N'dbo.vw_atac_progress', 'V') IS NULL
        EXEC(N'CREATE VIEW dbo.vw_atac_progress AS SELECT 1 AS Yak');
GO
ALTER VIEW dbo.vw_atac_progress
AS

SELECT TOP(25)  aqe.statement_id,
                aqe.action_code,
                aqe.session_id,
                aqe.status_code,
                aqe.statement_start,
                aqe.statement_end,
                DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, aqe.statement_start, COALESCE(aqe.statement_end, SYSDATETIME())), CAST('00:00:00.000' AS TIME(3))) AS statement_time,
                aqe.log_text,
                aqe.sort_order,
                aqe.entity,
                aqe.phase,
                aqe.sql_text,
                CAST(100E * (aqe.statement_id - 1) / wrk.total_items AS DECIMAL(5, 2)) AS statement_progress,
                CAST(100E * wrk.finished_items / wrk.total_items AS DECIMAL(5, 2)) AS total_progress
FROM            dbo.atac_queue AS aqe WITH (NOLOCK)
CROSS JOIN      (
                        SELECT  SUM(CASE WHEN aqe.status_code = 'F' THEN 1 ELSE 0 END) AS finished_items,
                                MAX(aqe.statement_id) AS total_items
                        FROM    dbo.atac_queue AS aqe WITH (NOLOCK)
                ) AS wrk
WHERE           aqe.status_code <> 'F'
ORDER BY        aqe.statement_id;
GO
