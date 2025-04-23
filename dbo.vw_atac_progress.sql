IF SCHEMA_ID(N'tools') IS NULL
        EXEC(N'CREATE SCHEMA tools;');
GO
IF OBJECT_ID(N'tools.vw_atac_progress', 'V') IS NULL
        EXEC(N'CREATE VIEW tools.vw_atac_progress AS SELECT 1 AS Yak');
GO
ALTER VIEW tools.vw_atac_progress
AS
SELECT TOP(25)  taq.statement_id,
                taq.action_code,
                taq.session_id,
                taq.status_code,
                taq.statement_start,
                taq.statement_end,
                DATEADD(MILLISECOND, DATEDIFF(MILLISECOND, taq.statement_start, COALESCE(taq.statement_end, SYSDATETIME())), CAST('00:00:00.000' AS TIME(3))) AS statement_time,
                taq.log_text,
                taq.sort_order,
                taq.entity,
                taq.phase,
                taq.sql_text,
                CAST(100E * (taq.statement_id - 1) / wrk.total_items AS DECIMAL(5, 2)) AS statement_progress,
                CAST(100E * wrk.finished_items / wrk.total_items AS DECIMAL(5, 2)) AS total_progress
FROM            tools.atac_queue AS taq WITH (NOLOCK)
CROSS JOIN      (
                        SELECT  SUM(CASE WHEN taq.status_code = 'F' THEN 1 ELSE 0 END) AS finished_items,
                                MAX(taq.statement_id) AS total_items
                        FROM    tools.atac_queue AS taq WITH (NOLOCK)
                ) AS wrk
WHERE           taq.status_code <> 'F'
ORDER BY        taq.statement_id;
GO
