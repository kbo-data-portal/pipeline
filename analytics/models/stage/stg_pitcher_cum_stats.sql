with cum_stats as (
    select 
        "SEASON_ID", 
        "TEAM_NM",
        "P_ID",
        "P_NM",
        "G_DT",
        sum("TBF") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) AS "TBF",
        sum("IP") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) AS "IP",
        sum("H") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) AS "H",
        sum("HR") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) AS "HR",
        sum("BB") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) AS "BB",
        sum("HBP") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) AS "HBP",
        sum("SO") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) AS "SO",
        sum("R") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) AS "R",
        sum("ER") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) AS "ER"
    from {{ ref('stg_pitcher_daily_summary') }}
)

select * from cum_stats