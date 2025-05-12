with cum_stats as (
    select 
        "SEASON_ID", 
        "TEAM_NM",
        "P_ID",
        "P_NM",
        "G_DT",
        sum("PA") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "PA",
        sum("AB") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "AB",
        sum("R") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "R",
        sum("H") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "H",
        sum("2B") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "2B",
        sum("3B") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "3B",
        sum("HR") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "HR",
        sum("RBI") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "RBI",
        sum("SB") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "SB",
        sum("CS") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "CS",
        sum("BB") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "BB",
        sum("HBP") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "HBP",
        sum("SO") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "SO",
        sum("GDP") over (partition by "SEASON_ID", "P_ID" order by "SEASON_ID", "G_DT" rows between unbounded preceding and current row) as "GDP"
    from {{ ref('stg_hitter_daily_summary') }}
)

select * from cum_stats
