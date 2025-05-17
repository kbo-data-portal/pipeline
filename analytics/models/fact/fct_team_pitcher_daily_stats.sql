with pitcher_stats as (
    select 
        "SEASON_ID", 
        to_date("G_DT"::text, 'YYYYMMDD') as "G_DT",
        "TEAM_NM",
        sum("TBF") as "TBF",
        sum("IP") as "IP",
        sum("H") as "H",
        sum("HR") as "HR",
        sum("BB") as "BB",
        sum("HBP") as "HBP",
        sum("SO") as "SO",
        sum("R") as "R",
        sum("ER") as "ER"
    from {{ ref('stg_pitcher_daily_summary') }}
    group by "SEASON_ID", "G_DT", "TEAM_NM"
	order by "SEASON_ID", "G_DT", "TEAM_NM"
),
cum_stats as (
    select 
        "SEASON_ID", 
        "TEAM_NM",
        "G_DT",
        sum("TBF") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "TBF",
        sum("IP") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "IP",
        sum("H") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "H",
        sum("HR") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "HR",
        sum("BB") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "BB",
        sum("HBP") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "HBP",
        sum("SO") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "SO",
        sum("R") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "R",
        sum("ER") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "ER"
    from pitcher_stats
)

select 
    *,
    round((("ER" * 9) / "IP")::numeric, 3) as "ERA",
    round((("BB" + "H") / "IP")::numeric, 3) as "WHIP"
from cum_stats