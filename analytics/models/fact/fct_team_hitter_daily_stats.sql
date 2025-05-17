with hitter_stats as (
    select 
        "SEASON_ID", 
        to_date("G_DT"::text, 'YYYYMMDD') as "G_DT",
        "TEAM_NM", 
        sum("PA") as "PA",
        sum("AB") as "AB",
        sum("R") as "R",
        sum("H") as "H",
        sum("2B") as "2B",
        sum("3B") as "3B",
        sum("HR") as "HR",
        sum("RBI") as "RBI",
        sum("SB") as "SB",
        sum("CS") as "CS",
        sum("BB") as "BB",
        sum("HBP") as "HBP",
        sum("SO") as "SO",
        sum("GDP") as "GDP"
    from {{ ref('stg_hitter_daily_summary') }}
    group by "SEASON_ID", "G_DT", "TEAM_NM"
	order by "SEASON_ID", "G_DT", "TEAM_NM"
),
cum_stats as (
    select 
        "SEASON_ID", 
        "TEAM_NM",
        "G_DT",
        sum("PA") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "PA",
        sum("AB") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "AB",
        sum("R") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "R",
        sum("H") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "H",
        sum("2B") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "2B",
        sum("3B") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "3B",
        sum("HR") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "HR",
        sum("RBI") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "RBI",
        sum("SB") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "SB",
        sum("CS") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "CS",
        sum("BB") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "BB",
        sum("HBP") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "HBP",
        sum("SO") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "SO",
        sum("GDP") over (partition by "SEASON_ID", "TEAM_NM" order by "G_DT" rows between unbounded preceding and current row) as "GDP"
    from hitter_stats
)

select 
    *,
    round("H" / "AB"::numeric, 3) as "AVG"
 from cum_stats