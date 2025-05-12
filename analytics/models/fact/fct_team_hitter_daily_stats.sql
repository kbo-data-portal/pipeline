with hitter_stats as (
    select 
        "SEASON_ID", 
        "G_DT",
        "TEAM_NM", 
        sum("G") as "G",
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
    from {{ source('player', 'hitter_season_summary') }}
    group by "SEASON_ID", "G_DT", "TEAM_NM"
	order by "SEASON_ID", "G_DT", "TEAM_NM"
)

select 
    *,
    round("H" / "AB"::numeric, 3) as "AVG"
 from hitter_stats