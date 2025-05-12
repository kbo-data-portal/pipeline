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
    from {{ ref('stg_pitcher_cum_stats') }}
    group by "SEASON_ID", "G_DT", "TEAM_NM"
	order by "SEASON_ID", "G_DT", "TEAM_NM"
)

select 
    *,
    round((("ER" * 9) / "IP")::numeric, 3) as "ERA"
from pitcher_stats