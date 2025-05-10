with recent_summary as (
	select 
        "SEASON_ID", 
        "TEAM_NM",
		max(case when cast("IS_HOME" as boolean) then "H_W_CN" else "A_W_CN" end) as "W_CN",
		max(case when cast("IS_HOME" as boolean) then "H_L_CN" else "A_L_CN" end) as "L_CN",
		max(case when cast("IS_HOME" as boolean) then "H_D_CN" else "A_D_CN" end) as "D_CN",
		sum("R") as "R",
		sum("H") as "H",
		sum("E") as "E",
		sum("B") as "B"
    from {{ ref('stg_game_recent_vs') }}
    where "SR_ID" = 0
    group by "SEASON_ID", "TEAM_NM"
	order by "SEASON_ID", "TEAM_NM"
)

select 
    *,
    round(("W_CN" + "D_CN" * 0.5) * 1.0 / ("W_CN" + "L_CN" + "D_CN"), 3) as "W_RATE" 
from recent_summary
