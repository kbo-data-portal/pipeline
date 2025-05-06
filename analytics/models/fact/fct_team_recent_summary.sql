with recent_summary as (
	select 
        "SEASON_ID", 
        "TEAM_NM",
		sum(case when "W_L" = '승' then 1 else 0 end) as "W_CN",
		sum(case when "W_L" = '패' then 1 else 0 end) as "L_CN",
		sum(case when "W_L" = '무' then 1 else 0 end) as "D_CN",
		sum("R") as "R",
		sum("H") as "H",
		sum("E") as "E",
		sum("B") as "B"
    from {{ ref('stg_recent_summary') }}
    where "SR_ID" = 0
    group by "SEASON_ID", "TEAM_NM"
	order by "SEASON_ID", "TEAM_NM"
)

select 
    *,
    round(("W_CN" + "D_CN" * 0.5) * 1.0 / ("W_CN" + "L_CN" + "D_CN"), 3) as "W_RATE" 
from recent_summary
