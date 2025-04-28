with season_summary as (
	select 
        "SEASON_ID", 
        "TEAM_NM",
		max(case when "H_A" = 'H' then "H_W_CN" else "A_W_CN" end) as "W_CN",
		max(case when "H_A" = 'H' then "H_L_CN" else "A_L_CN" end) as "L_CN",
		max(case when "H_A" = 'H' then "H_D_CN" else "A_D_CN" end) as "D_CN"
    from {{ ref('stg_game_summary') }}
    where "SR_ID" = 0
    group by "SEASON_ID", "TEAM_NM"
	order by "SEASON_ID", "TEAM_NM"
)

select 
    *,
    round(("W_CN" + "D_CN" * 0.5) * 1.0 / ("W_CN" + "L_CN" + "D_CN"), 3) as "W_RATE" 
from season_summary
