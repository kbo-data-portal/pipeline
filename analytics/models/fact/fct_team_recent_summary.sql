with recent_summary as (
	select 
        "SEASON_ID", 
        to_date("G_DT"::text, 'YYYY-MM-DD') as "G_DT",
        "TEAM_NM",
		"OPP_NM",
		sum(case when "R" > "OPP_R" then 1 else 0 end) over (partition by "TEAM_NM" order by "G_DT" rows between 4 preceding and current row) as "W_CN",
		sum(case when "R" < "OPP_R" then 1 else 0 end) over (partition by "TEAM_NM" order by "G_DT" rows between 4 preceding and current row) as "L_CN",
		sum(case when "R" = "OPP_R" then 1 else 0 end) over (partition by "TEAM_NM" order by "G_DT" rows between 4 preceding and current row) as "D_CN",
		avg("R") over (partition by "TEAM_NM" order by "G_DT" rows between 4 preceding and current row) as "AVG",
		avg("OPP_R") over (partition by "TEAM_NM" order by "G_DT" rows between 4 preceding and current row) as "OPP_AVG"
    from {{ ref('stg_game_result_vs') }}
)

select 
    *,
    round(("W_CN" + "D_CN" * 0.5) * 1.0 / ("W_CN" + "L_CN" + "D_CN"), 3) as "W_RATE" 
from recent_summary
