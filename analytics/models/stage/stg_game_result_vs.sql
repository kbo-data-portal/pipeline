with game_result_vs as (
    select 
        "team".*,
        "opp"."R" as "OPP_R", 
        "opp"."H" as "OPP_H",
        "opp"."E" as "OPP_E",
        "opp"."B" as "OPP_B",
        case when cast("team"."IS_HOME" as boolean) then "team"."HOME_NM" else "team"."AWAY_NM" end as "TEAM_NM",
	    case when cast("team"."IS_HOME" as boolean) then "team"."AWAY_NM" else "team"."HOME_NM" end as "OPP_NM"
    from {{ source('game', 'result') }} as "team"
    join {{ source('game', 'result') }} as "opp"
    on "team"."G_ID" = "opp"."G_ID"
    and "team"."IS_HOME" != "opp"."IS_HOME"
)

select * from game_result_vs
