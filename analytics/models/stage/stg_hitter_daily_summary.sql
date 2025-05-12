with daily_summary as (
    select 
        "summary"."TEAM_NM",
        "summary"."P_NM",
        "daily".*
    from {{ source('player', 'hitter_daily_stats') }} as "daily"
    left join {{ source('player', 'hitter_season_summary') }} as "summary"
    on "daily"."P_ID" = "summary"."P_ID"
    and "daily"."SEASON_ID" = "summary"."SEASON_ID"
)

select * from daily_summary
