with game_recent_vs as (
    select 
        row_number() over (partition by "SEASON_ID", "TEAM_NM" order by "G_ID" desc) as "RN",
        *
    from {{ ref('stg_game_result_vs') }}
)

select * from game_recent_vs
where "RN" <= 5
