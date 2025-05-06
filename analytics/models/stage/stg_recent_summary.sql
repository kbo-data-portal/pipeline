with recent_summary as (
    select 
        row_number() over (partition by "SEASON_ID", "TEAM_NM" order by "G_ID" desc) as "RN",
        *
    from {{ ref('stg_game_summary') }}
)

select * from recent_summary
where "RN" <= 5
