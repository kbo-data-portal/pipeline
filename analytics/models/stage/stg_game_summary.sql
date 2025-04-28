with game_summary as (
    select 
        *,
        case when "H_A" = 'H' then "HOME_NM" else "AWAY_NM" end as "TEAM_NM",
	    case when "H_A" = 'H' then "AWAY_NM" else "HOME_NM" end as "OP_NM"
    from {{ source('public', 'game_summary') }}
)

select * from game_summary
