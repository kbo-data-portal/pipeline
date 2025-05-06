with game_dataset as (
    select 
        gs."SEASON_ID", 
        gs."G_DT",
        gs."GAME_RESULT_CK",
        gs."HOME_NM", 
        gs."AWAY_NM", 
        gs."B_SCORE_CN" as "HOME_SCORE",
        gs."T_SCORE_CN" as "AWAY_SCORE",
        gs."B_RANK_NO" as "HOME_RANK",
        gs."T_RANK_NO" as "AWAY_RANK",
        ftss."R_AVG" as "HOME_R_AVG", 
        ftss."RA_AVG" as "HOME_RA_AVG", 
        aftss."R_AVG" as "AWAY_R_AVG", 
        aftss."RA_AVG" as "AWAY_RA_AVG",
        ftps."ERA" as "HOME_ERA", 
        ftps."WPCT" as "HOME_WPCT", 
        aftps."ERA" as "AWAY_ERA", 
        aftps."WPCT" as "AWAY_WPCT",
        fths."AVG" as "HOME_AVG", 
        afths."AVG" as "AWAY_AVG",
        ftrs."W_RATE" as "HOME_W_RATE_RECENT",
        aftrs."W_RATE" as "AWAY_W_RATE_RECENT",
        ftvs."W_RATE" as "HOME_W_RATE",
        aftvs."W_RATE" as "AWAY_W_RATE"
    from public.game_schedule gs 
    left join analytics.fct_team_season_summary ftss 
        on gs."SEASON_ID" = ftss."SEASON_ID" and gs."HOME_NM" = ftss."TEAM_NM" 
    left join analytics.fct_team_season_summary aftss 
        on gs."SEASON_ID" = aftss."SEASON_ID" and gs."AWAY_NM" = aftss."TEAM_NM" 
    left join analytics.fct_team_pitcher_stats ftps 
        on gs."SEASON_ID" = ftps."SEASON_ID" and gs."HOME_NM" = ftps."TEAM_NM" 
    left join analytics.fct_team_pitcher_stats aftps 
        on gs."SEASON_ID" = aftps."SEASON_ID" and gs."AWAY_NM" = aftps."TEAM_NM" 
    left join analytics.fct_team_hitter_stats fths 
        on gs."SEASON_ID" = fths."SEASON_ID" and gs."HOME_NM" = fths."TEAM_NM" 
    left join analytics.fct_team_hitter_stats afths 
        on gs."SEASON_ID" = afths."SEASON_ID" and gs."AWAY_NM" = afths."TEAM_NM" 
    left join analytics.fct_team_recent_summary ftrs 
        on gs."SEASON_ID" = ftrs."SEASON_ID" and gs."HOME_NM" = ftrs."TEAM_NM" 
    left join analytics.fct_team_recent_summary aftrs 
        on gs."SEASON_ID" = aftrs."SEASON_ID" and gs."AWAY_NM" = aftrs."TEAM_NM" 
    left join analytics.fct_team_vs_summary ftvs
        on gs."SEASON_ID" = ftvs."SEASON_ID" 
        and gs."HOME_NM" = ftvs."TEAM_NM" and gs."AWAY_NM" = ftvs."OP_NM" 
    left join analytics.fct_team_vs_summary aftvs
        on gs."SEASON_ID" = aftvs."SEASON_ID" 
        and gs."HOME_NM" = aftvs."OP_NM" and gs."AWAY_NM" = aftvs."TEAM_NM" 
)

select * from game_dataset
