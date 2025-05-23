with 
current_recent as (
    select distinct on("TEAM_NM") *
    from {{ ref('fct_team_recent_summary') }}
    where "G_DT" = (select max("G_DT") as "G_DT" from {{ ref('fct_team_recent_summary') }})
),
current_hitter as (
    select *
    from {{ ref('fct_team_hitter_daily_stats') }}
    where "G_DT" = (select max("G_DT") as "G_DT" from {{ ref('fct_team_hitter_daily_stats') }})
),
current_pitcher as (
    select *
    from {{ ref('fct_team_pitcher_daily_stats') }}
    where "G_DT" = (select max("G_DT") as "G_DT" from {{ ref('fct_team_pitcher_daily_stats') }})
),
game_predict as (
    select 
        "schedule"."SEASON_ID", 
        to_date("schedule"."G_DT"::text, 'YYYYMMDD') as "G_DT",
        "schedule"."G_ID",
        "schedule"."G_DT_TXT",
        "schedule"."G_TM",
        "schedule"."S_NM",
        "schedule"."GAME_RESULT_CK",
        "schedule"."HOME_ID",
        "schedule"."HOME_NM", 
        "schedule"."AWAY_NM", 
        "schedule"."B_SCORE_CN" as "HOME_SCORE",
        "schedule"."T_SCORE_CN" as "AWAY_SCORE",
        "schedule"."B_RANK_NO" as "HOME_RANK",
        "schedule"."T_RANK_NO" as "AWAY_RANK",
        "home_recent"."W_RATE" as "HOME_REC_RATE",
        "home_recent"."AVG" as "HOME_REC_RAVG",
        "home_recent"."OPP_AVG" as "HOME_REC_ORAVG",
        "away_recent"."W_RATE" as "AWAY_REC_RATE",
        "away_recent"."AVG" as "AWAY_REC_RAVG",
        "away_recent"."OPP_AVG" as "AWAY_REC_ORAVG",
        "home_h"."REC_PA" as "HOME_REC_PA",
        "home_h"."REC_AB" as "HOME_REC_AB",
        "home_h"."REC_R" as "HOME_REC_R",
        "home_h"."REC_H" as "HOME_REC_H",
        "home_h"."REC_2B" as "HOME_REC_2B",
        "home_h"."REC_3B" as "HOME_REC_3B",
        "home_h"."REC_HR" as "HOME_REC_HR",
        "home_h"."REC_RBI" as "HOME_REC_RBI",
        "home_h"."REC_SB" as "HOME_REC_SB",
        "home_h"."REC_CS" as "HOME_REC_CS",
        "home_h"."REC_BB" as "HOME_REC_BB",
        "home_h"."REC_HBP" as "HOME_REC_HBP",
        "home_h"."REC_SO" as "HOME_REC_SO",
        "home_h"."REC_GDP" as "HOME_REC_GDP",
        "home_h"."REC_AVG" as "HOME_REC_AVG",
        "home_h"."PA" as "HOME_PA",
        "home_h"."AB" as "HOME_AB",
        "home_h"."R" as "HOME_R",
        "home_h"."H" as "HOME_H",
        "home_h"."2B" as "HOME_2B",
        "home_h"."3B" as "HOME_3B",
        "home_h"."HR" as "HOME_HR",
        "home_h"."RBI" as "HOME_RBI",
        "home_h"."SB" as "HOME_SB",
        "home_h"."CS" as "HOME_CS",
        "home_h"."BB" as "HOME_BB",
        "home_h"."HBP" as "HOME_HBP",
        "home_h"."SO" as "HOME_SO",
        "home_h"."GDP" as "HOME_GDP",
        "home_h"."AVG" as "HOME_AVG",
        "home_p"."REC_TBF" as "HOME_REC_TBF",
        "home_p"."REC_IP" as "HOME_REC_IP",
        "home_p"."REC_H" as "HOME_REC_PH",
        "home_p"."REC_HR" as "HOME_REC_PHR",
        "home_p"."REC_BB" as "HOME_REC_PBB",
        "home_p"."REC_HBP" as "HOME_REC_PHBP",
        "home_p"."REC_SO" as "HOME_REC_PSO",
        "home_p"."REC_R" as "HOME_REC_PR",
        "home_p"."REC_ER" as "HOME_REC_ER",
        "home_p"."REC_ERA" as "HOME_REC_ERA",
        "home_p"."REC_WHIP" as "HOME_REC_WHIP",
        "home_p"."TBF" as "HOME_TBF",
        "home_p"."IP" as "HOME_IP",
        "home_p"."H" as "HOME_PH",
        "home_p"."HR" as "HOME_PHR",
        "home_p"."BB" as "HOME_PBB",
        "home_p"."HBP" as "HOME_PHBP",
        "home_p"."SO" as "HOME_PSO",
        "home_p"."R" as "HOME_PR",
        "home_p"."ER" as "HOME_ER",
        "home_p"."ERA" as "HOME_ERA",
        "home_p"."WHIP" as "HOME_WHIP",
        "away_h"."REC_PA" as "AWAY_REC_PA",
        "away_h"."REC_AB" as "AWAY_REC_AB",
        "away_h"."REC_R" as "AWAY_REC_R",
        "away_h"."REC_H" as "AWAY_REC_H",
        "away_h"."REC_2B" as "AWAY_REC_2B",
        "away_h"."REC_3B" as "AWAY_REC_3B",
        "away_h"."REC_HR" as "AWAY_REC_HR",
        "away_h"."REC_RBI" as "AWAY_REC_RBI",
        "away_h"."REC_SB" as "AWAY_REC_SB",
        "away_h"."REC_CS" as "AWAY_REC_CS",
        "away_h"."REC_BB" as "AWAY_REC_BB",
        "away_h"."REC_HBP" as "AWAY_REC_HBP",
        "away_h"."REC_SO" as "AWAY_REC_SO",
        "away_h"."REC_GDP" as "AWAY_REC_GDP",
        "away_h"."REC_AVG" as "AWAY_REC_AVG",
        "away_h"."PA" as "AWAY_PA",
        "away_h"."AB" as "AWAY_AB",
        "away_h"."R" as "AWAY_R",
        "away_h"."H" as "AWAY_H",
        "away_h"."2B" as "AWAY_2B",
        "away_h"."3B" as "AWAY_3B",
        "away_h"."HR" as "AWAY_HR",
        "away_h"."RBI" as "AWAY_RBI",
        "away_h"."SB" as "AWAY_SB",
        "away_h"."CS" as "AWAY_CS",
        "away_h"."BB" as "AWAY_BB",
        "away_h"."HBP" as "AWAY_HBP",
        "away_h"."SO" as "AWAY_SO",
        "away_h"."GDP" as "AWAY_GDP",
        "away_h"."AVG" as "AWAY_AVG",
        "away_p"."REC_TBF" as "AWAY_REC_TBF",
        "away_p"."REC_IP" as "AWAY_REC_IP",
        "away_p"."REC_H" as "AWAY_REC_PH",
        "away_p"."REC_HR" as "AWAY_REC_PHR",
        "away_p"."REC_BB" as "AWAY_REC_PBB",
        "away_p"."REC_HBP" as "AWAY_REC_PHBP",
        "away_p"."REC_SO" as "AWAY_REC_PSO",
        "away_p"."REC_R" as "AWAY_REC_PR",
        "away_p"."REC_ER" as "AWAY_REC_ER",
        "away_p"."REC_ERA" as "AWAY_REC_ERA",
        "away_p"."REC_WHIP" as "AWAY_REC_WHIP",
        "away_p"."TBF" as "AWAY_TBF",
        "away_p"."IP" as "AWAY_IP",
        "away_p"."H" as "AWAY_PH",
        "away_p"."HR" as "AWAY_PHR",
        "away_p"."BB" as "AWAY_PBB",
        "away_p"."HBP" as "AWAY_PHBP",
        "away_p"."SO" as "AWAY_PSO",
        "away_p"."R" as "AWAY_PR",
        "away_p"."ER" as "AWAY_ER",
        "away_p"."ERA" as "AWAY_ERA",
        "away_p"."WHIP" as "AWAY_WHIP"
    from {{ source('game', 'schedule') }} as "schedule"
    left join current_recent as "home_recent" 
        on "schedule"."HOME_NM" = "home_recent"."TEAM_NM" 
    left join current_recent as "away_recent"
        on "schedule"."AWAY_NM" = "away_recent"."TEAM_NM" 
    left join current_hitter as "home_h" 
        on "schedule"."HOME_NM" = "home_h"."TEAM_NM" 
    left join current_hitter as "away_h"
        on "schedule"."AWAY_NM" = "away_h"."TEAM_NM" 
    left join current_pitcher as "home_p" 
        on "schedule"."HOME_NM" = "home_p"."TEAM_NM" 
    left join current_pitcher as "away_p"
        on "schedule"."AWAY_NM" = "away_p"."TEAM_NM" 
)

select 
    *, 
    case when "HOME_SCORE" > "AWAY_SCORE" then 1 
         when "HOME_SCORE" < "AWAY_SCORE" then 0 
         else null end as "RESULT"
 from game_predict
