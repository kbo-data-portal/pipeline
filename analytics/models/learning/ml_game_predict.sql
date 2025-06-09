with game_predict as (
    select 
        "cur"."SEASON_ID", 
        "cur"."G_DT",
        "cur"."G_ID",
        "cur"."G_DT_TXT",
        "cur"."G_TM",
        "cur"."S_NM",
        "cur"."GAME_RESULT_CK",
        "cur"."HOME_ID",
        "cur"."HOME_NM", 
        "cur"."AWAY_NM", 
        "cur"."HOME_SCORE",
        "cur"."AWAY_SCORE",
        "cur"."HOME_RANK",
        "cur"."AWAY_RANK",
        "cur"."RESULT",
        COALESCE("home_recent"."W_RATE", "cur"."HOME_REC_RATE") as "HOME_REC_RATE",
        COALESCE("home_recent"."AVG", "cur"."HOME_REC_RAVG") as "HOME_REC_RAVG",
        COALESCE("home_recent"."OPP_AVG", "cur"."HOME_REC_ORAVG") as "HOME_REC_ORAVG",
        COALESCE("away_recent"."W_RATE", "cur"."AWAY_REC_RATE") as "AWAY_REC_RATE",
        COALESCE("away_recent"."AVG", "cur"."AWAY_REC_RAVG") as "AWAY_REC_RAVG",
        COALESCE("away_recent"."OPP_AVG", "cur"."AWAY_REC_ORAVG") as "AWAY_REC_ORAVG",
        COALESCE("home_h"."REC_PA", "cur"."HOME_REC_PA") as "HOME_REC_PA",
        COALESCE("home_h"."REC_AB", "cur"."HOME_REC_AB") as "HOME_REC_AB",
        COALESCE("home_h"."REC_R", "cur"."HOME_REC_R") as "HOME_REC_R",
        COALESCE("home_h"."REC_H", "cur"."HOME_REC_H") as "HOME_REC_H",
        COALESCE("home_h"."REC_2B", "cur"."HOME_REC_2B") as "HOME_REC_2B",
        COALESCE("home_h"."REC_3B", "cur"."HOME_REC_3B") as "HOME_REC_3B",
        COALESCE("home_h"."REC_HR", "cur"."HOME_REC_HR") as "HOME_REC_HR",
        COALESCE("home_h"."REC_RBI", "cur"."HOME_REC_RBI") as "HOME_REC_RBI",
        COALESCE("home_h"."REC_SB", "cur"."HOME_REC_SB") as "HOME_REC_SB",
        COALESCE("home_h"."REC_CS", "cur"."HOME_REC_CS") as "HOME_REC_CS",
        COALESCE("home_h"."REC_BB", "cur"."HOME_REC_BB") as "HOME_REC_BB",
        COALESCE("home_h"."REC_HBP", "cur"."HOME_REC_HBP") as "HOME_REC_HBP",
        COALESCE("home_h"."REC_SO", "cur"."HOME_REC_SO") as "HOME_REC_SO",
        COALESCE("home_h"."REC_GDP", "cur"."HOME_REC_GDP") as "HOME_REC_GDP",
        COALESCE("home_h"."REC_AVG", "cur"."HOME_REC_AVG") as "HOME_REC_AVG",
        COALESCE("home_h"."PA", "cur"."HOME_PA") as "HOME_PA",
        COALESCE("home_h"."AB", "cur"."HOME_AB") as "HOME_AB",
        COALESCE("home_h"."R", "cur"."HOME_R") as "HOME_R",
        COALESCE("home_h"."H", "cur"."HOME_H") as "HOME_H",
        COALESCE("home_h"."2B", "cur"."HOME_2B") as "HOME_2B",
        COALESCE("home_h"."3B", "cur"."HOME_3B") as "HOME_3B",
        COALESCE("home_h"."HR", "cur"."HOME_HR") as "HOME_HR",
        COALESCE("home_h"."RBI", "cur"."HOME_RBI") as "HOME_RBI",
        COALESCE("home_h"."SB", "cur"."HOME_SB") as "HOME_SB",
        COALESCE("home_h"."CS", "cur"."HOME_CS") as "HOME_CS",
        COALESCE("home_h"."BB", "cur"."HOME_BB") as "HOME_BB",
        COALESCE("home_h"."HBP", "cur"."HOME_HBP") as "HOME_HBP",
        COALESCE("home_h"."SO", "cur"."HOME_SO") as "HOME_SO",
        COALESCE("home_h"."GDP", "cur"."HOME_GDP") as "HOME_GDP",
        COALESCE("home_h"."AVG", "cur"."HOME_AVG") as "HOME_AVG",
        COALESCE("home_p"."REC_TBF", "cur"."HOME_REC_TBF") as "HOME_REC_TBF",
        COALESCE("home_p"."REC_IP", "cur"."HOME_REC_IP") as "HOME_REC_IP",
        COALESCE("home_p"."REC_H", "cur"."HOME_REC_PH") as "HOME_REC_PH",
        COALESCE("home_p"."REC_HR", "cur"."HOME_REC_PHR") as "HOME_REC_PHR",
        COALESCE("home_p"."REC_BB", "cur"."HOME_REC_PBB") as "HOME_REC_PBB",
        COALESCE("home_p"."REC_HBP", "cur"."HOME_REC_PHBP") as "HOME_REC_PHBP",
        COALESCE("home_p"."REC_SO", "cur"."HOME_REC_PSO") as "HOME_REC_PSO",
        COALESCE("home_p"."REC_R", "cur"."HOME_REC_PR") as "HOME_REC_PR",
        COALESCE("home_p"."REC_ER", "cur"."HOME_REC_ER") as "HOME_REC_ER",
        COALESCE("home_p"."REC_ERA", "cur"."HOME_REC_ERA") as "HOME_REC_ERA",
        COALESCE("home_p"."REC_WHIP", "cur"."HOME_REC_WHIP") as "HOME_REC_WHIP",
        COALESCE("home_p"."TBF", "cur"."HOME_TBF") as "HOME_TBF",
        COALESCE("home_p"."IP", "cur"."HOME_IP") as "HOME_IP",
        COALESCE("home_p"."H", "cur"."HOME_PH") as "HOME_PH",
        COALESCE("home_p"."HR", "cur"."HOME_PHR") as "HOME_PHR",
        COALESCE("home_p"."BB", "cur"."HOME_PBB") as "HOME_PBB",
        COALESCE("home_p"."HBP", "cur"."HOME_PHBP") as "HOME_PHBP",
        COALESCE("home_p"."SO", "cur"."HOME_PSO") as "HOME_PSO",
        COALESCE("home_p"."R", "cur"."HOME_PR") as "HOME_PR",
        COALESCE("home_p"."ER", "cur"."HOME_ER") as "HOME_ER",
        COALESCE("home_p"."ERA", "cur"."HOME_ERA") as "HOME_ERA",
        COALESCE("home_p"."WHIP", "cur"."HOME_WHIP") as "HOME_WHIP",
        COALESCE("away_h"."REC_PA", "cur"."AWAY_REC_PA") as "AWAY_REC_PA",
        COALESCE("away_h"."REC_AB", "cur"."AWAY_REC_AB") as "AWAY_REC_AB",
        COALESCE("away_h"."REC_R", "cur"."AWAY_REC_R") as "AWAY_REC_R",
        COALESCE("away_h"."REC_H", "cur"."AWAY_REC_H") as "AWAY_REC_H",
        COALESCE("away_h"."REC_2B", "cur"."AWAY_REC_2B") as "AWAY_REC_2B",
        COALESCE("away_h"."REC_3B", "cur"."AWAY_REC_3B") as "AWAY_REC_3B",
        COALESCE("away_h"."REC_HR", "cur"."AWAY_REC_HR") as "AWAY_REC_HR",
        COALESCE("away_h"."REC_RBI", "cur"."AWAY_REC_RBI") as "AWAY_REC_RBI",
        COALESCE("away_h"."REC_SB", "cur"."AWAY_REC_SB") as "AWAY_REC_SB",
        COALESCE("away_h"."REC_CS", "cur"."AWAY_REC_CS") as "AWAY_REC_CS",
        COALESCE("away_h"."REC_BB", "cur"."AWAY_REC_BB") as "AWAY_REC_BB",
        COALESCE("away_h"."REC_HBP", "cur"."AWAY_REC_HBP") as "AWAY_REC_HBP",
        COALESCE("away_h"."REC_SO", "cur"."AWAY_REC_SO") as "AWAY_REC_SO",
        COALESCE("away_h"."REC_GDP", "cur"."AWAY_REC_GDP") as "AWAY_REC_GDP",
        COALESCE("away_h"."REC_AVG", "cur"."AWAY_REC_AVG") as "AWAY_REC_AVG",
        COALESCE("away_h"."PA", "cur"."AWAY_PA") as "AWAY_PA",
        COALESCE("away_h"."AB", "cur"."AWAY_AB") as "AWAY_AB",
        COALESCE("away_h"."R", "cur"."AWAY_R") as "AWAY_R",
        COALESCE("away_h"."H", "cur"."AWAY_H") as "AWAY_H",
        COALESCE("away_h"."2B", "cur"."AWAY_2B") as "AWAY_2B",
        COALESCE("away_h"."3B", "cur"."AWAY_3B") as "AWAY_3B",
        COALESCE("away_h"."HR", "cur"."AWAY_HR") as "AWAY_HR",
        COALESCE("away_h"."RBI", "cur"."AWAY_RBI") as "AWAY_RBI",
        COALESCE("away_h"."SB", "cur"."AWAY_SB") as "AWAY_SB",
        COALESCE("away_h"."CS", "cur"."AWAY_CS") as "AWAY_CS",
        COALESCE("away_h"."BB", "cur"."AWAY_BB") as "AWAY_BB",
        COALESCE("away_h"."HBP", "cur"."AWAY_HBP") as "AWAY_HBP",
        COALESCE("away_h"."SO", "cur"."AWAY_SO") as "AWAY_SO",
        COALESCE("away_h"."GDP", "cur"."AWAY_GDP") as "AWAY_GDP",
        COALESCE("away_h"."AVG", "cur"."AWAY_AVG") as "AWAY_AVG",
        COALESCE("away_p"."REC_TBF", "cur"."AWAY_REC_TBF") as "AWAY_REC_TBF",
        COALESCE("away_p"."REC_IP", "cur"."AWAY_REC_IP") as "AWAY_REC_IP",
        COALESCE("away_p"."REC_H", "cur"."AWAY_REC_PH") as "AWAY_REC_PH",
        COALESCE("away_p"."REC_HR", "cur"."AWAY_REC_PHR") as "AWAY_REC_PHR",
        COALESCE("away_p"."REC_BB", "cur"."AWAY_REC_PBB") as "AWAY_REC_PBB",
        COALESCE("away_p"."REC_HBP", "cur"."AWAY_REC_PHBP") as "AWAY_REC_PHBP",
        COALESCE("away_p"."REC_SO", "cur"."AWAY_REC_PSO") as "AWAY_REC_PSO",
        COALESCE("away_p"."REC_R", "cur"."AWAY_REC_PR") as "AWAY_REC_PR",
        COALESCE("away_p"."REC_ER", "cur"."AWAY_REC_ER") as "AWAY_REC_ER",
        COALESCE("away_p"."REC_ERA", "cur"."AWAY_REC_ERA") as "AWAY_REC_ERA",
        COALESCE("away_p"."REC_WHIP", "cur"."AWAY_REC_WHIP") as "AWAY_REC_WHIP",
        COALESCE("away_p"."TBF", "cur"."AWAY_TBF") as "AWAY_TBF",
        COALESCE("away_p"."IP", "cur"."AWAY_IP") as "AWAY_IP",
        COALESCE("away_p"."H", "cur"."AWAY_PH") as "AWAY_PH",
        COALESCE("away_p"."HR", "cur"."AWAY_PHR") as "AWAY_PHR",
        COALESCE("away_p"."BB", "cur"."AWAY_PBB") as "AWAY_PBB",
        COALESCE("away_p"."HBP", "cur"."AWAY_PHBP") as "AWAY_PHBP",
        COALESCE("away_p"."SO", "cur"."AWAY_PSO") as "AWAY_PSO",
        COALESCE("away_p"."R", "cur"."AWAY_PR") as "AWAY_PR",
        COALESCE("away_p"."ER", "cur"."AWAY_ER") as "AWAY_ER",
        COALESCE("away_p"."ERA", "cur"."AWAY_ERA") as "AWAY_ERA",
        COALESCE("away_p"."WHIP", "cur"."AWAY_WHIP") as "AWAY_WHIP"
    from {{ ref('stg_current_game_data') }} as "cur"
    left join {{ ref('fct_team_recent_summary') }} as "home_recent" 
        on "cur"."SEASON_ID" = "home_recent"."SEASON_ID" 
        and "cur"."HOME_NM" = "home_recent"."TEAM_NM" 
        and "cur"."G_DT" = "home_recent"."G_DT" 
    left join {{ ref('fct_team_recent_summary') }} as "away_recent"
        on "cur"."SEASON_ID" = "away_recent"."SEASON_ID" 
        and "cur"."AWAY_NM" = "away_recent"."TEAM_NM" 
        and "cur"."G_DT" = "away_recent"."G_DT" 
    left join {{ ref('fct_team_hitter_daily_stats') }} as "home_h" 
        on "cur"."SEASON_ID" = "home_h"."SEASON_ID" 
        and "cur"."HOME_NM" = "home_h"."TEAM_NM" 
        and "cur"."G_DT" = "home_h"."G_DT" 
    left join {{ ref('fct_team_hitter_daily_stats') }} as "away_h"
        on "cur"."SEASON_ID" = "away_h"."SEASON_ID" 
        and "cur"."AWAY_NM" = "away_h"."TEAM_NM" 
        and "cur"."G_DT" = "away_h"."G_DT" 
    left join {{ ref('fct_team_pitcher_daily_stats') }} as "home_p" 
        on "cur"."SEASON_ID" = "home_p"."SEASON_ID" 
        and "cur"."HOME_NM" = "home_p"."TEAM_NM" 
        and "cur"."G_DT" = "home_p"."G_DT" 
    left join {{ ref('fct_team_pitcher_daily_stats') }} as "away_p"
        on "cur"."SEASON_ID" = "away_p"."SEASON_ID" 
        and "cur"."AWAY_NM" = "away_p"."TEAM_NM" 
        and "cur"."G_DT" = "away_p"."G_DT" 
)

select * from game_predict
