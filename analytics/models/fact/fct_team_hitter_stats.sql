with hitter_stats as (
        select "SEASON_ID", "TEAM_NM", 
        sum("G") as "G",
        sum("PA") as "PA",
        sum("AB") as "AB",
        sum("R") as "R",
        sum("H") as "H",
        sum("2B") as "2B",
        sum("3B") as "3B",
        sum("HR") as "HR",
        sum("TB") as "TB",
        sum("RBI") as "RBI",
        sum("SAC") as "SAC",
        sum("SF") as "SF",
        sum("BB") as "BB",
        sum("IBB") as "IBB",
        sum("HBP") as "HBP",
        sum("SO") as "SO",
        sum("GDP") as "GDP",
        sum("SLG") as "SLG",
        sum("OBP") as "OBP",
        sum("OPS") as "OPS",
        sum("MH") as "MH",
        sum("RISP") as "RISP",
        sum("PH_BA") as "PH_BA",
        sum("XBH") as "XBH",
        sum("GO") as "GO",
        sum("AO") as "AO",
        sum("GO_AO") as "GO_AO",
        sum("GW_RBI") as "GW_RBI",
        sum("BB_K") as "BB_K",
        sum("P_PA") as "P_PA",
        sum("ISOP") as "ISOP",
        sum("XR") as "XR",
        sum("GPA") as "GPA" 
    from {{ source('public', 'player_hitter_stats') }}
    group by "SEASON_ID", "TEAM_NM"
	order by "SEASON_ID", "TEAM_NM"
)

select * from hitter_stats