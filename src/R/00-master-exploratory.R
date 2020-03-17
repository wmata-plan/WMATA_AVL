#*******************************************************************************
#PROJECT:       WMATA AVL QJ
#DATE CREATED:  Tue Mar 17 01:53:58 2020
#TITLE:         Master File for R Exploratory Analysis
#AUTHOR:        Wylie Timmerman (wtimmerman@foursquareitp.com)
#*******************************************************************************


# Packages ----------------------------------------------------------------

suppressPackageStartupMessages({
  library(tidyverse) #1.2.1
  library(sf) #.7-4
  library(leaflet)
  library(leafem) #0.0.1
  library(mapview)
  library(janitor)
  library(tidytransit) #need 0.5 or higher i think
  library(plotly)
})


# Paths -------------------------------------------------------------------

if ("WylieTimmerman" %in% Sys.info()){
  sharepointpath <- "C:/OD/Foursquare ITP/Foursquare ITP SharePoint Site - Shared Documents/WMATA Queue Jump Analysis"
  datadir <- file.path(sharepointpath,
                       "Client Shared Folder",
                       "data",
                       "00-raw")
  
} else {
  stop('need to set dir where files live')
}

# Map Settings ------------------------------------------------------------

mapviewOptions(basemaps = c("CartoDB.Positron", 
                            "CartoDB.DarkMatter",
                            "Esri.WorldImagery"))
# mapviewOptions(viewer.suppress = TRUE) #if needed for full screen

wgs84CRS <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
DCCRS <- 2893L #using Maryland's, unit is usft

# Graphics ----------------------------------------------------------------

FITP_Theme <- theme(plot.title = element_text(family = "Calibri", 
                                              size = 12), 
                    axis.title = element_text(family = "Calibri", 
                                              size = 10),
                    # axis.text = element_text(family = "Calibri Light", 
                    #                          size = 10), 
                    legend.title = element_text(family = "Calibri", 
                                                size = 10), 
                    legend.text = element_text(family = "Calibri", 
                                               size = 10),
                    strip.text = element_text(family = "Calibri", 
                                              size = 10)
) 


# Other Params ------------------------------------------------------------
options("scipen" = 100, "digits" = 4)

