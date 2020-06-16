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
  library(extrafont)
  library(scales)
  library(patchwork)
  library(stplanr)
  library(dtplyr)
  library(leaflet.extras)
  library(tibbletime)
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

wgs84CRS <- 4326L
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


# Helpers -----------------------------------------------------------------

#conveninene function
make_extra_geom <- 
  function(df,lon,lat,name,crs){
    
    #not teh best way, oh well
    lon <- rlang::enquo(lon)
    lat <- rlang::enquo(lat)
    name <- rlang::enquo(name)
    
    lon_name <- rlang::as_label(lon)
    lat_name <- rlang::as_label(lat)
    # browser()
    
    extra <-
      df %>%
      #all this extra overhead because distinct doesn't play well when you
      #try to pass strings to it
      distinct(!!lon,!!lat) %>%
      drop_na() %>%
      st_as_sf(., 
               coords = c(lon_name, lat_name),
               crs = 4326L, #WGS84
               agr = "constant",
               remove = FALSE) %>%
      st_transform(crs = crs) %>%
      rename(!!name := geometry)
    
    #rejoin geoemtry to the original data frame and return that df for further 
    #piping
    df2 <-
      df %>%
      left_join(extra,
                by = purrr::set_names(c(lon_name,lat_name)))
  }
