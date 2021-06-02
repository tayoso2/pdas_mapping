# Packages ----------------------------------------------------------------

library(dplyr)
library(magrittr)
library(sf)
library(sp)
library(caret)
library(data.table)
library(rgdal)

library(party)
library(mlbench)

library(doParallel)
library(tmap)
library(tictoc)


# load data ------------------------------------------------------------------

# check the data going into TES

link <- "C:/Users/TOsosanya/Downloads/Warwickshire_Phase2_SC/"
pdas_ppt_group <- st_read(paste0(link,"PDaS_Phase2_Data/1-ShapeBuildings/fids_pdas_supergroup8m_drawing_groups.shp"))
pdas_pipes <- st_read(paste0(link,"PDaS_Phase2_Data/2-ShapePipes/phase_2_sewers_with_bearings.shp"))
fold_in_build <- file.path("C:/Users/TOsosanya/Downloads/Warwickshire_Phase2_SC/PDaS_Phase2_Data/1-ShapeBuildings/")
fold_in_pipes <- file.path("C:/Users/TOsosanya/Downloads/Warwickshire_Phase2_SC/PDaS_Phase2_Data/2-ShapePipes")
fold_in_source <- file.path("C:/Users/TOsosanya/Downloads/sewpigen-SC_drawpipe/sewpigen-SC_drawpipe/Scripts/SimpleLineGen/Source")  
fold_out_pipes <- file.path("C:/Users/TOsosanya/Downloads\\stw_region_s24")

summary(pdas_ppt_group)
summary(pdas_pipes)



