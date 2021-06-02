

# Packages ---------------------------------------------------------------------
library(sf)
library(tmap)
library(plyr)
library(dplyr)
library(magrittr)
library(data.table)
library(tibble)

# load data
link_f_o <- "D:/STW PDAS/Phase 2 datasets/Flag overlap/"
# new_polys_s24_drawing_groups <- fread(paste0(link_f_o,"new_polys_s24_drawing_groups.csv"))
# new_polys_pdas_drawinggroups <- fread(paste0(link_f_o,"new_polys_pdas_drawinggroups.csv"))
pdas_survey_buildings <- fread(paste0(link_f_o,"pdas_survey_buildings.csv"))
s24_survey_buildings <- fread(paste0(link_f_o,"s24_survey_buildings.csv"))
stw_region_s24 <- st_read("D:/STW PDAS/s24_phase_2/s24_lines_with_attrs_25012021.shp")
stw_region_pdas <- st_read("D:/STW PDAS/pdas_phase_2/pdas_lines_with_attrs_14012021.shp")


# Function

tag_anomalies_cat_pred <- function(x,y,col){
  # x is the model training data formatted as_tibble()
  # y is the new data you want to predict on. Also formatted as_tibble()
  # j is the number of columns
  x_col <- x[,col] %>% unlist()
  y_col <- y[,col] %>% unlist()
  
  "%ni%" <- Negate("%in%")
  y_filtered <- 
    y_filtered <- y[match(unlist(y[,col]), x_col, nomatch = 0) > 0, ]
  return(y_filtered)
}

# PROCESSING ---------------------------------------------------------------------
# create df with 
s24_survey_buildings <- unique(s24_survey_buildings[,c("fid")])
pdas_survey_buildings <- unique(pdas_survey_buildings[,c("fid","srvy_bl")])


# merge the S24 and PDaS surveyed assets
s24_survey_buildings <- s24_survey_buildings %>% 
  mutate(survey_flag = "s24_surveyed")
pdas_survey_buildings <- pdas_survey_buildings %>% 
  mutate(survey_flag = "pdas_surveyed")
surveyed_assets <- plyr::rbind.fill(s24_survey_buildings, pdas_survey_buildings) %>% 
  select(-srvy_bl)


# S24
# the original s24 data ------------------------------------------------------------------------------------
stw_region_s24 <- stw_region_s24 %>% dplyr::rename(propgroup = propgrp)
stw_region_s24$pip_q_d <- paste0("INF",as.integer(200000000+seq.int(nrow(stw_region_s24)))) # changed
stw_region_s24 <- stw_region_s24 %>% 
  mutate(AT_YR_CONF = paste0(Confdnc,"A"),AT_YR_CONF = as.factor(AT_YR_CONF)) %>% 
  select(-Confdnc) # changed
s24_asset <- as.data.frame(stw_region_s24)

# convert fid to propgroup in s24 (use both fid and propgroup to select ALL surveyed) -----------------------
surveyed_assets_s24version <- surveyed_assets %>% dplyr::rename(propgroup = fid)
# remove the s24s in surveyed
stw_region_s24_flagged <- tag_anomalies_cat_pred(as_tibble(surveyed_assets_s24version),s24_asset , "propgroup") %>% 
  mutate(survey_flag = "surveyed")
stw_region_s24_flagged_withfid <- tag_anomalies_cat_pred(as_tibble(surveyed_assets_s24version %>% dplyr::rename(fid = propgroup)),s24_asset , "fid") %>%
  mutate(survey_flag = "surveyed")
stw_region_s24_flagged <- rbind(stw_region_s24_flagged_withfid,stw_region_s24_flagged) %>% distinct()


# test the above
stw_region_s24 %>% filter(propgroup == "osgb1000002607035184")
stw_region_s24_flagged %>% filter(propgroup == "osgb1000002607035184") # unsurv
surveyed_assets_s24version %>% filter(propgroup == "osgb1000002607035184") # unsurv

stw_region_s24_flagged %>% filter(propgroup == "osgb1000020747701") # surv
surveyed_assets_s24version %>% filter(propgroup == "osgb1000020747701") # surv

# anti_join to select the one that were unsurveyed ----------------------------------------------------------
stw_region_s24_unflagged <- stw_region_s24 %>% 
  anti_join(stw_region_s24_flagged, by = "pip_q_d") %>% 
  mutate(survey_flag = "inferred")

# select the pdas surveyed and s24 surveyed ones
## first, rewrite the objects to be used.
pdas_surv <- as_tibble(surveyed_assets_s24version %>%
    filter(survey_flag == "pdas_surveyed"))
s24_surv <- as_tibble(surveyed_assets_s24version %>%
                         filter(survey_flag == "s24_surveyed"))
stw_region_s24_flagged_nogeom <- as.data.frame(stw_region_s24_flagged)

# select s24 surveyed ones
stw_region_s24_flagged_s24ssurv <- stw_region_s24_flagged %>%
  inner_join(s24_surv[,"propgroup"], by = "propgroup") %>%
  mutate(survey_flag = "s24_surveyed")
stw_region_s24_flagged_s24ssurv_withfid <- stw_region_s24_flagged %>%
  inner_join(s24_surv[,"propgroup"], by = c("fid"="propgroup")) %>%
  mutate(survey_flag = "s24_surveyed")
stw_region_s24_flagged_s24ssurv <- rbind.fill(stw_region_s24_flagged_s24ssurv,stw_region_s24_flagged_s24ssurv_withfid) %>%
  distinct()

# select pdas surveyed ones
stw_region_s24_flagged_pdassurv <- stw_region_s24_flagged %>%
  anti_join(stw_region_s24_flagged_s24ssurv, by = "pip_q_d") %>% 
  mutate(survey_flag = "pdas_surveyed")


# merge all 3 pdas surv, s24 surv, inferred
stw_region_s24_flagged_combined <- rbind.fill(stw_region_s24_flagged_pdassurv,stw_region_s24_flagged_s24ssurv,stw_region_s24_unflagged) %>% 
  mutate(survey_flag = as.factor(survey_flag)) %>% 
  distinct()
summary(stw_region_s24_flagged_combined)
st_write(stw_region_s24_flagged_combined,paste0(link_f_o,"stw_region_s24_flagged_25012021.shp"))




# PDAS
# the original pdas data ---------------------------------------------------------------
stw_region_pdas <- stw_region_pdas %>% dplyr::rename(propgroup = propgrp)
stw_region_pdas$pip_q_d <- paste0("INF",as.integer(100000000+seq.int(nrow(stw_region_pdas)))) # changed
stw_region_pdas <- stw_region_pdas %>% 
  mutate(AT_YR_CONF = paste0(Confdnc,"A"),AT_YR_CONF = as.factor(AT_YR_CONF)) %>% 
  select(-Confdnc) # changed
pdas_asset <- as.data.frame(stw_region_pdas)

# convert fid to propgroup in pdas -------------------------------------------------------
surveyed_assets_pdasversion <- surveyed_assets %>% dplyr::rename(propgroup = fid)
# remove the s24s in surveyed
stw_region_pdas_flagged <- tag_anomalies_cat_pred(as_tibble(surveyed_assets_pdasversion), 
                                                  pdas_asset, "propgroup") %>% 
  mutate(survey_flag = "surveyed")
stw_region_pdas_flagged_withfid <- tag_anomalies_cat_pred(as_tibble(surveyed_assets_pdasversion %>% 
                                                                      dplyr::rename(fid = propgroup)),pdas_asset , "fid") %>%
  mutate(survey_flag = "surveyed")
stw_region_pdas_flagged <- rbind(stw_region_pdas_flagged_withfid,stw_region_pdas_flagged) %>% distinct()

# test the above
stw_region_pdas %>% filter(propgroup == "osgb1000035352474")
stw_region_pdas_flagged %>% filter(propgroup == "osgb1000035352474") # surv
surveyed_assets_pdasversion %>% filter(propgroup == "osgb1000035352474") # surv

# anti_join to select the one that were unsurveyed ------------------------------------------
stw_region_pdas_unflagged <- stw_region_pdas %>% 
  anti_join(stw_region_pdas_flagged, by = "pip_q_d") %>% 
  mutate(survey_flag = "inferred")

# select the pdas surveyed and s24 surveyed ones
## first, rewrite the objects to be used.
pdas_surv <- as_tibble(surveyed_assets_pdasversion %>%
                         filter(survey_flag == "pdas_surveyed"))
s24_surv <- as_tibble(surveyed_assets_pdasversion %>%
                        filter(survey_flag == "s24_surveyed"))
stw_region_pdas_flagged_nogeom <- as.data.frame(stw_region_pdas_flagged)

# select pdas surveyed ones
stw_region_pdas_flagged_pdassurv <- stw_region_pdas_flagged %>%
  inner_join(pdas_surv[,"propgroup"], by = "propgroup") %>%
  mutate(survey_flag = "pdas_surveyed")
stw_region_pdas_flagged_pdassurv_withfid <- stw_region_pdas_flagged %>%
  inner_join(pdas_surv[,"propgroup"], by = c("fid"="propgroup")) %>%
  mutate(survey_flag = "pdas_surveyed")
stw_region_pdas_flagged_pdassurv <- plyr::rbind.fill(stw_region_pdas_flagged_pdassurv_withfid,stw_region_pdas_flagged_pdassurv) %>%
  distinct()

# select s24 surveyed ones
stw_region_pdas_flagged_s24surv <- stw_region_pdas_flagged %>%
  anti_join(stw_region_pdas_flagged_pdassurv, by = "pip_q_d") %>%
  mutate(survey_flag = "s24_surveyed")

# merge all 3 pdas surv, s24 surv, inferred
stw_region_pdas_flagged_combined <- plyr::rbind.fill(stw_region_pdas_flagged_pdassurv,stw_region_pdas_flagged_s24surv,stw_region_pdas_unflagged) %>% 
  mutate(survey_flag = as.factor(survey_flag)) %>% 
  distinct()
summary(stw_region_pdas_flagged_combined)
st_write(stw_region_pdas_flagged_combined,paste0(link_f_o,"stw_region_pdas_flagged_14012021.shp"))

