

# Packages ---------------------------------------------------------

library(dplyr)
library(sf)
library(caret)
library(data.table)
library(rgdal)

library(party)
library(mlbench)

library(doParallel)
library(foreach)
library(tictoc)

library(readr)
library(tibble)
library(lwgeom)

# functions

get_nearest_features_using_grid <- function(listofgrids,columns = columns){
  for(i in 1:length(listofgrids[[1]])){
    message(paste0("Grid:", i))
    test1 <- listofgrids[[1]][[i]] %>% st_sf() %>% st_set_crs(27700)
    test1$pipe_uq_id <- seq.int(nrow(test1))
    test2 <- listofgrids[[2]][[i]] %>% st_sf() %>% st_set_crs(27700)
    
    # get_nearest_features adding 2 new columns to x
    test_res <- test2[st_nearest_feature(test1,test2), columns]
    added_col <- as.data.frame(test_res[,columns])
    
    # merge the obj above
    added_col <- added_col[,columns]
    test_result <- cbind(as.data.frame(test1),added_col)
    final_test_result <- plyr::rbind.fill(test_result)
    
    if (i == 1) {
      final.out <- final_test_result
    } else{
      final.out <- plyr::rbind.fill(final.out, final_test_result)
    }
  }
  return(final.out)
}

# merge wills geometry to my proxy_bearing_atts

proxy_bearing_atts <- readRDS("D:/STW PDAS/Phase 2 datasets/proxy_bearing_and_attrs.rds")
will_proxies <- st_read("D:/STW PDAS/Phase 2 datasets/sewer_with_proxy/full_offset_proxy_geometries_v2.shp")
year_lookup_updated <- fread("D:/STW PDAS/Phase 2 datasets/updated_building_polygons/year_lookup_updated.csv")

# proxy_bearing_atts

will_proxies <- will_proxies #%>% select(-bearing)

# match tayo's sewer id with will's Tag

# proxy_bearing_atts <- proxy_bearing_atts %>% #filter(uid == "P") %>%
#   mutate(
#     swr_id = case_when(
#       nchar(read_order) == 1 ~ paste0("P000000000", read_order),
#       nchar(read_order) == 2 ~ paste0("P00000000", read_order),
#       nchar(read_order) == 3 ~ paste0("P0000000", read_order),
#       nchar(read_order) == 4 ~ paste0("P000000", read_order),
#       nchar(read_order) == 5 ~ paste0("P00000", read_order),
#       nchar(read_order) == 6 ~ paste0("P0000", read_order),
#       nchar(read_order) == 7 ~ paste0("P000", read_order),
#       nchar(read_order) == 8 ~ paste0("P00", read_order),
#       nchar(read_order) == 9 ~ paste0("P0", read_order)))

# merge wills proxy and tayos proxie

updated_proxy_bearing_atts <- proxy_bearing_atts %>% 
  as.data.frame() %>% 
  select(proxy_id, sys_type, bearing, CONFIDENCE, MATERIA, DIAMETE) %>% 
  inner_join(will_proxies, by = c("sys_type"="fnl_sys", "proxy_id"="nw_nq_d"))

# generate nw proxy_id
updated_proxy_bearing_atts$pip_q_d <- paste0("P",as.integer(100000000 + seq.int(nrow(updated_proxy_bearing_atts))))
updated_proxy_bearing_atts <- updated_proxy_bearing_atts %>% select(pip_q_d, sys_type, bearing, CONFIDENCE, MATERIA, DIAMETE, geometry)
updated_proxy_bearing_atts <- updated_proxy_bearing_atts %>% st_sf() %>% st_set_crs(27700)

# add the yearlaid
source("D:/STW PDAS/Git/pdas_mapping/pipe-depth-from-lidar-images/scripts/4 - Subsetter.R")
link_nbp <- "D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/"
phase_2_props_with_age <- st_read(paste0(link_nbp,"phase_2_props_with_age.shp"))
tic()
listofgrids <- createGrid(mainshapes = updated_proxy_bearing_atts, phase_2_props_with_age, XCount = 5, YCount = 5,
                          buffer = 300, mainexpand = F, extrasexpand = T, removeNull = T, report = T)
toc()

tic()
updated_proxy_bearing_atts_nearest <- get_nearest_features_using_grid(listofgrids, columns = c("res_buil_1"))
toc()

# rename yearlaid column and write file
updated_proxy_bearing_atts_nearest <- updated_proxy_bearing_atts_nearest %>%
  rename(YEARLAI = added_col) %>%
  select(pip_q_d, sys_type, bearing, CONFIDENCE, MATERIA, DIAMETE, YEARLAI, geometry) %>% 
  st_sf() %>% st_set_crs(27700)
updated_proxy_bearing_atts_nearest %>% summary()
# save.image("D:/STW PDAS/Phase 2 datasets/sewer_with_proxy/updated_proxy_bearing_atts_nearest.rdata")

# add confidence column, rename confidence column,ensure the id is a P123456789 and merge with lookup to add mdn_ylaid
updated_proxy_bearing_atts_nearest2 <- updated_proxy_bearing_atts_nearest %>% 
  mutate(YR_CONF = "A",
         AT_YR_CONF = paste0(CONFIDENCE,YR_CONF)) %>% 
  left_join(year_lookup_updated[,c("res_buil_1","MEDIAN_YEARLAID")], by = c("YEARLAI"="res_buil_1")) %>% 
  rename(MDN_YEARLAID = MEDIAN_YEARLAID) %>% 
           select(-YR_CONF,-CONFIDENCE,-YEARLAI)

# DELETE BELOW >>>>>>>>>>>>>>>>>>>>
# proxy_bearing_atts_x3 <- proxy_bearing_atts_x2 %>% 
#   mutate(pip_q_d = ifelse(pip_q_d == "P1.01e+08", "P101000000",
#                           ifelse(pip_q_d == "P1.02e+08","P102000000",as.character(pip_q_d))))
# 
# updated_proxy_bearing_atts_nearest2 <- proxy_bearing_atts_x3
# 
# st_geometry(proxy_bearing_atts_x3) <- NULL
# proxy_bearing_atts_x3 %>% arrange(pip_q_d)
# proxy_bearing_atts_x3 %>% arrange(desc(pip_q_d))
# DELETE ABOVE <<<<<<<<<<<<<<<<<<<<

  
st_write(updated_proxy_bearing_atts_nearest2, "D:/STW PDAS/Phase 2 datasets/sewer_with_proxy/updated_proxy_bearing_atts_05012021.shp")
                  