
# Packages ----------------------------------------------------------------

library(dplyr)
library(magrittr)
library(sf)
library(sp)
library(lwgeom)
library(data.table)
library(rgdal)

library(party)
library(mlbench)

library(doParallel)
library(tmap)
library(tictoc)
        
# WHAT THIS SCRIPT DOES -----------------------------------------------------
# this script filters the proxies which are within 20m of a ppt (object is named "mapped_proxies" below)
# and then duplicates separated as "F" and "S" and leaves combined as "C"
       
# load data ------------------------------------------------------------------

link_one <- "D:/STW PDAS/Phase 2 datasets/updated_building_polygons/"
link_d <- "D:/STW PDAS/Phase 2 datasets/All_Sewerln_With_Attributes/"
link_nbp <- "D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/"
link_seven <- "D:/STW PDAS/Phase 2 datasets/stw_wide_system_preds/"
year_lookup <- read.csv(paste0(link_one,"year_lookup.csv"))
updated_sprint_5_sewers_2 <- st_read(paste0(link_d,"updated_sprint_5_sewers_2.shp"))
phase_2_props_with_age <- st_read(paste0(link_nbp,"phase_2_props_with_age.shp")) # building poly with age
stw_wide_system_preds <- read.csv(paste0(link_seven,"stw_wide_system_preds.csv"))

# functions ------------------------------------------------------------------
get_mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}
get_intersection_get_mode <- function(x,y,x_row.id=x_row.id,y_col.id=y_col.id,group.id = x_row.id,modal){
  # x = your x object
  # y = your y object
  # x_row.id = x unique id
  # y_col.id = y unique id
  # group.id = default is the x_row.id. This is what to group the object by and subsequently find the modal value for that group
  # modal = find the mode value of this var
  
  myintersect <- st_intersects(x,y)
  myintersect <- as.data.frame(myintersect)
  
  # convert myintersect column names
  myintersect$x_row.id <- myintersect$row.id
  myintersect$y_col.id <- myintersect$col.id
  myintersect <- myintersect[,c("x_row.id","y_col.id")]
  
  # add new UIDs
  x$x_row.id <- seq.int(nrow(x))
  y$y_col.id <- seq.int(nrow(y))
  
  # Get the id of the buildings in buffer
  
  x_col_id <- left_join(myintersect, as.data.frame(x), by = "x_row.id") %>% select(-geometry)
  df_combine <- left_join(x_col_id, as.data.frame(y), by = "y_col.id") %>% select(-geometry)
  colnames(df_combine)[which(names(df_combine) == group.id)] <- "group.id"
  colnames(df_combine)[which(names(df_combine) == modal)] <- "modal"
  
  # get the modal age
  df_2 <- df_combine %>%
    select(-c(x_row.id,y_col.id)) %>%
    group_by(group.id) %>%
    distinct()
  
  df <- df_2 %>%
    mutate(modal_value = get_mode(modal)) %>%
    as.data.frame()
  
  df <- df %>%
    select(group.id,modal_value) %>%
    distinct()
  
  colnames(df)[which(names(df) == "group.id")] <- group.id
  colnames(df)[which(names(df) == "modal")] <- modal
  
  return(df)
} # needs improving but works


# LOAD WILLS FILTERED PROXIES IN HERE-----------------------------------------


# data pre-processing
mapped_proxies <- updated_sprint_5_sewers_2 %>% filter(rod_flg == 1)
mapped_proxies
st_crs(mapped_proxies) <- 27700
st_crs(phase_2_props_with_age) <- 27700


# load("G:/Desktop/STW PDaS/Git/pdas_mapping/proxy sewer analysis/offset_proxies.rdata")


# retain the proxy sewers which intersect with buildings --------------------------------------------------------
# add buffer to the mapped_proxies
mapped_proxies_buffer <- st_buffer(mapped_proxies[,"Tag"],20)
mapped_proxies_buffer <- mapped_proxies_buffer %>% rename(Tag_buffer = Tag)

# join system type to the ppt data
phase_2_props_with_age <- phase_2_props_with_age %>% 
  left_join(stw_wide_system_preds[,c("fid","type_pred","CnNghTy","system_pred")], by ="fid")

# get the intersection between buffered proxies and the props ...
# ... to duplicate only where we have separate type -----------------------------------------------------------
intersect <- st_intersects(mapped_proxies_buffer,phase_2_props_with_age)
intersect <- as.data.frame(intersect)
mapped_proxies_buffer$row.id <- seq.int(nrow(mapped_proxies_buffer))
phase_2_props_with_age$col.id <- seq.int(nrow(phase_2_props_with_age))

# Get the id of the buildings in buffer
mapped_proxies_buffer_col_id <- left_join(intersect, as.data.frame(mapped_proxies_buffer), by = "row.id") %>% 
  select(-geometry)
combine <- left_join(mapped_proxies_buffer_col_id, as.data.frame(phase_2_props_with_age), by = "col.id") %>% 
  select(-geometry)

# get the modal age
year_lookup <- year_lookup %>% 
  mutate(res_buil_1_fact = as.integer(res_buil_1))
combine_two_0 <- combine %>% 
  select(-c(row.id,col.id)) %>% 
  group_by(Tag_buffer) %>% 
  left_join(year_lookup[,c("res_buil_1", "res_buil_1_fact")], by = "res_buil_1") %>%
  select(-c(fid,propgroup,count)) %>% 
  distinct()
combine_two <- combine_two_0 %>%
  #arrange(year_index) %>%
  select(Tag_buffer,P_Type, res_buil_1, res_buil_1_fact, system_pred) %>% 
  mutate(res_buil_1 = as.factor(res_buil_1),
         res_buil_1_mode = get_mode(res_buil_1),
         system_pred_mode = get_mode(system_pred)) %>% 
  as.data.frame()
combine_three <- combine_two %>% 
  select(Tag_buffer,res_buil_1_mode,system_pred_mode) %>% 
  distinct()
duplicate_me <- mapped_proxies %>% 
  inner_join(combine_three, by = c("Tag"="Tag_buffer")) %>% 
  filter(system_pred_mode == "separate")

# duplicate only where we have separate type -----------------------------------------------------
duplicate_me_test = NULL
duplicate_me_test = duplicate_me
duplicate_me_test$sewer_index <- 1:dim(duplicate_me_test[1])
for (i in duplicate_me_test$sewer_index) {
  # add 2 to all
  duplicate_me_test$geom[[i]][[1]] <- duplicate_me_test[["geometry"]][[i]][[1]]+(-2)
}

print(duplicate_me_test) # check if the duplication occurred, new geometry columns is "geom"
st_geometry(duplicate_me_test) <- "geom"
st_crs(duplicate_me_test) <- 27700

# add to mapped proxies and delete geometry column.. tag the duplicates as F and S and the anti join as C
proxy_foul <- duplicate_me_test %>% select(-geometry) %>% rename(geometry = geom) %>% 
  mutate(proxy_purpose = "F") %>% select(-sewer_index)
proxy_combined <- mapped_proxies %>% 
  anti_join(as.data.frame(proxy_foul), by = "Tag") %>% 
  mutate(proxy_purpose = "C") %>% 
  inner_join(unique(combine_two[,c("Tag_buffer","res_buil_1_mode", "system_pred_mode")]), by = c("Tag"="Tag_buffer"))
proxy_surface <- mapped_proxies %>% 
  inner_join(as.data.frame(proxy_foul[,c("Tag")]), by = "Tag") %>% 
  rename(geometry = geometry.x) %>% 
  select(-geometry.y) %>% 
  mutate(proxy_purpose = "S") %>% 
  inner_join(unique(combine_two[,c("Tag_buffer","res_buil_1_mode", "system_pred_mode")]), by = c("Tag"="Tag_buffer"))
proxy_sewer_net <- rbind(proxy_foul,proxy_combined,proxy_surface)
proxy_sewer_net <- proxy_sewer_net[!duplicated(proxy_sewer_net$geometry),] 

# plot the above
tmap_mode("view")
qtm(mapped_proxies_test,lines.col = "blue", basemaps = "Esri.WorldStreetMap") + qtm(mapped_proxies,lines.col = "red",basemaps = "Esri.WorldStreetMap")
st_write(proxy_sewer_net,"proxy_sewer_network.shp")



