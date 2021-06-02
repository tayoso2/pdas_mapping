# building age data infill
# 27-08-2020



library(data.table)
library(dplyr)
library(sf)
library(units)
library(tmap)
library(ggplot2)
library(rgdal)
library(tictoc)



# read building age data, year_lookup and rdata ----------------------------------------------------------------

link_one <- "D:/STW PDAS/Phase 2 datasets/updated_building_polygons/"
link_two <- "D:/STW PDAS/Phase 2 datasets/sprint_5_sewers/"
link_j <- "D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/"
building_age <- st_read(paste0(link_one,"building_age_centroid.shp"))
sewer <- st_read(paste0(link_two,"sprint_5_sewers.shp"))
year_lookup <- read.csv(paste0(link_one,"year_lookup.csv"))
sprint_4_props <- st_read(paste0(link_j,"sprint_4_props_with_atts_age.shp"))
#saveRDS(dlx,paste0(link_one,"postcode_age.rds"))
dlx <- readRDS(paste0(link_one,"postcode_age.rds"))
summary(building_age)
summary(sewer)

# find the nearest building to sewers to grab its sewer age using st_nearest ---------------------

# first transform crs to 27700

st_crs(building_age) <- 27700
st_crs(dlx) <- 27700

get_nearest_features <- function(x, y, pipeid, id, feature) {
  
  # pipeid is id of x
  # id is id of y
  # feature is attribute to be added to x from y
  # function needs some improvement
  
  # Determine CRSs
  message(paste0("x Coordinate reference system is ESPG: ", st_crs(x)$epsg))
  message(paste0("y Coordinate reference system is ESPG: ", st_crs(y)$epsg))
  
  # Transform y CRS to x CRS if required
  if (st_crs(x) != st_crs(y)) {
    message(paste0(
      "Transforming y coordinate reference system to ESPG: ",
      st_crs(x)$epsg
    ))
    y <- st_transform(y, st_crs(x))
  }
  
  # Get rownumber of x
  x$rowguid <- seq.int(nrow(x))
  y$rowguid <- seq.int(nrow(y))
  
  # edit column names
  colnames(x)[which(names(x) == pipeid)] <- "id"
  colnames(y)[which(names(y) == id)] <- "id"
  colnames(y)[which(names(y) == feature)] <- "feature"
  
  # get nearest features
  nearest <- st_nearest_feature(x,y)
  x$index <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- merge(x, as.data.frame(y2), by.x = "index", by.y = "rowguid")
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id.x")] <- pipeid
  colnames(output)[which(names(output) == "id.y")] <- id
  colnames(output)[which(names(output) == "feature")] <- feature
  return(output)
}

pair_age <- get_nearest_features(building_age,dlx,"OBJECTID","Postcod","Year")


# convert year to integer and then categorize using look up table

pair_categorise <- pair_age %>% mutate(Year = as.integer(Year)) %>% 
  left_join(year_lookup, by = c("Year"="post_year")) %>% 
  mutate(res_buil_1  = ifelse(is.na(res_buil_1.x),as.character(res_buil_1.y),as.character(res_buil_1.x))) %>% 
  select(-c(Year,res_buil_1.x,res_buil_1.y))

pair_categorise %>%
  filter(OBJECTID == "1252153")

# subset age categ. with na and those without na

pair_categorise_nona <- pair_categorise %>% filter(!is.na(res_buil_1))
pair_categorise_na <- pair_categorise %>% filter(is.na(res_buil_1))


# assign age attribute from subset with nona to that with na using nearest futures function

pair_age_na_nona <- get_nearest_features(pair_categorise_na[,c("OBJECTID","res_buil_1","geometry")],
                                         pair_categorise_nona,"OBJECTID","OBJECTID","res_buil_1")

# change column names and merge subsets with infilled na and that without na

names(pair_age_na_nona) <- c("OBJECTID", "null_res_buil_1", "rowguid", "index","id.y","res_buil_1","geometry")
pair_age_na_nona <- pair_age_na_nona %>% 
  select(OBJECTID,res_buil_1)
pair_categorise_nona <- pair_categorise_nona %>% 
  select(OBJECTID,res_buil_1)
infilled_building_age <- rbind(pair_categorise_nona,pair_age_na_nona)
infilled_building_age


# load in the sprint 4 data and assign nearest age attributes -----------------------------------------------

link_three <- "D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/"
sprint_4_props_with_atts <- st_read(paste0(link_three,"sprint_4_props_with_atts.shp"))


sprint_4_props_with_atts$newid <- seq.int(nrow(sprint_4_props_with_atts))
st_crs(sprint_4_props_with_atts) <- 27700
get_nearest_features_xycentroid <- function(x, y, pipeid, id, feature) {
  
  # pipeid is id of x
  # id is id of y
  # feature is attribute to be added to x from y
  # function needs some improvement
  
  # Determine CRSs
  message(paste0("x Coordinate reference system is ESPG: ", st_crs(x)$epsg))
  message(paste0("y Coordinate reference system is ESPG: ", st_crs(y)$epsg))
  
  # Transform y CRS to x CRS if required
  if (st_crs(x) != st_crs(y)) {
    message(paste0(
      "Transforming y coordinate reference system to ESPG: ",
      st_crs(x)$epsg
    ))
    y <- st_transform(y, st_crs(x))
  }
  
  # Get rownumber of x
  x$rowguid <- seq.int(nrow(x))
  y$rowguid <- seq.int(nrow(y))
  
  # edit column names
  colnames(x)[which(names(x) == pipeid)] <- "id"
  colnames(y)[which(names(y) == id)] <- "id"
  colnames(y)[which(names(y) == feature)] <- "feature"
  
  # convert x and y to centroid
  x <- st_centroid(x)
  y <- st_centroid(y)
  
  # get nearest features
  nearest <- st_nearest_feature(x,y)
  x$index <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- merge(x, as.data.frame(y2), by.x = "index", by.y = "rowguid")
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id.x")] <- pipeid
  colnames(output)[which(names(output) == "id.y")] <- id
  colnames(output)[which(names(output) == "feature")] <- feature
  return(output)
}

tic()
merged_iba_s4 <- get_nearest_features_centroid(sprint_4_props_with_atts,infilled_building_age,"newid","OBJECTID","res_buil_1")
toc()
columns = c("fid","propgroup","count","P_Type","res_buil_1","geometry")

# add the polygon back
merged_iba_s4_geom <- merged_iba_s4
st_geometry(merged_iba_s4_geom) <- NULL
merged_iba_s4_geom <- as.data.frame(merged_iba_s4_geom) %>% left_join(sprint_4_props_with_atts[,c("newid","geometry")], by = "newid")
merged_iba_s4_geom <- st_as_sf(merged_iba_s4_geom,crs = 27700)

# st_write(merged_iba_s4_geom[,columns],"D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/sprint_4_props_with_atts_age.shp")



# area analysis to filter out small polygons ----------------------------------------------------
new_builds <- st_read("D:/STW PDAS/Phase 2 datasets/updated_building_polygons/Detailed_Polygons_Intersected_Waste_Catchments_NoSAPID.shp")
ubp <- st_read("D:/STW PDAS/Phase 2 datasets/updated_building_polygons/updated_building_polygons.shp")


st_crs(new_builds) <- 27700
st_crs(ubp) <- 27700

tic()
new_builds$area_cal <- st_area(new_builds)
toc()

tic()
ubp$area_cal <- st_area(ubp)
toc()

new_builds_2 <- new_builds %>% mutate(area_cal = as.numeric(area_cal)) %>% filter(area_cal > 25)
ubp_2 <- ubp %>% mutate(area_cal = as.numeric(area_cal)) %>% filter(area_cal > 25)

# st_write(new_builds_2,"D:/STW PDAS/Phase 2 datasets/updated_building_polygons/Detailed_Polygons_Intersected_Waste_Catchments_NoSAPID.shp")
# st_write(ubp_2,"D:/STW PDAS/Phase 2 datasets/updated_building_polygons/updated_building_polygons_2.shp")
# saveRDS(new_builds,"D:/STW PDAS/Phase 2 datasets/updated_building_polygons/Detailed_Polygons_Intersected_Waste_Catchments_NoSAPID.rds")
# saveRDS(new_builds_2,"D:/STW PDAS/Phase 2 datasets/updated_building_polygons/Detailed_Polygons_Intersected_Waste_Catchments_NoSAPID_2.rds")


# load in the sprint 4 data and assign nearest age attributes

ubp_2$newid <- seq.int(nrow(ubp_2))
st_crs(ubp_2) <- 27700
get_nearest_features_centroid <- function(x, y, pipeid, id, feature) {
  
  # pipeid is id of x
  # id is id of y
  # feature is attribute to be added to x from y
  # function needs some improvement
  
  # Determine CRSs
  message(paste0("x Coordinate reference system is ESPG: ", st_crs(x)$epsg))
  message(paste0("y Coordinate reference system is ESPG: ", st_crs(y)$epsg))
  
  # Transform y CRS to x CRS if required
  if (st_crs(x) != st_crs(y)) {
    message(paste0(
      "Transforming y coordinate reference system to ESPG: ",
      st_crs(x)$epsg
    ))
    y <- st_transform(y, st_crs(x))
  }
  
  # Get rownumber of x
  x$rowguid <- seq.int(nrow(x))
  y$rowguid <- seq.int(nrow(y))
  
  # edit column names
  colnames(x)[which(names(x) == pipeid)] <- "id"
  colnames(y)[which(names(y) == id)] <- "id"
  colnames(y)[which(names(y) == feature)] <- "feature"
  
  # convert x to centroid
  x <- st_centroid(x)
  
  # get nearest features
  nearest <- st_nearest_feature(x,y)
  x$index <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- merge(x, as.data.frame(y2), by.x = "index", by.y = "rowguid")
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id.x")] <- pipeid
  colnames(output)[which(names(output) == "id.y")] <- id
  colnames(output)[which(names(output) == "feature")] <- feature
  return(output)
}

tic()
merged_iba_ubp <- get_nearest_features_centroid(ubp_2,infilled_building_age,"newid","OBJECTID","res_buil_1")
toc()

# save this because it takes long to process
saveRDS(merged_iba_ubp,"D:/STW PDAS/Phase 2 datasets/merged_iba_ubp.rds")
columns = c("fid","res_buil_1","geometry")

# add the polygon back
merged_iba_ubp_geom <- merged_iba_ubp
st_geometry(merged_iba_ubp_geom) <- NULL
merged_iba_ubp_geom <- as.data.frame(merged_iba_ubp_geom) %>% left_join(ubp_2[,c("newid","geometry")], by = "newid")
merged_iba_ubp_geom <- st_as_sf(merged_iba_ubp_geom,crs = 27700)

# replace the fid/geom here with that of sprint 4 props. Use anti_join
merged_iba_ubp_no_geom <- merged_iba_ubp_geom
st_geometry(merged_iba_ubp_no_geom) <- NULL
sprint_4_props_no_geom <- sprint_4_props
st_geometry(sprint_4_props_no_geom) <- NULL
sprint_4_props_no_geom <- new_builds %>% left_join(sprint_4_props_no_geom, by = "fid") # remove if this causes a problem
not_in_sprint_4 <- merged_iba_ubp_no_geom %>% anti_join(sprint_4_props_no_geom, by = "fid") %>% 
  select(fid)
not_in_sprint_4 <- not_in_sprint_4 %>% left_join(merged_iba_ubp_geom[,c("fid","res_buil_1")], by = "fid") # add geom back
# not_in_sprint_4$geometry <- paste0("MULTI",not_in_sprint_4$geometry)
not_in_sprint_4 <- st_as_sf(not_in_sprint_4)
save.image("D:/STW PDAS/Phase 2 datasets/building polygon analysis.rdata")


# tic()
# st_cast(not_in_sprint_4[1:1000,], "MULTIPOLYGON")
# toc()
# not_in_sprint_4_mp <- st_cast(not_in_sprint_4, "MULTIPOLYGON")
# not_in_sprint_4_mp <- not_in_sprint_4
# 
# not_in_sprint_4_mp$geometry <- as.character(not_in_sprint_4_mp$geometry)
# not_in_sprint_4_mp$geometry <- as.character(not_in_sprint_4_mp$geometry)
# not_in_sprint_4_mp$geometry <- gsub("POLYGON","",not_in_sprint_4_mp$geometry[1:10])
# not_in_sprint_4_mp$geometry <- gsub("list(c","MULTIPOLYGON ((",not_in_sprint_4_mp$geometry)

st_crs(not_in_sprint_4) <- 27700
st_crs(sprint_4_props_no_geom) <- 27700
phase_2_props <- plyr::rbind.fill(sprint_4_props_no_geom,not_in_sprint_4)
# phase_2_props <- plyr::rbind.fill(as.data.frame(sprint_4_props_no_geom),as.data.frame(not_in_sprint_4))

st_write(sprint_4_props_no_geom,"D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/sprint_4_props_no_geom.shp", GEOMETRY = "AS_WKT")
st_write(not_in_sprint_4,"D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/not_in_sprint_4.shp", GEOMETRY = "AS_WKT")
st_write(phase_2_props,"D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/phase_2_props_with_age.shp", GEOMETRY = "AS_WKT")

# delete unnecessary columns
combined_ubp <- st_read("D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/phase_2_props_clipped.shp")
combined_ubp <- combined_ubp[,c("fid","propgroup","area_cal","count","res_buil_1","P_Type")]
st_write(combined_ubp,"D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/phase_2_props_clipped.shp")


# infill missing age -----------------------------------------------------------------------------

phase_2_props_clipped <- st_read("D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/phase_2_props_clipped.shp")
st_crs(phase_2_props_clipped) <- 27700
with_age <- phase_2_props_clipped %>% filter(res_buil_1 != "UNKNOWN DATE") %>% mutate(id_0 = row_number())
without_age <- phase_2_props_clipped %>% filter(res_buil_1 == "UNKNOWN DATE") %>% mutate(id_1 = row_number()) %>% rename(res_buil_2 = res_buil_1)
inferred_age <- get_nearest_features_xycentroid(without_age,with_age,"id_1","id_0","res_buil_1")

# merge both age and infilled age back
inferred_age_2 <- inferred_age[,c("fid", "propgroup", "area_cal", "count", "res_buil_1", "P_Type","id_1")]
inferred_age_3 <- inferred_age_2 %>% as.data.frame() %>% select(-geometry) %>% 
  left_join(without_age[,"id_1"], by = "id_1") %>% 
  select(-id_1) %>% 
  st_as_sf()
with_age <- with_age %>% select(-id_0)
phase_2_props_with_age <- rbind(with_age,inferred_age_3) %>% mutate(res_buil_1 = as.character(res_buil_1)) %>% 
  filter(res_buil_1 != "") %>%  mutate(res_buil_1 = as.factor(res_buil_1))
phase_2_props_with_age
phase_2_props_with_age$res_buil_1 %>% summary()

st_write(phase_2_props_with_age,"D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/phase_2_props_with_age.shp")



