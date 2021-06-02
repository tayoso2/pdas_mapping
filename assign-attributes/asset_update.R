
# lock r env
# renv::init()
# renv::restore()



library(renv)
library(sf)
library(dplyr)
library(tidyr)
library(magrittr)
library(data.table)
library(tictoc)


# set wd
setwd("D:/STW PDAS/Phase 2 datasets/descriptive stats/")

# read data -----------------------------------------------------

fids_props <- fread("D:\\STW PDAS\\Phase 2 datasets\\Flag overlap\\new_fids_all2.csv") # added
link_nbp <- "D:/STW PDAS/Phase 2 datasets/sprint_4_props_with_atts/"
link_e <- "D:/STW PDAS/Phase 2 datasets/Postcode/"

props_with_county_and_postcode <- read.csv(paste0(link_e,"props_with_county_and_postcode.csv"))
phase_2_props_with_age <- st_read(paste0(link_nbp,"phase_2_props_with_age.shp"))
year_lookup <- fread("D:/STW PDAS/Phase 2 datasets/updated_building_polygons/year_lookup_updated.csv")
new_pdas <- st_read("D:/STW PDAS/Phase 2 datasets/Flag overlap/pdas_updated_assetbase_01032021.shp")
new_s24 <- st_read("D:/STW PDAS/Phase 2 datasets/Flag overlap/s24_updated_assetbase_01032021.shp")

outst_pdas_poly <- st_read("D:/STW PDAS/Phase 2 datasets/PDaS_All_Pipes - With Proxy/PDaS_All_Pipes - With Proxy/Dataflow Execution/Dataflow Execution Folder/1-ShapeBuildings/[RIPPED]PDaS_Props_Poly.shp")
outst_s24_poly <- st_read("D:/STW PDAS/Phase 2 datasets/S24_All_Pipes - With Proxy/S24_All_Pipes - With Proxy/Dataflow Execution/Dataflow Execution Folder/1-ShapeBuildings/[RIPPED]S24_Props_Poly.shp")

# find area of phase2props AND merge fidsprops and phase2props datasets
phase_2_props_with_age$area <- st_area(phase_2_props_with_age)
props = phase_2_props_with_age[,c("fid","area")] %>% 
  right_join(fids_props, by = "fid") %>% 
  mutate(area = as.numeric(area))
props_filtered = props %>% filter(area >= 25) # test if all under 25m^2
# write the fids shp file to replace the phase_2_props_with_age
fiddle <- props_filtered[,c("fid","geometry")] %>% 
  inner_join(fids_props, by = "fid")
fiddle <- fiddle[!duplicated(fiddle$fid), ]
fiddle$area <- st_area(fiddle)

# Load functions ------------------------------------------------------------------

get_nearest_feature_column <- function(x, y, col = "column") {
  # x is the spatial dataframe you want to append a column to
  # y is the spatial dataframe you want to get the column from
  # col is the column(s) to add from y to x
  res <- y[st_nearest_feature(x, y), col]
  x[, col] <- res[, col]
  return(x)
}

create_lists <- function(x, splits = 29, each = 1) {
  # Description: This function will aid faster processing especially with for loop functions
  # x is the dataframe to split into lists
  # splits is the number of lists
  my_split <- base::rep(1:splits, each = each, length = nrow(x))
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  list_lists <- base::split(my_split2, my_split2$my_split)
  # list_lists <- purrr::map(my_split3$data, ~ split(., func_arg))
  return(list_lists)
}
get_nearest_feature_column_lists <- function(x,y, col = "id") {
  # x is the spatial dataframe you want to append a column to
  # y is the spatial dataframe you want to get the column from
  # col is the column(s) to add from y to x
  for (i in 1:length(x)) {
    message(paste0("list ",i))
    
    x[[i]] <-
      get_nearest_feature_column(x[[i]], y, col)
    
    if (i == 1) {
      output.data.test <- x[[i]]
    } else
    {
      output.data.test <-
        rbind(output.data.test, x[[i]])
    }
  }
  return(output.data.test)
}
remove_anomalies_cat_pred <- function(x, y, col) {
  # Description: this function removes rows from y that don't match the column classes in x
  # x is the model training data
  # y is the new data you want to have the same classes in the column you select,
  #   this is the data you want to predict on
  # col is the column containing the classes you want to match
  x_col <- x[, col] %>% unlist()
  y_col <- y[, col] %>% unlist()
  
  "%ni%" <- Negate("%in%")
  y_filtered <-
    y_filtered <- y[match(unlist(y[, col]), x_col, nomatch = 0) == 0, ]
  y_filtered_2 <- y %>% anti_join(y_filtered, by = col)
  return(y_filtered_2)
}
get_mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

get_one_to_one_distance <- function(x, x_geom, y_geom, splits = 50, centroid = TRUE) {
  
  # x is the dataframe with both the x geometry and the y geometry
  # x_geom is the geometry of the object on the lhs
  # y_geom is the geometry of the object on the rhs
  # splits is the number of lists
  # centroid default is TRUE. If TRUE convert x_geom and y_geom to centroid
  
  # duplicate x
  y <- x
  
  # assign the active geometry on the lhs and rhs
  st_geometry(x) <- x_geom
  st_geometry(y) <- y_geom
  
  # Transform y CRS to x CRS if required
  if (st_crs(x) != 4326) {
    message(paste0(
      "Transforming x coordinate reference system to ESPG: 4326"
    ))
    x <- st_transform(x, crs = 4326)
  }
  
  # Transform y CRS to x CRS if required
  if (st_crs(x) != st_crs(y)) {
    message(paste0(
      "Transforming y coordinate reference system to ESPG: ",
      st_crs(x)$epsg
    ))
    y <- st_transform(y, st_crs(x))
  }
  
  # Transform x_geom and y_geom to centroid
  if (isTRUE(centroid)) {
    message("Converting x_geom and y_geom to centroid")
    x <- st_centroid(x)
    y <- st_centroid(y)
  } else{
    message("Convert to centroid for faster processing")
  }
  
  # create a list of dataframes for x
  my_split <- base::rep(1:splits, each = 1, length = nrow(x))
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  list_x <- base::split(my_split2, my_split2$my_split)
  
  # create a list of dataframes for y  
  my_split <- base::rep(1:splits, each = 1, length = nrow(y))
  my_split2 <- dplyr::mutate(y, my_split = my_split)
  list_y <- base::split(my_split2, my_split2$my_split)
  
  # loop through each list and compute the distance
  for (i in 1:length(list_x)) {
    # message for progress
    message(paste0((i/length(list_x))*100,"% at ",Sys.time()))
    
    x_1 <- list_x[[i]]
    y_1 <- list_y[[i]]
    for (j in 1:nrow(list_x[[i]])) {
      # calculate the distance
      x_1$length_to_centroid[[j]] <- as.numeric(lwgeom::st_geod_distance(x_1[j, x_geom], y_1[j, y_geom]))
    }
    if (i == 1) {
      output_final <- x_1
    } else {
      output_final <- plyr::rbind.fill(output_final, x_1)
    }
  }
  return(as.data.frame(output_final))
}

get_the_distance_with_lists <- function(x, y, x_id, y_id) {
  # x is the data with the x geometry as the active geom
  # y is the data with the y geometry as the active geom
  for (i in 1:length(x)) {
    x_1 <- x[[i]]
    y_1 <- y[[i]]
    x_2 <- get_one_to_one_distance(x_1,y_1,x_id,y_id)
    if (i == 1) {
      output.final <- x_2
    } else {
      output.final <- plyr::rbind.fill(output.final, x_2)
    }
  }
  return(as.data.frame(output.final))
}


# fix fiddle yearlaid -----------------------------------------------------------

fiddle <- fiddle %>%
  dplyr::rename(YEARLAI = age) %>%
  left_join(year_lookup[,c("res_buil_1", "MEDIAN_YEARLAID")], by = c("YEARLAI" = "res_buil_1")) %>% 
  select(-YEARLAI) %>% 
  dplyr::rename(YEARLAI = MEDIAN_YEARLAID) %>% 
  st_sf() %>% st_set_crs(27700)

fiddle_mode_year <- fiddle %>% 
  as.data.frame() %>%
  group_by(propgroup) %>%
  summarise(YEARLAI = get_mode(YEARLAI))

# load s24 buildings polygons ----------------------------------------------------

outst_s24_poly$area <- st_area(outst_s24_poly)

outst_s24_poly_2_pre <- outst_s24_poly %>% 
  as.data.frame() %>% 
  select(-geometry) %>%
  mutate(area = as.numeric(area)) %>% 
  group_by(propgrp) %>%
  summarise(n = n(),
            tot_area = sum(area),
            mean_area = mean(area),
            min_area = min(area),
            max_area = max(area)) %>% 
  arrange(desc(n))

# subset the s24 to go through based on toms queries
upper_outer_bound_p <- 186
outst_s24_poly_2 <- outst_s24_poly_2_pre %>%
  dplyr::rename(propgroup = propgrp) %>% 
  left_join(fiddle_mode_year, by = "propgroup") %>% 
  filter(mean_area <= upper_outer_bound_p & n > 1 & YEARLAI <= 1937) %>%
  # filter(mean_area <= upper_outer_bound_p) %>% 
  unique()

# s24s to add to pdas
outst_s24_to_pdas <- outst_s24_poly_2_pre %>%
  dplyr::rename(propgroup = propgrp) %>% 
  left_join(fiddle_mode_year, by = "propgroup") %>% 
  filter(mean_area <= upper_outer_bound_p & YEARLAI > 1937) %>%
  # filter(mean_area <= upper_outer_bound_p) %>% 
  unique()


# load pdas buildings polygons -----------------------------------------------------

outst_pdas_poly$area <- st_area(outst_pdas_poly)

outst_pdas_poly_2 <- outst_pdas_poly %>% 
  as.data.frame() %>% 
  select(-geometry) %>%
  mutate(area = as.numeric(area)) %>% 
  group_by(propgrp) %>%
  summarise(n = n(),
            tot_area = sum(area),
            mean_area = mean(area),
            min_area = min(area),
            max_area = max(area)) %>% 
  arrange(desc(n))

# subset the pdas to go through based on toms queries
upper_outer_bound_p <- 186
outst_pdas_poly_2 <- outst_pdas_poly_2 %>%
  # filter(mean_area <= upper_outer_bound_p & n == 1) %>% 
  filter(mean_area <= upper_outer_bound_p) %>% 
  dplyr::rename(propgroup = propgrp) %>% 
  left_join(fiddle_mode_year, by = "propgroup") %>% 
  unique()
# add the s24s that are pdas
outst_pdas_poly_2 <- rbind(outst_s24_to_pdas,outst_pdas_poly_2)

# add props for pdas and s24 ---------------------------------------------
# read the final assetbase and do some descriptive statistics

new_s24_nogeom <- new_s24 %>% as.data.frame()
new_pdas_nogeom <- new_pdas %>% as.data.frame()
# new_s24_nogeom <- new_s24_nogeom %>% left_join(s24_nogeom[,c("fid","pip_q_d")],
# by = "pip_q_d")


# s24 props -------------------------------------------------------------------------------
fiddle_s24_1 <- remove_anomalies_cat_pred(outst_s24_poly_2,as.data.frame(fiddle), "propgroup") %>% 
  st_sf()

# save the centroid of fiddle as centroid feature
# make centroid the active geom
fiddle_s24_1$centroid <- fiddle_s24_1$geometry
st_geometry(fiddle_s24_1) <- "centroid"
fiddle_2 <- st_centroid(fiddle_s24_1)

# grab the uid and join back to the 
fiddle_2$uid <- 1:nrow(fiddle_2)

tic()
fiddle_3 <- get_nearest_feature_column(fiddle_2[,c("uid","centroid")], new_s24, "pip_q_d")
toc()

fiddle_s24 <- fiddle_3 %>% 
  st_drop_geometry() %>% 
  left_join(as.data.frame(fiddle_2[,c("geometry","propgroup","fid","P_Type","uid")]), by = "uid") %>% 
  left_join(new_s24_nogeom[, c("MATERIA",
                               "DIAMETE",
                               "YEARLAI",
                               "AT_YR_C",
                               "pip_q_d",
                               "County",
                               "length",
                               "geometry")], by = c("pip_q_d")) %>% 
  select(-c(centroid, uid)) %>% 
  select(fid, propgroup, everything()) %>% 
  st_sf()
fiddle_s24 %>% summary()

fiddle_s24 <- fiddle_s24[!duplicated(fiddle_s24$fid), ]

fiddle_s24 %>% st_write("D:/STW PDAS/Phase 2 datasets/Flag overlap/s24_properties_13032021.shp")

# save as rds to use later
saveRDS(fiddle_s24,"fiddle_s24.rds")

# pdas ------------------------------------------------------------------------

# fiddle_pdas_1 <- remove_anomalies_cat_pred(propgrp_pdas_fin_2,as.data.frame(fiddle), "propgroup") %>% 
#   st_sf()
fiddle_pdas_1 <- remove_anomalies_cat_pred(outst_pdas_poly_2,as.data.frame(fiddle), "propgroup") %>% 
  st_sf()

# save the centroid of fiddle as centroid feature
# make centroid the active geom
fiddle_pdas_1$centroid <- fiddle_pdas_1$geometry
st_geometry(fiddle_pdas_1) <- "centroid"
fiddle_2 <- st_centroid(fiddle_pdas_1)

# grab the uid and join back to the 
fiddle_2$uid <- 1:nrow(fiddle_2)

tic()
fiddle_3 <- get_nearest_feature_column(fiddle_2[,c("uid","centroid")], new_pdas, "pip_q_d")
toc()

fiddle_pdas <- fiddle_3 %>% 
  st_drop_geometry() %>% 
  left_join(as.data.frame(fiddle_2[,c("geometry","propgroup","fid","P_Type","uid")]), by = "uid") %>% 
  left_join(new_pdas_nogeom[, c("MATERIA",
                                "DIAMETE",
                                "YEARLAI",
                                "AT_YR_C",
                                "pip_q_d",
                                "County",
                                "length",
                                "geometry")], by = c("pip_q_d")) %>% 
  select(-c(centroid, uid)) %>% 
  select(fid, propgroup, everything()) %>% 
  st_sf()

# check for dups
fiddle_pdas <- fiddle_pdas[!duplicated(fiddle_pdas$fid), ]

# write your pdas polygon results
fiddle_pdas %>% st_write("D:/STW PDAS/Phase 2 datasets/Flag overlap/pdas_properties_13032021.shp")

# save as rds to use later
saveRDS(fiddle_pdas,"fiddle_pdas.rds")
# toms 2 additions
# We should also have a cut off where if there isn't anything within a certain distance then we still include the property but have "unknown" for the attributes. I don't know what that distance should be - what do you think? Do you think this approach would be possible?
#  I think it's also worth us having a field called something like "Transfer" with options "PDaS" or "S24". Would it be possible to add that in please?


# add county ---------------------------------------------------------------------

#### save file as chunks ----------------------------------------------------------
save_file_chunks <- function(x, splits = 4,my_func = st_write ,file_location) {
  
  # x is the shapefile to be split
  # splits is the number of WHOLE NUMBER splits/ number of lists
  # file_location is the folder location
  
  # create the list of repeated numbers
  my_split <- base::rep(1:splits, each = 1, length = nrow(x))
  
  # add the list to the dataframe/object
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  
  # split the dataframe/object based on this list
  list_lists <- base::split(my_split2, my_split2$my_split)
  
  # save each list of list seperately
  for (i in 1:length(list_lists)){
    my_func(list_lists[[i]] %>% dplyr::select(-my_split),paste0(file_location,"chunks_",i,".shp"))
  }
}


save_file_chunks(new_pdas,5,st_write,file_location = "D:/STW PDAS/Phase 2 datasets/Flag overlap/03_03_2021_chunks/final_pdas_")
save_file_chunks(new_s24,5,st_write,file_location = "D:/STW PDAS/Phase 2 datasets/Flag overlap/03_03_2021_chunks/final_s24_")

save_file_chunks(fiddle_pdas,5,st_write,file_location = "D:/STW PDAS/Phase 2 datasets/Flag overlap/03_03_2021_chunks/pdas_props_")
save_file_chunks(fiddle_s24,5,st_write,file_location = "D:/STW PDAS/Phase 2 datasets/Flag overlap/03_03_2021_chunks/s24_props_")

# test the above
nrow(list_lists[[1]]) + nrow(list_lists[[2]]) + nrow(list_lists[[3]]) + nrow(list_lists[[4]]) 
nrow(x)

# chunk by counties
# save each list of list seperately
# pdas
fiddle_pdas <- fiddle_pdas %>%
  mutate(
    County = ifelse(
      County == "Worcestershire/Gloucestershire",
      "Worcestershire_Gloucestershire",
      as.character(County)
    )
  )
fiddle_pdas_chunk <- split(fiddle_pdas, fiddle_pdas$County)
for (i in 1:length(fiddle_pdas_chunk)) {
  st_write(
    fiddle_pdas_chunk[[i]],
    paste0(
      "D:/STW PDAS/Phase 2 datasets/Flag overlap/03_03_2021_chunks/",
      "pdas_props_chunk_",
      as.data.frame(fiddle_pdas_chunk[[i]][1, "County"])[[1]],
      ".shp"
    )
  )
}

# s24
fiddle_s24 <- fiddle_s24 %>%
  mutate(
    County = ifelse(
      County == "Worcestershire/Gloucestershire",
      "Worcestershire_Gloucestershire",
      as.character(County)
    )
  )
fiddle_s24_chunk <- split(fiddle_s24, fiddle_s24$County)
for (i in 1:length(fiddle_s24_chunk)) {
  st_write(
    fiddle_s24_chunk[[i]],
    paste0(
      "D:/STW PDAS/Phase 2 datasets/Flag overlap/03_03_2021_chunks/",
      "s24_props_chunk_",
      as.data.frame(fiddle_s24_chunk[[i]][1, "County"])[[1]],
      ".shp"
    )
  )
}

