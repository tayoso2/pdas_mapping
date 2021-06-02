# Initialise environment -------------------------------------------------------
rm(list = ls())


# Packages ---------------------------------------------------------------------
library(sf)
library(raster)
library(lwgeom)
library(tmap)
library(pbapply)
library(dplyr)


# go to the current working directory
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# User functions ---------------------------------------------------------------
source("functions.R")
source("4 - Subsetter.R")


setwd("D:/STW PDAS/Phase 2 datasets/STW_Area/")

# Folder structure -------------------------------------------------------------
fold_raster <- "D:/STW PDAS/Phase 2 datasets/STW_Area/"
fold_pipes     <- "D:/STW PDAS/Phase 2 datasets/segmented_road_proxies_distances_FCS/"# formerly segmented_road_proxies/


# File filters -----------------------------------------------------------------
file_raster    <- list.files(fold_raster, ".asc", full.names = TRUE)
file_pipes     <- list.files(fold_pipes, ".shp", full.names = TRUE)


# Import pipe data -------------------------------------------------------------
if(!validate_shape(file_pipes)){
  stop("Specified shape file for 'file_pipes' does not contain all mandatory files.")
}

data_pipes <- file_pipes %>% 
  sf::st_read(stringsAsFactors = FALSE) %>% 
  dplyr::mutate(read_order = 1:n())


# Import raster images (asc) ---------------------------------------------------
list_raster <- pbapply::pblapply(file_raster, function(i){
  i %>% 
    raster::raster() %>% 
    bbox_to_poly() %>% 
    sf::st_as_sf() %>% 
    dplyr::rename(geometry = x) %>%
    sf::st_set_crs(27700) %>% 
    dplyr::mutate(filepath = i)
})
data_raster <- base::do.call(base::rbind, list_raster)

# Coverage test
tmap::tmap_mode("view")
tmap::qtm(data_raster, fill = "blue")


# Testing 'polygon contains point' methodology ---------------------------------
if(FALSE){
  #   Create POINT objects that are contained by raster polygons for testing
  mat_point <- base::matrix(c(415020, 305020,   # Contained by raster_1m row 1
                              416020, 304020,   # Contained by raster_1m row 3
                              416020, 306020,   # Contained by raster_1m row 5
                              416050, 306050),  # Contained by raster_1m row 5
                            ncol = 2, byrow = TRUE)
  
  list_points <- base::lapply(1:nrow(mat_point), function(i){
    xy <- mat_point[i, ]
    pt <- make_point(xy)
  })
  
  data_points <- base::do.call(rbind, list_points) %>% 
    dplyr::mutate(Tag = paste0(10, 1:n()),
                  id_0 = 1:n()) %>% 
    dplyr::select(Tag, id_0, geometry)
  
  data_points <- data_points %>% 
    dplyr::rowwise() %>% 
    dplyr::mutate(raster_id = poly_contains_point(geometry, data_raster)) %>% 
    dplyr::ungroup()
}


# # TEST # select the pipes you want the gradient gotten for
# data_pipes_raster <- data_pipes %>% filter(id > 70000, id < 80000)


# Assign raster image to each pipe ---------------------------------------------
data_test <- NA #nrow(data_pipes)                  # For all pipes set 'data_test' to 'NA'
if(is.na(data_test)){
  data_pipes_raster <- data_pipes
}else{
  data_pipes_raster <- data_pipes[base::sample(1:base::nrow(data_pipes), data_test), ]
}

# Get start and end points for the pipes
data_pipes_raster <- data_pipes_raster %>%
  dplyr::mutate(start_point = lwgeom::st_startpoint(geometry),
                end_point = lwgeom::st_endpoint(geometry))

# Old 'polygon contains point' JB methodology (deprecated - slow)
# data_pipes_raster <- data_pipes_raster %>%
#   dplyr::rowwise() %>% 
#   dplyr::mutate(start_point_raster = poly_contains_point(start_point, data_raster)
#                 end_point_raster = poly_contains_point(end_point, data_raster)) %>% 
#   dplyr::ungroup()

# New 'join by geometry' NH methodology
data_pipes_raster <- data_pipes_raster %>% 
  # Start point
  st_set_geometry(data_pipes_raster$start_point) %>% 
  st_join(data_raster %>%
            st_set_crs(st_crs(data_pipes_raster))) %>% 
  rename(start_point_raster = filepath) %>% 
  # End point
  st_set_geometry(data_pipes_raster$end_point) %>% 
  st_join(data_raster %>% st_set_crs(st_crs(data_pipes_raster))) %>% 
  rename(end_point_raster = filepath) %>% 
  # Reset geometry
  st_set_geometry(data_pipes_raster$geometry)

# Test for start and end point rasters being the same file
# TODO: Make use of this information to streamline the repeated sections below
data_pipes_raster <- data_pipes_raster %>% 
  mutate(end_equals_start = start_point_raster == end_point_raster)


# Determine pipe start point depths --------------------------------------------
file_raster_req_s <- data_pipes_raster %>% 
  dplyr::pull(start_point_raster) %>% 
  base::unique() %>% 
  base::sort(na.last = TRUE)

list_pipes_raster <- base::list()
for(i in file_raster_req_s){
  # If raster could not be found skip the spacial operations
  if(base::is.na(i)){
    data_pipes_i  <- data_pipes_raster %>% 
      dplyr::filter(is.na(start_point_raster)) %>% 
      dplyr::mutate(start_point_depth = NA_real_)
    
    list_pipes_raster[["NA"]] <- data_pipes_i
    next
  }
  
  # Load raster and subset pipes 
  data_raster_i <- raster::raster(i)
  data_pipes_i  <- data_pipes_raster %>% 
    dplyr::filter(start_point_raster == i)
  
  # Get pipe end point depth
  # TODO: This operation generates warnings about the CRS being lost:
  #       "In proj4string(x) : CRS object has comment, which is lost in output"
  #       Need to determine the cause/impact and revise code to prevent warnings
  #       from being generated
  data_depth_i  <- data_raster_i %>% 
    raster::extract(data_pipes_i %>% 
                      dplyr::pull(start_point) %>% 
                      # raster::extract does not appear to support 'sfc_POINT' geometries
                      sf::st_as_sf())
  
  data_pipes_i  <- data_pipes_i %>% 
    dplyr::mutate(start_point_depth = data_depth_i)
  
  # Add pipes to list
  j <- i %>% 
    base::basename() %>% 
    tools::file_path_sans_ext()
  
  list_pipes_raster[[j]] <- data_pipes_i
}
data_pipes_raster <- base::do.call(base::rbind, list_pipes_raster)


# Determine pipe end point depths ----------------------------------------------
# TODO: It is inefficient to repeat the methodology for start and end points when
#       many (most?) of the lines have both points contained by the same raster
file_raster_req_e <- data_pipes_raster %>% 
  dplyr::pull(end_point_raster) %>% 
  base::unique() %>% 
  base::sort(na.last = TRUE)

list_pipes_depth <- base::list()
for(i in file_raster_req_e){
  # If raster could not be found skip the spacial operations
  if(base::is.na(i)){
    data_pipes_i  <- data_pipes_raster %>% 
      dplyr::filter(is.na(end_point_raster)) %>% 
      dplyr::mutate(end_point_depth = NA_real_)
    
    list_pipes_raster[["NA"]] <- data_pipes_i
    next
  }
  
  # Load raster and subset pipes 
  data_raster_i <- raster::raster(i)
  data_pipes_i  <- data_pipes_raster %>% 
    dplyr::filter(end_point_raster == i)
  
  # Get pipe end point depth
  data_depth_i  <- data_raster_i %>% 
    raster::extract(data_pipes_i %>% 
                      dplyr::pull(end_point) %>% 
                      # raster::extract does not appear to support 'sfc_POINT' geometries
                      sf::st_as_sf())
  
  data_pipes_i  <- data_pipes_i %>% 
    dplyr::mutate(end_point_depth = data_depth_i)
  
  # Add pipes to list
  j <- i %>% 
    base::basename() %>% 
    tools::file_path_sans_ext()
  
  list_pipes_raster[[j]] <- data_pipes_i
}
data_pipes_raster <- base::do.call(plyr::rbind.fill, list_pipes_raster)


# Calculate pipe gradient ------------------------------------------------------
# TODO: Calculate gradient of pipe in addition to change in depth
#       Positive values of 'delta_depth' indicate the start of the pipe is above
#       the end of the pipe so flow is assumed to be 'start to end'
data_pipes_raster_2 <- data_pipes_raster %>% 
  dplyr::arrange(read_order) %>% 
  dplyr::mutate(delta_depth = end_point_depth - start_point_depth) %>% 
  select(id, read_order,end_equals_start,start_point_depth,geometry,start_point,end_point, end_point_depth, delta_depth) #%>% 
  # dplyr::mutate(end_point = ifelse(delta_depth < 0, start_point,end_point),
  #               start_point = ifelse(delta_depth < 0, start_point,end_point)


saveRDS(data_pipes_raster_2,
        "D:/STW PDAS/Git/pdas_mapping/pipe-depth-from-lidar-images/scripts/data_pipes_raster_2.rds")
        

