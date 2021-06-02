

# Packages ----------------------------------------------------------------

library(dplyr)
library(magrittr)
library(sf)
library(sp)
library(caret)
library(data.table)
library(rgdal)
library(tmap)
library(tictoc)
library(polylineSplitter)

# Parallelisation
library(parallel)
library(doParallel)
library(foreach)
library(tictoc)


# FOR ROADS  ----------------------------------------------------------------------------------------------

# load data
proxy_link <- "D:/STW PDAS/Phase 2 datasets/segmented_roads/"
proxy_assetbase0 <- st_read(paste0(proxy_link, "segmented_roads.shp"))


# functions
split_my_linestrings <- function(x,meters, messaging = 1000){
  x$id_0 <- seq.int(nrow(x))
  for (i in 1:nrow(x)) {
    # message detailing progress
    if(i %% messaging == 0){message(paste0("row number ",i))}
    
    # convert to spatial feature
    x1_sp <- as_Spatial(x$geometry[i])
    
    # split it
    x1_splitted <- polylineSplitter::splitLines(x1_sp, meters)
    proj4string(x1_splitted) <- CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84")
    x1_splitted_sf <- st_as_sf(as(x1_splitted, "SpatialLines"))
    
    # add the unique id column and other columns
    x1_splitted_sf$Tag <- x$Tag[i]
    x1_splitted_sf$id_0 <- x$id_0[i]
    
    if(i == 1) {
      out <- x1_splitted_sf
    } else{
      out <- rbind(out,x1_splitted_sf)
    }
  }
  return(as.data.frame(out))
}
divide_and_conquer <- function(x,your_func = your_func, func_arg = func_arg, splits = 100,each = 1){
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- mutate(x, my_split = my_split)
  my_split3 <- tidyr::nest(my_split2, - my_split) 
  models <- purrr::map(my_split3$data, ~ your_func(., func_arg)) 
  bind_lists <- plyr::rbind.fill(models)
  return(bind_lists)
}
divide_and_conquer_2 <- function(x,your_func = your_func, splits = 1000,each = 1){
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- mutate(x, my_split = my_split)
  my_split3 <- tidyr::nest(my_split2, - my_split) 
  models <- purrr::map(my_split3$data, ~ your_func(.)) 
  bind_lists <- plyr::rbind.fill(models)
  return(bind_lists)
}
split_lines_using_rules_2 <- function(x,s24_quartile_1 = 10,s24_quartile_3 = 28){
  # get the number of times to split the lines by
  st_geometry(x) <- NULL
  
  s24_quartile_1 <- s24_quartile_1
  s24_quartile_3 <- s24_quartile_3
  for (i in 1:nrow(x)){
    # message detailing progress
    #if(i %% messaging == 0){message(paste0("row number ",i))}
    #j = i/nrow(x)
    if(i %% (nrow(x)/10) == 0){message(paste0((i/nrow(x))*100),"% at ", Sys.time())}
    
    my_length = x[i,"length_m"]
    if(my_length > s24_quartile_3){
      my_mulitiplier = ceiling(my_length/s24_quartile_3)
      no_of_splits = my_mulitiplier
      x[i,"splits_in_meters"] <- my_length/no_of_splits
    }
    else{
      x[i,"splits_in_meters"] <- my_length
    }
  }
  return(x)
}
split_lines_using_rules <- function(x,s24_quartile_1 = 10,s24_quartile_3 = 28){
  # get the number of times to split the lines by
  st_geometry(x) <- NULL
  
  s24_quartile_1 <- s24_quartile_1
  s24_quartile_3 <- s24_quartile_3
  for (i in 1:nrow(x)){
    # message for progress
    if(i/(nrow(x)) %% 10 == 0){message(paste0((i/x)*100,"% at ", Sys.time()))}
    
    my_length = x[i,"length_m"]
    if(my_length > s24_quartile_3){
      my_mulitiplier = floor(my_length/s24_quartile_1)
      no_of_splits = my_mulitiplier
      x[i,"splits_in_meters"] <- my_length/no_of_splits
    }
    else{
      x[i,"splits_in_meters"] <- my_length
    }
  }
  return(x)
}
split_my_linestrings_2 <- function(x, meter_column, messaging = 1000){
  x <- x %>% as.data.frame() %>% mutate(splits_no = length_m/splits_in_meters)
  x$id_0 <- seq.int(nrow(x))
  for (i in 1:nrow(x)) {
    # message detailing progress
    if(i %% messaging == 0){message(paste0("row number ",i))}
    
    # sp
    x1_sp <- as_Spatial(x$geometry[i])
    
    # meter selection
    row_meter = x[i,meter_column] #%>% select(-geometry)
    splits_no = x[i, "splits_no"]
    
    # split it
    x1_splitted <- polylineSplitter::splitLines(x1_sp, row_meter)
    proj4string(x1_splitted) <- CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84")
    x1_splitted_sf <- st_as_sf(as(x1_splitted, "SpatialLines"))
    x1_splitted_sf$s24_id <- x$s24_id[i]
    x1_splitted_sf$id_0 <- x$id_0[i]
    # x1_splitted_sf$new_length <- st_length(x1_splitted_sf[i,] %>% st_sf() %>% st_set_crs(27700))
    
    if(i == 1) {
      out <- x1_splitted_sf
    } else{
      out <- rbind(out,x1_splitted_sf)
    }
  }
  out <- out %>% 
    #split_my_linestrings_2("splits_in_meters") %>% 
    st_sf() %>% st_set_crs(27700) %>%
    mutate(new_length = st_length(.), new_length = as.numeric(new_length)) %>% 
    filter(new_length > 0.01)
  
  return(as.data.frame(out))
}
split_my_linestrings_3 <- function(x, meter_column, messaging = 1000){
  x <- x %>% as.data.frame() %>% mutate(splits_no = length_m/splits_in_meters)
  x$id_0 <- seq.int(nrow(x))
  for (i in 1:nrow(x)) {
    # message detailing progress
    if(i %% messaging == 0){message(paste0("row number ",i))}
    
    # sp
    x1_sp <- as_Spatial(x$geometry[i])
    
    # meter selection
    row_meter = x[i,meter_column] #%>% select(-geometry)
    splits_no = x[i, "splits_no"]
    
    # split it
    x1_splitted <- polylineSplitter::splitLines(x1_sp, row_meter)
    proj4string(x1_splitted) <- CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84")
    x1_splitted_sf <- st_as_sf(as(x1_splitted, "SpatialLines"))
    x1_splitted_sf$s24_id <- x$s24_id[i]
    x1_splitted_sf$id_0 <- x$id_0[i]
    # x1_splitted_sf$new_length <- st_length(x1_splitted_sf[i,] %>% st_sf() %>% st_set_crs(27700))
    
    # write the result of each to a shp file in the given location using this as the differentiating suffix
    # my_time <- as.integer(runif(1)*100000000)
    my_time <- x[,"my_split"] %>% as.data.frame() %>% unique()#%>% select(-geometry)
    
    if(i == 1) {
      out <- x1_splitted_sf
    } else{
      out <- rbind(out,x1_splitted_sf)
    }
  }
  out <- out %>% 
    #split_my_linestrings_2("splits_in_meters") %>% 
    st_sf() %>% st_set_crs(27700) %>%
    mutate(new_length = st_length(.), new_length = as.numeric(new_length)) %>% 
    filter(new_length > 0.01)
  
  # write the result of each to a shp file in the given location
  out %>%
    st_write(paste0(fold_out, "\\segmented_lines-",my_time,".shp"), update = TRUE)
  
  return(as.data.frame(out))
}
break_lines <- function(x_newpoint){
  x_points1 <- st_cast(st_geometry(x_newpoint), "POINT") 
  n <- length(x_points1) - 1
  x <- lapply(X = 1:n, FUN = function(p) {
    
    x_pair <- st_combine(c(x_points1[p], x_points1[p + 1]))
    x <- st_cast(x_pair, "LINESTRING")
    #return(line)
  })
  for(i in 1:length(x)){
    if(i == 1){
      x2 <- x[[i]]
    } else{
      x2 <- c(x2,x[[i]])
    }
  }
  x2 <- x2 %>% as.data.frame() %>% st_sf() %>% st_set_crs(27700)
  x2$length_m <- st_length(x2$geometry) 
  x2 <- x2 %>% mutate(length_m = round(as.numeric(length_m),4))
  x2$s24_id <- x_newpoint$s24_id
  x2$s24_id_ls <- x_newpoint$s24_id_ls
  return(x2)
}
break_to_smallest <- function(broken_geom_2) {
  for (i in 1:nrow(broken_geom_2)) {
    # message for progress
    #if(i %% (nrow(broken_geom_2)/100) == 0){message(paste0((i/nrow(broken_geom_2))*100),"% at ", Sys.time())}
    #if(i %% (nrow(broken_geom_2)/10) == 0){message(paste0((i/nrow(broken_geom_2))*100),"% at ", Sys.time())}
    message(paste0("row number ",i))
    if (i == 1) {
      broken_geom_3 <- break_lines(broken_geom_2[i,])
    } else{
      #broken_geom_4 <- break_lines(broken_geom_2[i,])
      broken_geom_3 <- plyr::rbind.fill(break_lines(broken_geom_2[i,]), broken_geom_3)
    }
  }
  broken_geom_3
}
create_lists <- function(x, splits = 29,each = 1){
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- mutate(x, my_split = my_split)
  list_lists <- split(my_split2,my_split2$my_split) 
  #list_lists <- purrr::map(my_split3$data, ~ split(., func_arg)) 
  return(list_lists)
}

fix_xymax <- function(x, messaging = 1){
  for (i in 1:nrow(x)) {
    # message detailing progress
    if(i %% messaging == 0){message(paste0("row number ",i))}
    x[["geometry"]][[i]][[1]][[4]] <- ifelse(abs(x[["geometry"]][[i]][[1]][[3]] - x[["geometry"]][[i]][[1]][[4]]) < 0.1, 
                                             x[["geometry"]][[i]][[1]][[4]] + 0.1,x[["geometry"]][[i]][[1]][[4]])
    x[["geometry"]][[i]][[1]][[2]] <- ifelse(abs(x[["geometry"]][[i]][[1]][[1]] - x[["geometry"]][[i]][[1]][[2]]) < 0.1, 
                                             x[["geometry"]][[i]][[1]][[2]] + 0.1,x[["geometry"]][[i]][[1]][[2]])
  }
  return(x)
}
stdh_cast_substring <- function(x, to = "MULTILINESTRING") {
  
  # https://dieghernan.github.io/201905_Cast-to-subsegments/ 
  
  # split lines into segments
  
  ggg <- st_geometry(x)
  
  if (!unique(st_geometry_type(ggg)) %in% c("POLYGON", "LINESTRING")) {
    stop("Input should be  LINESTRING or POLYGON")
  }
  for (k in 1:length(st_geometry(ggg))) {
    sub <- ggg[k]
    geom <- lapply(
      1:(length(st_coordinates(sub)[, 1]) - 1),
      function(i)
        rbind(
          as.numeric(st_coordinates(sub)[i, 1:2]),
          as.numeric(st_coordinates(sub)[i + 1, 1:2])
        )
    ) %>%
      st_multilinestring() %>%
      st_sfc()
    
    if (k == 1) {
      endgeom <- geom
    }
    else {
      endgeom <- rbind(endgeom, geom)
    }
  }
  endgeom <- endgeom %>% st_sfc(crs = st_crs(x))
  if (class(x)[1] == "sf") {
    endgeom <- st_set_geometry(x, endgeom)
  }
  
  if (to == "LINESTRING") {
    endgeom <- endgeom %>% st_cast("LINESTRING")
  }
  return(endgeom)
}

# split at vertices -----------------------------------------------------------
# break up the multilinestrings and then cast to linestring 
proxy_1 <- proxy_assetbase0
proxy_1 <- proxy_1 %>% 
  mutate(s24_id = row_number()) %>% 
  select(s24_id,geometry) %>% 
  # prep for linestring
  group_by(s24_id) %>% st_cast("LINESTRING") %>% 
  as.data.frame() %>% st_sf() %>% 
  mutate(s24_id_ls = row_number()) %>% 
  as.data.frame()

my_proxy_1 <- proxy_1 %>% st_sf() %>% st_set_crs(27700)


ncore <- 2
cl    <- makeCluster(ncore)
registerDoParallel(cl, cores = ncore)

segment_lines <- function(x){
  x2 <- x %>%
    st_segmentize(dfMaxLength = 12) %>% # testing created median ~10m
    stdh_cast_substring() %>% 
    st_cast("LINESTRING")
  
  x2$id <- rownames(x2)
  rownames(x2) <- 1:nrow(x2)
  
  return(x2)
}

tic()
my_proxy_1_list <- create_lists(my_proxy_1)
something <- foreach(df_list = iter(my_proxy_1_list),
                     .packages = c('dplyr', 'sp', 'sf', 'polylineSplitter'), .errorhandling='stop') %dopar%
  segment_lines(x = df_list)

toc()


something_2 <- plyr::rbind.fill(something) %>% st_sf() %>% st_set_crs(27700) %>% 
  select(-s24_id_ls,-my_split,-id) %>% 
  rename(id = s24_id)
something_2 %>% 
  st_write("D:/segemented_road_proxies.shp")

st_length(something_2) %>% summary()
