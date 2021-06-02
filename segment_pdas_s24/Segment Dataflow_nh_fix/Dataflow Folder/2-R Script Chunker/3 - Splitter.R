# Clear environment ------------------------------------------------------------
env_keep <- NULL
rm(list = setdiff(ls(), env_keep))

# Packages ----------------------------------------------------------------

library(plyr)
library(dplyr)
library(magrittr)
library(sf)
library(sp)
library(caret)
library(data.table)
library(rgdal)
library(tmap)
library(tictoc)
# library(utils)

# Parallelisation
library(parallel)
library(doParallel)
library(foreach)
library(tictoc)



# Script configuration settings ------------------------------------------------
parallel <- TRUE
dataflow <- FALSE

if (dataflow == TRUE) {
  
  # Dataflow TaskServices|R.Execute (1.0) file header ----------------------------
  fold_wd       <- getwd()
  fold_in       <- file.path(fold_wd, 'Input') 
  fold_out      <- file.path(fold_wd, 'Output')
  # Input data
  # fold_in_build  <- file.path(fold_in, 'ShapeBuildings')
  fold_in_pipes  <- file.path(fold_in, 'ShapePipes')
  fold_in_source <- file.path(fold_in, 'SourceScripts')
  # Output data
  fold_out_pipes <- file.path(fold_out, 'ShapeNewPipes')
  # Create output folder structure on TEs local disk
  if(!dir.exists(fold_out_pipes)){
    dir.create(fold_out_pipes)
  }
} else if(dataflow == FALSE) {
  
  # Non-Dataflow ------------------------------------------------------------
  # Input data
  
  #fold_in_build <- file.path("C:\\PDaS\\Dataset\\WarwickTest\\warwickshire_props_and_sewers\\props")
  # fold_in_build <- file.path("C:\\PDaS\\Dataset\\fids_pipe_drawing_groups_shp")
  #fold_in_build <- file.path("C:\\PDaS_Phase2\\Dataflow Execution\\Dataflow Execution Folder\\1-ShapeBuildings")
  
  #fold_in_pipes  <- file.path("C:\\PDaS\\Dataset\\coventry_sewers_and_buildings\\sewer")
  # fold_in_pipes  <- file.path("C:/Users/TOsosanya/Downloads/Segment Dataflow_nh_fix/Pipes")
  fold_in_pipes  <- file.path("D:/STW PDAS/Git/pdas_mapping/segment_pdas_s24/Segment Dataflow_nh_fix/Pipes")
  
  
  fold_in_source <- file.path("D:/STW PDAS/Git/pdas_mapping/segment_pdas_s24/Segment Dataflow_nh_fix/Dataflow Folder/3-R Script Source")
  #fold_in_source <- file.path("C://PDaS_Phase2//Dataflow Execution//Dataflow Execution Folder//4-SourceScripts")
  
  # Output data
  #fold_out_pipes <- file.path("C://PDaS_Phase2//output")
  fold_out_pipes <- file.path("D:/STW PDAS/Git/pdas_mapping/segment_pdas_s24/Segment Dataflow_nh_fix/fold_out")
  
  
  # Create output folder structure on TEs local disk
  if(!dir.exists(fold_out_pipes)){
    dir.create(fold_out_pipes)
  }
}


# Source user functions from secondary script(s) -------------------------------
file_in_source <- list.files(path = fold_in_source, pattern = ".[r|R]", full.names = TRUE)
for(i in file_in_source){
  source(i)
}


# User functions ---------------------------------------------------------------
validate_shape <- function(path){
  # Tests for mandatory shape files in a given folder given the ".shp" file name
  
  # An Esri Shape file is actualy a set of several files, three of which are mandatory:
  #
  #   1. Shape File  - (shp) - Contains the geometry features themselves
  #   2. Shape Index - (shx) - Positional index of the geometry objects for quick searching
  #   3. Attributes  - (dbf) - table of attribute data for the geometry objects
  #   
  #   There are up to ten other files providing secondary information, such as the
  #   geometry projection (prj), that could be present. This function only tests
  #   for the mandatory files.
  #
  # args
  #   path   A character scalar. The full path of the Shape File (shp) to validate
  #
  # returns
  #   A logical scalar. Does the folder contain the mandatory shape files?
  #
  mandatory  <- c(".shp", ".shx", ".dbf")
  
  fold_shape <- dirname(path)
  file_shape <- tools::file_path_sans_ext(basename(path))
  file_shape <- paste0(file_shape, mandatory)
  path_shape <- file.path(fold_shape, file_shape)
  
  all(sapply(path_shape, file.exists))
}

# Input file setup -------------------------------------------------------------
file_in_pipes <- list.files(path = fold_in_pipes, pattern = ".shp", full.names = TRUE)
if(length(file_in_pipes) != 1){
  warning("Multiple shape files exist for Port 'ShapePipes'. Only the first of the ",
          length(file_in_pipes),
          " detected files will be used")
  file_in_pipes <- file_in_pipes[1]
}
if(!validate_shape(file_in_pipes)){
  stop("Specified shape file for 'ShapePipes' does not contain all mandatory files.")
}


# FOR pipe  ----------------------------------------------------------------------------------------------

# load data
pipe_assetbase0 <- st_read(file_in_pipes)

# IMPORTANT: upper threshold for s24, change for pdas
upper_thres <- 17.5
# rules
# 5.5,17.5 pdas
# 9.9,27.9 s24

# determine number of cores to use
ncore <- 3
cl    <- makeCluster(ncore) 
registerDoParallel(cl, cores = ncore)



# functions
split_my_linestrings <- function(x,meters, messaging = 1000){
  x$id_0 <- seq.int(nrow(x))
  for (i in 1:nrow(x)) {
    # message detailing progress
    if(i %% messaging == 0){message(paste0("row number ",i))}
    
    # convert to spatial feature
    x1_sp <- as_Spatial(x$geometry[i])
    
    # split it
    x1_splitted <- splitLines(x1_sp, meters)
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
split_my_linestrings_2 <- function(x, meter_column, messaging = 1000){
  x <- x %>% as.data.frame() %>% dplyr::mutate(splits_no = length_m/splits_in_meters)
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
    x1_splitted <- splitLines(x1_sp, row_meter)
    proj4string(x1_splitted) <- CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84")
    x1_splitted_sf <- st_as_sf(as(x1_splitted, "SpatialLines"))
    x1_splitted_sf$pipe_id <- x$pipe_id[i]
    x1_splitted_sf$id_0 <- x$id_0[i]
    
    if(i == 1) {
      out <- x1_splitted_sf
    } else{
      out <- rbind(out,x1_splitted_sf)
    }
  }
  out <- out %>% 
    st_sf() %>% st_set_crs(27700) %>%
    dplyr::mutate(new_length = st_length(.), new_length = as.numeric(new_length)) %>% 
    filter(new_length > 0.01)
  
  return(as.data.frame(out))
}
split_my_linestrings_3 <- function(x, meter_column, messaging = 1000){
  x <- x %>% as.data.frame() %>% dplyr::mutate(splits_no = length_m/splits_in_meters)
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
    x1_splitted <- splitLines(x1_sp, row_meter)
    proj4string(x1_splitted) <- CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84")
    x1_splitted_sf <- st_as_sf(as(x1_splitted, "SpatialLines"))
    x1_splitted_sf$pipe_id <- x$pipe_id[i]
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
    st_sf() %>% st_set_crs(27700) %>%
    dplyr::mutate(new_length = st_length(.), new_length = as.numeric(new_length)) %>% 
    filter(new_length > 0.01)
  
  # write the result of each to a shp file in the given location
  out %>%
    st_write(paste0(fold_out, "\\segmented_lines-",my_time,".shp"), update = TRUE)
  
  return(as.data.frame(out))
}
divide_and_conquer <- function(x,your_func = your_func, func_arg = func_arg, splits = 100,each = 1){
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  my_split3 <- tidyr::nest(my_split2, - my_split) 
  models <- purrr::map(my_split3$data, ~ your_func(., func_arg)) 
  bind_lists <- plyr::rbind.fill(models)
  return(bind_lists)
}
divide_and_conquer_2 <- function(x,your_func = your_func, splits = 1000,each = 1){
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  my_split3 <- tidyr::nest(my_split2, - my_split) 
  models <- purrr::map(my_split3$data, ~ your_func(.)) 
  bind_lists <- plyr::rbind.fill(models)
  return(bind_lists)
}
split_lines_using_rules <- function(x,pipe_quartile_1 = 10,pipe_quartile_3 = upper_thres){
  # get the number of times to split the lines by
  st_geometry(x) <- NULL
  
  pipe_quartile_1 <- pipe_quartile_1
  pipe_quartile_3 <- pipe_quartile_3
  for (i in 1:nrow(x)){
    # message for progress
    if(i/(nrow(x)) %% 10 == 0){message(paste0((i/x)*100,"% at ", Sys.time()))}
    
    my_length = x[i,"length_m"]
    if(my_length > pipe_quartile_3){
      my_mulitiplier = floor(my_length/pipe_quartile_1)
      no_of_splits = my_mulitiplier
      x[i,"splits_in_meters"] <- my_length/no_of_splits
    }
    else{
      x[i,"splits_in_meters"] <- my_length
    }
  }
  return(x)
}
split_lines_using_rules_2 <- function(x,pipe_quartile_1 = 10,pipe_quartile_3 = upper_thres){
  # get the number of times to split the lines by
  st_geometry(x) <- NULL
  
  pipe_quartile_1 <- pipe_quartile_1
  pipe_quartile_3 <- pipe_quartile_3
  for (i in 1:nrow(x)){
    # message detailing progress
    #if(i %% messaging == 0){message(paste0("row number ",i))}
    #j = i/nrow(x)
    if(i %% (nrow(x)/10) == 0){message(paste0((i/nrow(x))*100),"% at ", Sys.time())}
    
    my_length = x[i,"length_m"]
    if(my_length > pipe_quartile_3){
      my_mulitiplier = ceiling(my_length/pipe_quartile_3)
      no_of_splits = my_mulitiplier
      x[i,"splits_in_meters"] <- my_length/no_of_splits
    }
    else{
      x[i,"splits_in_meters"] <- my_length
    }
  }
  return(x)
}
break_lines <- function(x_newpoint){
  x_points1 <- st_cast(st_geometry(x_newpoint), "POINT") 
  n <- length(x_points1) - 1
  x <- lapply(X = 1:n, FUN = function(p) {
    
    x_pair <- st_combine(c(x_points1[p], x_points1[p + 1]))
    x <- st_cast(x_pair, "LINESTRING")
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
  x2 <- x2 %>% dplyr::mutate(length_m = round(as.numeric(length_m),4))
  x2$pipe_id <- x_newpoint$pipe_id
  x2$pipe_id_ls <- x_newpoint$pipe_id_ls
  return(x2)
}
break_to_smallest <- function(broken_geom_2,messaging = 100) {
  for (i in 1:nrow(broken_geom_2)) {
    # message for progress
    #if(i %% (nrow(broken_geom_2)/100) == 0){message(paste0((i/nrow(broken_geom_2))*100),"% at ", Sys.time())}
    #if(i %% (nrow(broken_geom_2)/10) == 0){message(paste0((i/nrow(broken_geom_2))*100),"% at ", Sys.time())}
    if(i %% messaging == 0){message(paste0("row number ",i))}
    message(paste0("row number ",i))
    if (i == 1) {
      broken_geom_3 <- break_lines(broken_geom_2[i,])
    } else{
      broken_geom_3 <- plyr::rbind.fill(break_lines(broken_geom_2[i,]), broken_geom_3)
    }
  }
  broken_geom_3
}
create_lists <- function(x, splits = 29,each = 1){
  my_split <- base::rep(1:splits,each = each, length = nrow(x))
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  list_lists <- base::split(my_split2,my_split2$my_split) 
  #list_lists <- purrr::map(my_split3$data, ~ split(., func_arg)) 
  return(list_lists)
}
fix_xymax <- function(x, messaging = 1000){
  x$index = 1:nrow(x)
  for (i in x$index) {
    # message detailing progress
    if(i %% messaging == 0){message(paste0("row number ",i))}
    x$geometry[[i]][[1]][4] <- ifelse(round(as.numeric(x[["geometry"]][[i]][[1]][3]),1)== round(as.numeric(x[["geometry"]][[i]][[1]][4]),1),
                                      as.numeric(x[["geometry"]][[i]][[1]][4]) + 0.1, x[["geometry"]][[i]][[1]][4])
    x$geometry[[i]][[1]][2] <- ifelse(round(as.numeric(x[["geometry"]][[i]][[1]][1]),1) == round(as.numeric(x[["geometry"]][[i]][[1]][2]),1), 
                                      as.numeric(x[["geometry"]][[i]][[1]][2]) + 0.1, x[["geometry"]][[i]][[1]][2])
  }
  return(x)
}


# break up the multi-linestrings and then cast to linestring ------------------------------
my_pipe <- pipe_assetbase0
my_pipe_lines <- my_pipe %>% 
  dplyr::mutate(pipe_id = dplyr::row_number()) %>% 
  select(pipe_id,geometry) %>% 
  # prep for linestring
  group_by(pipe_id) %>% st_cast("LINESTRING") %>% 
  as.data.frame() %>% st_sf() %>% 
  dplyr::mutate(pipe_id_ls = row_number()) %>% 
  as.data.frame()

my_newpoint <- my_pipe_lines %>% st_sf() %>% st_set_crs(27700)


# break the linestrings further into multipoints and then cast to linestrings -------------------------
my_newpoint_list <- create_lists(my_newpoint)

tic()
my_newlines <- foreach(df_list = iter(my_newpoint_list),
                       .packages = c('dplyr', 'sp', 'sf','plyr'), .errorhandling='stop') %dopar%
  break_to_smallest(broken_geom_2 = df_list)
my_newlines <- plyr::rbind.fill(my_newlines)
my_newlines <- my_newlines[!duplicated(my_newlines$geometry),]
toc()



# calculate the number of splits each linestring needs -----------------------------
my_newlines1 <- my_newlines %>% dplyr::mutate(pipe_id_ls_remove = row_number()) %>% st_sf()
my_newlines_collated <- divide_and_conquer(my_newlines1,st_cast,"MULTILINESTRING",100)

my_newlines2 <- my_newlines_collated %>% st_sf() %>% st_set_crs(27700) %>% 
  dplyr::mutate(pipe_id_ls_2 = row_number())
tic()
my_newlines2_list <- create_lists(my_newlines2)
new_pipe <- foreach(df_list = iter(my_newlines2_list),
                    .packages = c('dplyr', 'sp', 'sf','plyr'), .errorhandling='stop') %dopar%
  split_lines_using_rules_2(df_list)
new_pipe <- plyr::rbind.fill(new_pipe)
toc()

new_pipe_2 <- new_pipe %>% 
  dplyr::mutate(pipe_id_ls_2 = row_number()) %>%
  left_join(my_newlines2[,"pipe_id_ls_remove"], by = "pipe_id_ls_remove")


# split by the specified meters, first get unique id and recalculate length--------------------------------------------------------------
new_pipe_3 <- new_pipe_2 %>% 
  as.data.frame() %>% 
  dplyr::mutate(pipe_id_ls_2 = row_number()) %>% 
  st_sf() %>% st_set_crs(27700)
new_pipe_3$length_m <- st_length(new_pipe_3$geometry)

# convert the units to 5d.p numeric
new_pipe_3 <- new_pipe_3 %>% dplyr::mutate(length_m = round(as.numeric(length_m),5))

# fix ymax in the below with length > upper_thres
new_pipe_3_upper_thres <- new_pipe_3 %>% filter(length_m > upper_thres)
# fixed_xymax <- fix_xymax(new_pipe_3_upper_thres)
tic()
fixed_xymax_list <- create_lists(new_pipe_3_upper_thres)
fixed_xymax <- foreach(df_list = iter(fixed_xymax_list),
                       .packages = c('dplyr', 'sp', 'sf'), .errorhandling='stop') %dopar%
  divide_and_conquer_2(df_list,fix_xymax)
fixed_xymax <- plyr::rbind.fill(fixed_xymax)
toc()

# split the line strings using the number of splits in meters -------------------------->
tic()
new_pipe_3_list <- create_lists(fixed_xymax, splits = 1000)
something <- foreach(df_list = iter(new_pipe_3_list),
                     .packages = c('dplyr', 'sp', 'sf'), .errorhandling='stop') %dopar%
  
  split_my_linestrings_2(x = df_list,
                         meter_column = "splits_in_meters",
                         messaging = 1)
toc()


# append those less than Q3
new_pipe_3_0_upper_thres <- new_pipe_3 %>% filter(length_m >= 0,length_m <= upper_thres) %>% 
  st_sf() %>% st_set_crs(27700) %>% 
  dplyr::rename(new_length = length_m) %>% 
  select(pipe_id, geometry, new_length)

# select columns to append to result from the input dataset ---------------------------->
atts <- my_pipe %>% dplyr::mutate(pipe_id = row_number()) %>% as.data.frame() %>% select(-geometry)
something2 <- plyr::rbind.fill(something) %>% select(-id_0) %>% st_sf() %>% st_set_crs(27700)

# add atts column and write the result
rbind(something2,new_pipe_3_0_upper_thres) %>%
  left_join(atts, by = "pipe_id") %>% 
  st_write(paste0(fold_out_pipes,"/merged_linestrings_pdas.shp"))


# the end 

