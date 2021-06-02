# Clear environment ------------------------------------------------------------
env_keep <- NULL
rm(list = setdiff(ls(), env_keep))

# Packages ---------------------------------------------------------------------
library(tidyverse)
library(sf)
library(tmap)
library(profvis)
library(microbenchmark)
library(lazyeval)
library(pbapply)
#library(matlib)
library(data.table)

# Parallelisation
library(parallel)
library(doParallel)
library(foreach)
library(tictoc)



# source script
#

# Script configuration settings ------------------------------------------------
parallel <- TRUE
dataflow <- TRUE

if (dataflow == TRUE) {
  
  # Dataflow TaskServices|R.Execute (1.0) file header ----------------------------
  fold_wd       <- getwd()
  fold_in       <- file.path(fold_wd, 'Input') 
  fold_out      <- file.path(fold_wd, 'Output')
  # Input data
  fold_in_build  <- file.path(fold_in, 'ShapeBuildings')
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
  fold_in_build <- file.path("C:\\PDaS\\Dataset\\fids_s24_drawing_groups_shp")
  #fold_in_build <- file.path("C:\\PDaS_Phase2\\Dataflow Execution\\Dataflow Execution Folder\\1-ShapeBuildings")
  
  #fold_in_pipes  <- file.path("C:\\PDaS\\Dataset\\coventry_sewers_and_buildings\\sewer")
  fold_in_pipes  <- file.path("C:\\PDaS\\Dataset\\phase_2_sewers_with_bearings")
  #fold_in_pipes  <- file.path("C:\\PDaS_Phase2\\Dataflow Execution\\Dataflow Execution Folder\\2-ShapePipes")
  
  
  fold_in_source <- file.path("C:\\Projects\\sewpigen\\Scripts\\SimpleLineGen\\Source")
  #fold_in_source <- file.path("C:\\PDaS_Phase2\\Dataflow Execution\\Dataflow Execution Folder\\4-SourceScripts")
  
  # Output data
  #fold_out_pipes <- file.path("C:\\PDaS_Phase2\\output")
  fold_out_pipes <- file.path("C:\\PDaS\\output\\stw_region_s24")
  
  
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

combinetogether <- function(justalist) {
  t <- justalist[[1]]
  for (x in justalist[-1]){
    t <- rbind(t, x)
  } 
  t
}





isSameGrid <- function(centroids){
  # Make sure only doing this grid piece to not get any repeats
  centroids <- centroids %>%
    filter(Xband == ActualX & 
             Yband == ActualY)
  
  return(centroids)
  
}

getActualX <- function(centroids){
  ActualX <- as.data.frame(centroids) %>% 
    select(ActualX) %>% 
    slice(1) %>% 
    pull()
  return(ActualX)
}

getActualY <- function(centroids){
  ActualY <- as.data.frame(centroids) %>% 
    select(ActualY) %>% 
    slice(1) %>% 
    pull()
  return(ActualY)
}

doachunk3 <- function(centroids, n24, chunksize = 1, fold_out) {
  
  
  if (is.null(centroids)) {
    return()
  } else if(is.null(n24)) {
    return()
  }
  
  # Only keep the centroids in the same grid
  centroids <- isSameGrid(centroids)
  ActualX <- getActualX(centroids)
  ActualY <- getActualY(centroids)
  
  # # Edit this to be real but tbh do not care.
  # if(file.exists(paste0(fold_out, "/test-", startval, "-", (startval + numberofchunks*chunksize), ".shp"))) {
  #   print(paste0(fold_out, "/test-", startval, "-", (startval + numberofchunks*chunksize), ".shp", " Already exists, skipping this one"))
  #   return()
  # }
  
  # if(startval > dim(shapeinfo[["Centroids"]])[1]) {
  #   print("Index of the first centroid is too large, aborting!")
  #   stop()
  # }
  
  maxi <- ceiling(dim(centroids)[1] / 100)
  
  
  b <- list()
  
  for (i in 1:maxi) {
    if (i == maxi) {
      b[[i]] <- centroids[(1 + (chunksize * (i-1))):dim(centroids)[1], ]
      
    } else {
      b[[i]] <- centroids[(1 + (chunksize * (i-1))):((chunksize * i)), ]
    }
  }
  
  listofresults <- lapply(b, notionalfunction, n24) %>% 
    rbind_list() %>%
    st_as_sf %>% 
    st_set_crs(27700) %>% 
    st_simplify(dTolerance = 0.0000000001)
  
  
  
  # combinedtogether %>% 
  #   st_write(paste0("Testoutput/test-", startval, "-", maxval, ".shp"))
  
  # combinedtogether %>% 
  #   st_write(paste0(fold_out, "/test-", startval, "-", (startval + maxval), ".shp"))
  
  # listofresults %>% 
  #   st_write(paste0(fold_out, "/test-", ActualX, "-", ActualY, ".shp"))
  
  return(listofresults)
  
  
}





# -------------------------------------------------------------------------


# Main function to draw L shape -------------------------------------------


# -------------------------------------------------------------------------

# buildings: building polygon
# n24: (sorry for the bad naming) sewer main 
# type: sewer main type, the valid values are:
# C: combined sewer (default or invalid value)
# F: foul sewer
# S: surface water 
# position: pipe goes behind/infront the property group, the valid values are:
# back: go behind (default or invalid value)
# infront: go infront (Surface water)
# distance: the buffer distance of boundary of property group

drawLShapebyGroup <- function(buildings, n24, distance = 1, fold_out){
  # create a new empty geometry object 
  L_shapes <- st_set_crs(st_sf(st_sfc()), 27700)
  
  # fill in "F" to sewers without Tp_purpose
  n24[is.na(n24$Tp_purpose), "Tp_purpose"] <- "F"
  
  # which chunk
  ActualX <- getActualX(buildings)
  ActualY <- getActualY(buildings)
  
  # for test purpose
  #print(paste("Current X:", ActualX, " |  Current Y:", ActualY))
  
  # iterate through drawing groups in current chunk (sorry again for the bad naming
  for(pgroup in unique(buildings$drwng__)){
    new_L_shape <- drawLShape(buildings, n24, pgroup, distance = 1)
    
    if((nrow(L_shapes)>0 )& (!is.null(new_L_shape))){
      L_shapes <- rbind(L_shapes,
                        new_L_shape
      )
      
    }else if(!is.null(new_L_shape)){
      L_shapes <- new_L_shape
      
    }
    
  }
  
  # write to the .shp file to given location
  L_shapes %>%
    st_write(paste0(fold_out, "\\LShape-",ActualX, "-",ActualY, ".shp"), update = TRUE)
  
  return(L_shapes)
  
}

# adjustment to angle function <- get_angle
get_angle <- function(x, y, degree = TRUE) {
  insideoftheta <- (x %*% y / (len(x) * len(y)))[1,1]
  theta <- acos(round(insideoftheta, 10))
  if(degree) theta <- r2d(theta)
  theta
}

# XY Subsetting -----------------------------------------------------------



# Input file setup -------------------------------------------------------------
file_in_build <- list.files(path = fold_in_build, pattern = ".shp", full.names = TRUE)
if(length(file_in_build) != 1){
  warning("Multiple shape files exist for Port 'ShapeBuildings'. Only the first of the ",
          length(file_in_build),
          " detected files will be used")
  file_in_build <- file_in_build[1]
}
if(!validate_shape(file_in_build)){
  stop("Specified shape file for 'ShapeBuildings' does not contain all mandatory files.")
}

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


# load shape file --------------------------------------------------------------
shapeinfo <- loadin(buildingpath = file_in_build,
                    n24path = file_in_pipes,
                    crs = 27700)


# shapeinfo[["Buildings"]] <- shapeinfo[["Buildings"]] %>%
#   rename(drwng__ =drawing_group_id )


# Geometry processing ----------------------------------------------------------

chunksize <- 100 # Keep this at this as it appears to be the fastest "chunk size"

# Remove small polygons and cast POLYGON to MULTIPOLYGON

divide_and_conquer <- function(x,your_func = your_func, func_arg = func_arg, splits = 100,each = 1){
  # x is your object
  # your_func is the function you want to apply to x
  # func_arg is the 2nd argument within the function
  # splits is the amount of splits, the more the split the faster the processing. Default is 100
  # each is the number of times you want to replicate the splits
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- mutate(x, my_split = my_split)
  my_split3 <- tidyr::nest(my_split2, - my_split) 

  models <- purrr::map(my_split3$data, ~ your_func(., func_arg))

  # merge all the splits back together
  bind_lists <- plyr::rbind.fill(models)
  return(bind_lists)
}
shapeinfo[["Buildings"]]$area_cal <- st_area(shapeinfo[["Buildings"]])
shapeinfo[["Buildings"]] <- shapeinfo[["Buildings"]] %>% 
  mutate(area_cal = as.numeric(area_cal)) %>% 
  filter(area_cal > 30) %>% select(-area_cal)
shapeinfo[["Buildings"]] <- divide_and_conquer(shapeinfo[["Buildings"]],st_cast,"MULTIPOLYGON",100)


# process Geom

to_process <- performGeomOperations(st_as_sf(shapeinfo[["Buildings"]]), 
                                    grouping = "drwng__", type = "multipolygon") %>%
  ungroup()


tic()
listofgrids <- createGrid(mainshapes = to_process, shapeinfo[["n24"]], XCount = 40, YCount = 40,
                          buffer = 300, mainexpand = F, extrasexpand = T, removeNull = T, report = T)
toc()


buildingspool <- pblapply(listofgrids[[1]], function(x) left_join(x %>% select(-geometry),
                                                                  shapeinfo[["Buildings"]] %>%
                                                                    as_tibble(),
                                                                  by = "drwng__"))

n24pool <- listofgrids[[2]]




warning("Subsetted fine")

# End subsetting
rm(shapeinfo)
rm(listofgrids)


# Parallel process ---- 
# Set up the parallel workers 
ncore <- 29
cl    <- makeCluster(ncore) 
registerDoParallel(cl, cores = ncore)



# Test Purpose Only -------------------------------------------------------


# n24pool_orig <- n24pool
# buildingspool_orig <- buildingspool


#n24pool <- n24pool_orig
#buildingspool <- n24pool_orig

#n24pool <- n24pool_orig[c(450:460)]
#buildingspool <- buildingspool_orig[c(450:460)]


# n24 <- n24pool[[1]]
# buildings <- buildingspool[[1]]



# Test Section Ends -------------------------------------------------------



tic()
something <- foreach(n24 = iter(n24pool), buildings = iter(buildingspool),
                     .packages = c('dplyr', 'stringr', 'sf', 'lazyeval', 'pbapply'), .errorhandling='stop') %dopar%
  
  drawLShapebyGroup( buildings =  buildings[!duplicated(buildings$geometry),], 
                     n24 =  n24[!duplicated(n24$geometry),], 
                     distance = 1, 
                     fold_out = fold_out_pipes)

toc()




