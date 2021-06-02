

# Packages ---------------------------------------------------------------------
library(sf)
library(tmap)
library(plyr)
library(dplyr)
library(magrittr)
library(data.table)
library(tibble)

# Parallelisation
library(parallel)
library(doParallel)
library(foreach)
library(tictoc)



# load the needed data
elevation_assetbase <- readRDS("D:/STW PDAS/Git/pdas_mapping/pipe-depth-from-lidar-images/scripts/data_pipes_raster_2.rds")
data_pipes_raster_2 <- st_read("D:/STW PDAS/Phase 2 datasets/segmented_road_proxies_distances_FCS/segmented_road_proxies_distances_FCS.shp") # load wills filtered proxies

# add the elevation attrs
data_pipes_raster_2$id_0 <- 1:nrow(data_pipes_raster_2)
elevation_assetbase$id_0 <- 1:nrow(elevation_assetbase)
elevation_assetbase <- elevation_assetbase %>% select(-geometry)
data_pipes_raster_2 <- data_pipes_raster_2 %>% 
  filter(assmd_s != "close_to_F_and_S") %>% # remove if you want to include all proxies
  left_join(elevation_assetbase, by = "id_0")
rm(elevation_assetbase)

# functions
get_bearing_per_row <- function(df = df,messaging = 1) {
  df_2 <- df %>%
    mutate(from_geom = ifelse(end_point_depth > start_point_depth & delta_depth > 0,end_point,
                              ifelse(start_point_depth > end_point_depth  & delta_depth > 0 ,start_point,"unknown")),
           to_geom = ifelse(end_point_depth > start_point_depth  & delta_depth > 0,start_point,
                            ifelse(start_point_depth > end_point_depth  & delta_depth > 0,end_point,"unknown"))
    ) %>% 
    filter(from_geom != "unknown" | to_geom != "unknown") %>% 
    st_cast("MULTIPOINT")
  df_3 <- df %>%
    mutate(from_geom = ifelse(end_point_depth > start_point_depth & delta_depth > 0,end_point,
                              ifelse(start_point_depth > end_point_depth  & delta_depth > 0 ,start_point,"unknown")),
           to_geom = ifelse(end_point_depth > start_point_depth  & delta_depth > 0,start_point,
                            ifelse(start_point_depth > end_point_depth  & delta_depth > 0,end_point,"unknown"))
    ) %>% 
    filter(from_geom == "unknown" | to_geom == "unknown") %>% 
    st_cast("MULTIPOINT")
  
  # Add iteration
  for (i in 1:nrow(df_2)) {
    # message for progress
    #if(i %% (nrow(df_2)/100) == 0){message(paste0((i/nrow(df_2))*100),"% at ", Sys.time())}
    #if(i %% (nrow(df_2)/10) == 0){message(paste0((i/nrow(df_2))*100),"% at ", Sys.time())}
    if(i %% messaging == 0){message(paste0("row number ",i))}
    message(paste0("row number ",i))
    # get the 4 important coords for calculating the bearing
    x_from <- as.matrix(df_2$from_geom[[i]], col=2)[1,1]
    y_from <- as.matrix(df_2$from_geom[[i]], col=2)[1,2]
    x_to <- as.matrix(df_2$to_geom[[i]], col=2)[1,1]
    y_to <- as.matrix(df_2$to_geom[[i]], col=2)[1,2]
    # add bearing for each row
    df_2$bearing[[i]] <- (360 + (90 - ((180/pi)*atan2(y_to-y_from, x_to-x_from)))) %% 360
    df_2$x_from[[i]] <- x_from
    df_2$y_from[[i]] <- y_from
    df_2$x_to[[i]] <- x_to
    df_2$y_to[[i]] <- y_to
  }
  df_2 <- plyr::rbind.fill(df_2,df_3)
  return(df_2)
}
get_bearing_per_row_2 <- function(df = df ,messaging = 1) {
  df_2 <- df %>%
    mutate(from_geom = start_point,
           to_geom = end_point) #%>% 
  # filter(from_geom != "unknown" | to_geom != "unknown") %>% 
  # st_cast("MULTIPOINT")
  
  # Add iteration
  for (i in 1:nrow(df_2)) {
    # message for progress
    #if(i %% (nrow(df_2)/100) == 0){message(paste0((i/nrow(df_2))*100),"% at ", Sys.time())}
    #if(i %% (nrow(df_2)/10) == 0){message(paste0((i/nrow(df_2))*100),"% at ", Sys.time())}
    if(i %% messaging == 0){message(paste0("row number ",i))}
    message(paste0("row number ",i))
    # get the 4 important coords for calculating the bearing
    x_to <- as.matrix(df_2$from_geom[[i]], col=2)[1,1]
    y_to <- as.matrix(df_2$from_geom[[i]], col=2)[1,2]
    x_from <- as.matrix(df_2$to_geom[[i]], col=2)[1,1]
    y_from <- as.matrix(df_2$to_geom[[i]], col=2)[1,2]
    # add bearing for each row
    df_2$bearing[[i]] <- (360 + (90 - ((180/pi)*atan2(y_to-y_from, x_to-x_from)))) %% 360
    df_2$x_from[[i]] <- x_from
    df_2$y_from[[i]] <- y_from
    df_2$x_to[[i]] <- x_to
    df_2$y_to[[i]] <- y_to
  }
  return(df_2)
}
apply_function_per_row <- function(x = x ,n24s = n24s,messaging = 1) {
  for (i in 1:nrow(x)) {
    # message for progress
    #if(i %% (nrow(x)/100) == 0){message(paste0((i/nrow(x))*100),"% at ", Sys.time())}
    #if(i %% (nrow(x)/10) == 0){message(paste0((i/nrow(x))*100),"% at ", Sys.time())}
    if(i %% messaging == 0){message(paste0("row number ",i))}
    message(paste0("row number ",i))
    if (i == 1) {
      x2 <- getFlowDirection(x[i,],n24s)
    } else{
      #broken_geom_4 <- break_lines(x[i,])
      x2 <- rbind(data.frame(getFlowDirection(x[i,],n24s)), x2)
    }
  }
  # rename the column, get rid of the index
  colnames(x2) <- "flow_direction"
  x2 <- x2 %>% as.data.table()
}
convert_list_to_points <- function(final_null_bearings = final_null_bearings,col_list = "start_point"){
  
  # get rid of the geometry column
  final_null_bearings <- final_null_bearings %>% as.data.frame() 
  
  # rename the geom column to a proxy name
  colnames(final_null_bearings)[which(names(final_null_bearings) == col_list)] <- "my_geom_column"
  final_null_bearings[,"my_geom_column"] <- gsub("c(", "", final_null_bearings[,"my_geom_column"], fixed = TRUE)
  final_null_bearings[,"my_geom_column"] <- gsub(")", "", final_null_bearings[,"my_geom_column"], fixed = TRUE)
  final_null_bearings[,"my_geom_column"] <- gsub(",", "", final_null_bearings[,"my_geom_column"], fixed = TRUE) 
  
  # split th x and y coords
  fix_points <- final_null_bearings %>% 
    mutate(X = as.numeric(sapply(my_geom_column, function(i) unlist(stringr::str_split(i, " "))[1])), # split the X Y
           Y = as.numeric(sapply(my_geom_column, function(i) unlist(stringr::str_split(i, " "))[2]))) %>% 
    dplyr::select(-my_geom_column)
  
  # convert the coords to points
  fix_points <- st_as_sf(fix_points,coords = c("X","Y"))
  st_crs(fix_points) <- 27700
  
  # give the column its old name back
  fix_points <- fix_points %>% as_tibble() %>% 
    dplyr::rename(my_geom_column = geometry)
  colnames(fix_points)[which(names(fix_points) == "my_geom_column")] <- col_list
  
  return(fix_points)
}
get_start_end_points_for_zero_elev <- function(null_bearings,not_null_bearings_nogeom, messaging = 1){
  # null_bearings <- bearing_seg_lines %>% 
  #   as.data.frame() %>%
  #   select(-geometry) %>%
  #   filter(is.na(bearing)) %>%
  #   mutate_all(as.character)
  # not_null_bearings_nogeom <- bearing_seg_lines %>% 
  #   as.data.frame() %>%
  #   select(-geometry) %>%
  #   filter(!is.na(bearing)) %>%
  #   mutate_all(as.character)
  
  null_bearings_columns = c("read_order","start_point","end_point")
  not_null_bearings_columns = c("read_order","start_point","end_point","x_from", "y_from", "x_to", "y_to","delta_depth")
  
  for (i in 1:nrow(null_bearings)) {
    # message detailing progress
    if(i %% messaging == 0){message(paste0("row number ",i))}
    
    # get the possible 
    test_bearing_grabber_1 <- merge(null_bearings[i,null_bearings_columns],not_null_bearings_nogeom[,not_null_bearings_columns],
                                    by = "start_point", all.x=TRUE, all.y=FALSE)
    test_bearing_grabber_1$keep_col <-  "x start-x end"
    test_bearing_grabber_2 <- merge(null_bearings[i,null_bearings_columns],not_null_bearings_nogeom[,not_null_bearings_columns],
                                    by.x = "start_point",by.y = "end_point", all.x=TRUE, all.y=FALSE)
    test_bearing_grabber_2$keep_col <-  "x start-x start"
    test_bearing_grabber_3 <- merge(null_bearings[i,null_bearings_columns],not_null_bearings_nogeom[,not_null_bearings_columns],
                                    by = "end_point", all.x=TRUE, all.y=FALSE)
    test_bearing_grabber_3$keep_col <-  "x end-x start"
    test_bearing_grabber_4 <- merge(null_bearings[i,null_bearings_columns],not_null_bearings_nogeom[,not_null_bearings_columns],
                                    by.x = "end_point",by.y = "start_point", all.x=TRUE, all.y=FALSE)
    test_bearing_grabber_4$keep_col <-  "x end-x end"
    
    # rename the columns and bind all 3 outputs
    colnames(test_bearing_grabber_1) <- gsub(".x","",colnames(test_bearing_grabber_1),fixed = TRUE)
    colnames(test_bearing_grabber_1) <- gsub(".y","_new",colnames(test_bearing_grabber_1),fixed = TRUE)
    colnames(test_bearing_grabber_1) <- gsub("_from","_from_new",colnames(test_bearing_grabber_1),fixed = TRUE)
    colnames(test_bearing_grabber_1) <- gsub("_to","_to_new",colnames(test_bearing_grabber_1),fixed = TRUE)
    
    colnames(test_bearing_grabber_2) <- gsub(".x","",colnames(test_bearing_grabber_2),fixed = TRUE)
    colnames(test_bearing_grabber_2) <- gsub(".y","_new",colnames(test_bearing_grabber_2),fixed = TRUE)
    colnames(test_bearing_grabber_2) <- gsub("_from","_from_new",colnames(test_bearing_grabber_2),fixed = TRUE)
    colnames(test_bearing_grabber_2) <- gsub("_to","_to_new",colnames(test_bearing_grabber_2),fixed = TRUE)
    
    colnames(test_bearing_grabber_3) <- gsub(".x","",colnames(test_bearing_grabber_3),fixed = TRUE)
    colnames(test_bearing_grabber_3) <- gsub(".y","_new",colnames(test_bearing_grabber_3),fixed = TRUE)
    colnames(test_bearing_grabber_3) <- gsub("_from","_from_new",colnames(test_bearing_grabber_3),fixed = TRUE)
    colnames(test_bearing_grabber_3) <- gsub("_to","_to_new",colnames(test_bearing_grabber_3),fixed = TRUE)
    
    colnames(test_bearing_grabber_4) <- gsub(".x","",colnames(test_bearing_grabber_4),fixed = TRUE)
    colnames(test_bearing_grabber_4) <- gsub(".y","_new",colnames(test_bearing_grabber_4),fixed = TRUE)
    colnames(test_bearing_grabber_4) <- gsub("_from","_from_new",colnames(test_bearing_grabber_4),fixed = TRUE)
    colnames(test_bearing_grabber_4) <- gsub("_to","_to_new",colnames(test_bearing_grabber_4),fixed = TRUE)
    
    # merge all the above and pass the rules
    test_bearing_grabber_all <- plyr::rbind.fill(test_bearing_grabber_1,test_bearing_grabber_2,
                                                 test_bearing_grabber_3,test_bearing_grabber_4) %>% 
      distinct() %>% 
      dplyr::filter(!is.na(read_order_new)) %>%
      mutate(start_point = case_when(keep_col == "x start-x end" ~ end_point,
                                     keep_col != "x start-x end" ~ start_point),
             end_point = case_when(keep_col == "x end-x start" ~ start_point,
                                   keep_col != "x end-x start" ~ end_point)) %>% 
      dplyr::select(-start_point_new,-end_point_new)
    
    if(i == 1) {
      out <- test_bearing_grabber_all
    } else{
      out <- rbind(out,test_bearing_grabber_all)
    }
  }
  return(as.data.frame(out))
}
give_pipe_nearest_bearing_attribute <- function(pos_bearing = pos_bearing,bearing_seg_lines = bearing_seg_lines){
  
  # remove columns if exist
  # pos_bearing = subset(pos_bearing, select = -c(start_point,end_point,from_geom,to_geom))
  
  null_bearings_r_p <- bearing_seg_lines %>% 
    as.data.frame() %>%
    anti_join(pos_bearing, by = "read_order") %>% 
    dplyr::select(-geometry) %>%
    dplyr::filter(is.na(bearing)) %>%
    mutate_all(as.character) %>% 
    mutate(bearing = as.numeric(bearing))
  
  not_null_bearings_nogeom_r_p <- bearing_seg_lines %>%
    mutate(read_order = as.character(read_order)) %>% 
    as.data.frame() %>%
    dplyr::select(-geometry) %>%
    anti_join(null_bearings_r_p, by = "read_order") %>%
    mutate_all(as.character) %>% 
    mutate(bearing = as.numeric(bearing))
  
  # get the rowcolumn where null_bearing startpoint = not_null startpoint or endpoint AND
  ## null_bearing endpoint = not_null startpoint or endpoint -------------------------------------------------------
  final_null_bearings_r_p <- get_start_end_points_for_zero_elev(null_bearings_r_p,not_null_bearings_nogeom_r_p)
  final_null_bearings_r_p$read_order %>% unique()
  final_null_bearings_r_p <- final_null_bearings_r_p %>% dplyr::select(start_point, read_order, end_point, keep_col) %>% 
    mutate(keep_col = ifelse(keep_col == "x end-x end","x start-x start",keep_col)) %>% 
    distinct()
  # convert the start and end points to geometry columns
  final_null_bearings_st_point_r_p <- convert_list_to_points(final_null_bearings_r_p,"start_point")
  final_null_bearings_end_point_r_p <- convert_list_to_points(final_null_bearings_st_point_r_p,"end_point") %>%
    as.data.frame()
  
  # get bearing of these pipes
  bearings_for_null_bearings_r_p <- get_bearing_per_row_2(final_null_bearings_end_point_r_p)
  
  # add the id and get the median bearing for each read_order (for NA depth)
  bearings_for_null_bearings_2_r_p <- bearings_for_null_bearings_r_p %>%
    mutate(read_order = as.integer(read_order),
           keep_col_mode = mode(keep_col)) %>% 
    dplyr::filter(keep_col == keep_col_mode) %>% 
    dplyr::select(-keep_col_mode) %>% 
    left_join(unique(data_pipes_raster_3[,c("id","read_order","geometry","end_equals_start",
                                            "start_point_depth","end_point_depth", "delta_depth")]),by = "read_order")
  
  # merge NA depth with +ve depth
  pos_bearing <- plyr::rbind.fill(bearings_for_null_bearings_2_r_p,pos_bearing)  %>% distinct()
  pos_bearing <- pos_bearing %>% st_sf()
  st_geometry(pos_bearing) <- "geometry"
  pos_bearing$geometry <- st_cast(pos_bearing$geometry, "LINESTRING")
  pos_bearing <- pos_bearing %>% 
    dplyr::select(-start_point,-end_point,-from_geom, -to_geom) %>%
    st_sf() %>% st_set_crs(27700)
  
  return(pos_bearing)
  
}
create_lists <- function(x, splits = 29,each = 1){
  my_split <- base::rep(1:splits,each = each, length = nrow(x))
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  list_lists <- base::split(my_split2,my_split2$my_split) 
  #list_lists <- purrr::map(my_split3$data, ~ split(., func_arg)) 
  return(list_lists)
}
get_mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}
# rerun_this_func <- function(pos_bearing = pos_bearing,bearing_seg_lines = bearing_seg_lines,give_pipe_nearest_bearing_attribute){
#   out = pos_bearing
#   while(nrow(out) != nrow(bearing_seg_lines)){
#     out <- give_pipe_nearest_bearing_attribute(out,bearing_seg_lines)
#   }
#   return(out)
# }

# go to the current working directory and load functions
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
source("4 - Subsetter.R")



# re-categorize what is termed start and end point -------------------------------
# data_pipes_raster_2 <- data_pipes_raster_2 %>% filter(id > 60000, id < 100000) # delete me, I am for testing.
data_pipes_raster_3 <- data_pipes_raster_2 %>%
  mutate(
    end_point = as.character(end_point),
    start_point = as.character(start_point),
    end_point = ifelse(
      end_point_depth > start_point_depth | start_point_depth > end_point_depth,
      end_point,end_point),
    start_point = ifelse(
      end_point_depth > start_point_depth | start_point_depth > end_point_depth,
      start_point,start_point)) 
data_pipes_raster_3a <- convert_list_to_points(data_pipes_raster_3,"start_point")
data_pipes_raster_3b <- convert_list_to_points(data_pipes_raster_3a,"end_point") %>%
  st_sf() %>% st_set_crs(27700)

# get the bearing for each pipe linestring (+ve depth) -----------------------------------------------
# determine number of cores to use
ncore <- 1
cl    <- makeCluster(ncore) 
registerDoParallel(cl, cores = ncore)

tic()
data_pipes_raster_3b <- create_lists(data_pipes_raster_3b)
bearing_seg_lines <- foreach(df_list = iter(data_pipes_raster_3b),
                             .packages = c('dplyr', 'sp', 'sf'), .errorhandling='stop') %dopar%
  get_bearing_per_row(df_list)
bearing_seg_lines <- plyr::rbind.fill(bearing_seg_lines)
toc()

# add the geometry linestrings
bearing_seg_lines <- bearing_seg_lines %>% left_join(data_pipes_raster_3[,c("read_order","geometry")], by = "read_order") %>% st_sf()
st_geometry(bearing_seg_lines) <- "geometry"
pos_bearing <- bearing_seg_lines %>% filter(!is.na(bearing))

# save.image
save.image("D:/STW PDAS/RDATA/pipe_direction.rdata")

# apply flow direction for unknown bearings -----------------------------------------------
# get intersecting pipes for the null bearings
# create grids to hasten the processing 
pos_bearing_geom <- pos_bearing %>% 
  st_drop_geometry() %>% 
  left_join(unique(bearing_seg_lines[, c(
    "read_order", "geometry","start_point","end_point")]), 
    by = "read_order") %>% st_sf()
st_geometry(pos_bearing_geom) <- "geometry"

# dup pos_bearing_geom, this time with no geometry
pos_bearing_no_geom <- pos_bearing_geom
st_geometry(pos_bearing_no_geom) <- NULL

# filter to select non-positive delta_depth
neg_zero_bearing <- bearing_seg_lines %>%
  anti_join(pos_bearing_no_geom, by = "read_order") 

# this needs to be updated to grab the bearing of the pos and add to the neg
tic()
listofgrids <- createGrid(mainshapes = neg_zero_bearing, pos_bearing_geom, XCount = 10, YCount = 10,
                          buffer = 300, mainexpand = F, extrasexpand = T, removeNull = T, report = T)
toc()

# try and hasten the process using the list of grid
my_func <- function(listofgrids){
  for(i in 1:length(listofgrids[[1]])){ # 2){
    message(paste0("Grid:", i))
    listofgrids_1 <- listofgrids[[1]][i] %>% rbind.fill() %>% st_sf() %>%
      as.data.frame() %>%
      dplyr::select(-geometry) %>%
      filter(is.na(bearing)) %>%
      mutate_all(as.character) %>%
      mutate(bearing = as.numeric(bearing))
    # st_geometry(listofgrids_1) <- "geometry"
    listofgrids_2 <- listofgrids[[2]][i] %>% rbind.fill() %>% st_sf() %>%
      as.data.frame() %>%
      dplyr::select(-geometry) %>%
      filter(!is.na(bearing)) %>% 
      # add this join to the top
      # left_join(unique(bearing_seg_lines[, c(
      #   "read_order", "geometry","start_point","end_point")]), 
      #   by = "read_order") %>%
      mutate_all(as.character) %>%
      mutate(bearing = as.numeric(bearing)) 
    # st_geometry(listofgrids_2) <- "geometry"
    # tic()
    # listofgrids_1_lists <- create_lists(listofgrids_1)
    # final_null_bearings <- foreach(listofgrids_1 = iter(listofgrids_1),
    #                                .packages = c('dplyr', 'sp', 'sf'), .errorhandling='stop') %dopar%
    final_null_bearings <- get_start_end_points_for_zero_elev(listofgrids_1,listofgrids_2)#not_null_bearings_nogeom)
    final_null_bearings <- plyr::rbind.fill(final_null_bearings)
    # toc()
    if (i == 1) {
      final.out <- final_null_bearings
    } else{
      final.out <- plyr::rbind.fill(final.out, final_null_bearings)
    }
  }
  return(final.out)
}


tic()
my_ans <- my_func(listofgrids)
toc()

# final_null_bearings <- get_start_end_points_for_zero_elev(null_bearings,not_null_bearings_nogeom)
final_null_bearings <- my_ans %>% dplyr::select(start_point, read_order, end_point, keep_col) %>% 
  mutate(keep_col = ifelse(keep_col == "x end-x end","x start-x start",keep_col)) %>% 
  distinct()
# convert the start and end points to geometry columns
final_null_bearings_st_point <- convert_list_to_points(final_null_bearings,"start_point")
final_null_bearings_end_point <- convert_list_to_points(final_null_bearings_st_point,"end_point") %>%
  as.data.frame()

# get bearing of these pipes
bearings_for_null_bearings <- get_bearing_per_row_2(final_null_bearings_end_point)

# add the id and get the median bearing for each read_order (for NA/-ve depth) --------
bearings_for_null_bearings_2 <- bearings_for_null_bearings %>%
  mutate(read_order = as.integer(read_order),
         keep_col_mode = mode(keep_col)) %>% 
  filter(keep_col == keep_col_mode) %>% 
  dplyr::select(-keep_col_mode) %>% 
  left_join(unique(data_pipes_raster_3[,c("id","read_order","geometry","end_equals_start",
                                          "start_point_depth","end_point_depth", "delta_depth")]),by = "read_order")

# merge NA depth with +ve depth
pos_bearing <- plyr::rbind.fill(bearings_for_null_bearings_2,pos_bearing) %>% distinct()
pos_bearing <- pos_bearing %>% st_sf()
st_geometry(pos_bearing) <- "geometry"
pos_bearing$geometry <- st_cast(pos_bearing$geometry, "LINESTRING")
pos_bearing <- pos_bearing %>% 
  dplyr::select(-start_point,-end_point,-from_geom, -to_geom) %>%
  st_sf() %>% st_set_crs(27700)

# get the bearing for the remaining pipes -------->> REPEAT UNTIL NO MORE PIPES TO GRAB ATTRS FROM <<------------------
# first create a list of grids to re-run with
pos_bearing_geom_2 <- pos_bearing %>% 
  st_drop_geometry() %>% 
  left_join(unique(bearing_seg_lines[, c(
    "read_order", "geometry","start_point","end_point")]), 
    by = "read_order") %>% st_sf() %>% st_set_crs(27700)
st_geometry(pos_bearing_geom_2) <- "geometry"
neg_zero_bearing_2 <- bearing_seg_lines %>% 
  as.data.frame() %>%
  anti_join(pos_bearing, by = "read_order") %>% 
  st_sf() %>% st_set_crs(27700)
tic()
listofgrids_unknown_1 <- createGrid(mainshapes = neg_zero_bearing_2, pos_bearing_geom_2, XCount = 40, YCount = 40,
                                    buffer = 300, mainexpand = F, extrasexpand = T, removeNull = T, report = T)
toc()

tic()
my_ans_2 <- my_func(listofgrids_unknown_1)
toc()

# mutate end-end to start-start because they mean the same thing
final_null_bearings_unknown <- my_ans_2 %>% dplyr::select(start_point, read_order, end_point, keep_col) %>% 
  mutate(keep_col = ifelse(keep_col == "x end-x end","x start-x start",keep_col)) %>% 
  distinct()
# convert the start and end points to geometry columns
final_null_bearings_unknown_st_point <- convert_list_to_points(final_null_bearings_unknown,"start_point")
final_null_bearings_unknown_end_point <- convert_list_to_points(final_null_bearings_unknown_st_point,"end_point") %>%
  as.data.frame()

# get bearing of these pipes
bearings_for_null_bearings_unknown <- get_bearing_per_row_2(final_null_bearings_unknown_end_point)

# add the id and get the median bearing for each read_order (for NA/-ve depth) --------
bearings_for_null_bearings_unknown_2 <- bearings_for_null_bearings_unknown %>%
  mutate(read_order = as.integer(read_order),
         keep_col_mode = mode(keep_col)) %>% 
  filter(keep_col == keep_col_mode) %>% 
  dplyr::select(-keep_col_mode) %>% 
  left_join(unique(data_pipes_raster_3[,c("id","read_order","geometry","end_equals_start",
                                          "start_point_depth","end_point_depth", "delta_depth")]),by = "read_order")

# merge NA depth with +ve depth
pos_bearing <- plyr::rbind.fill(bearings_for_null_bearings_unknown_2,pos_bearing) %>% distinct()
pos_bearing <- pos_bearing %>% st_sf()
st_geometry(pos_bearing) <- "geometry"
pos_bearing$geometry <- st_cast(pos_bearing$geometry, "LINESTRING")
pos_bearing <- pos_bearing %>% 
  dplyr::select(-start_point,-end_point,-from_geom, -to_geom) %>%
  st_sf() %>% st_set_crs(27700)


# --------------------------------------- END OF REPEAT ----------------------------------------------->

# save workspace image
# save.image("D:/STW PDAS/RDATA/pipe_direction_3.rdata")
# load("D:/STW PDAS/RDATA/pipe_direction_3.rdata")


# add on the remaining assets and save as proxy_bearing
save_me <- plyr::rbind.fill(pos_bearing,neg_zero_bearing_2) %>% distinct()
save_me <- save_me %>% st_sf()
st_geometry(save_me) <- "geometry"
save_me$geometry <- st_cast(save_me$geometry, "LINESTRING")
save_me <- save_me %>% 
  dplyr::select(-start_point,-end_point,-from_geom, -to_geom, -id_0, -my_split, -id, -id_x) %>%
  st_sf() %>% st_set_crs(27700)

# test that it contains 1 row per asset
save_me <- save_me[!duplicated(save_me$read_order),]
save_me %>% filter(read_order == "224446")


# add system type ---------------------------------------------------------------------------
proxyful <- readRDS("D:/STW PDAS/Git/pdas_mapping/proxy sewer analysis/proxy_asset_base_atts.rds")
proxyful <- proxyful %>% as.data.frame() %>% select(-geometry)
save_me_2 <- save_me %>% 
  left_join(proxyful[,c("old_nq_","Type","CONFIDENCE", "MATERIA", "DIAMETE", "nw_nq_d")], by = c("read_order"="old_nq_")) %>% 
  select(-c(start_point_depth,end_point_depth,delta_depth,end_equals_start, keep_col,x_from,x_to,y_from, y_to, id_y)) %>% 
  rename(sys_type = Type) %>% 
  rename(proxy_id = nw_nq_d) %>% 
  mutate(sewer_type_flag = "1") %>% 
  select(proxy_id, everything())
summary(save_me_2)
save_me_2 %>% 
  saveRDS("C:/Users/TOsosanya/Desktop/proxy_bearing_and_attrs.rds")
save_me_2 %>% 
  st_write("C:/Users/TOsosanya/Desktop/proxy_bearing_and_attrs.shp")

