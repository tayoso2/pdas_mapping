# review the ones with negative delta_depth
bearing_seg_lines %>% filter(delta_depth < 0)
bearing_seg_lines %>% filter(delta_depth < -1)
bearing_seg_lines %>% filter(read_order == "2909141") # should be x_from ...790, x_to ...781


# apply flow direction for unknown bearings -----------------------------------------------
# get intersecting pipes for the null bearings
bearing_seg_lines <- bearing_seg_lines %>% left_join(data_pipes_raster_3[,c("read_order","geometry")], by = "read_order") %>% st_sf()
st_geometry(bearing_seg_lines) <- "geometry"
bearing_seg_lines$geometry <- st_cast(bearing_seg_lines$geometry, "LINESTRING")
bearing_seg_lines %>% 
  select(-start_point,-end_point) %>% 
  st_sf() %>% st_set_crs(27700) %>% 
  st_write("D:/subset_bearing.shp")
bearing_seg_lines %>% filter(delta_depth == 0)
validate_connection <- bearing_seg_lines %>% 
  mutate(x_from = as.character(x_from),x_to = as.character(x_to)) %>% 
  as.data.frame() %>% 
  select(read_order,x_from,y_from,x_to,y_to,start_point,end_point,delta_depth)

#
#.......
#

# final_null_bearings <- get_start_end_points_for_zero_elev(bearing_seg_lines)
null_bearings[20,] # 20
bearings_for_null_bearings[20,] # 20
bearing_seg_lines %>% filter(id == "72094") # 20
bearing_seg_lines %>% filter(id == "72093")
bearing_seg_lines %>% filter(id == "72095")

bearings_for_null_bearings[6,] # 6 # 119 degrees

bearings_for_null_bearings[4,] # 4 # 90 degrees
##################### if the start is supposed to be end, rearrange the start and end using the depth ################

bearing_seg_lines_71140 <- get_bearing_per_row(data_pipes_raster_2 %>% st_sf() %>% filter(id == "71140"))
bearing_seg_lines_71140$bearing # should be 130 degrees
bearing_seg_lines_71140 <- get_bearing_per_row(data_pipes_raster_3b %>% filter(id == "71140"))
bearing_seg_lines_71140$bearing # should be 130 degrees

bearing_seg_lines_72034 <- get_bearing_per_row(data_pipes_raster_2 %>% st_sf() %>% filter(id == "72034"))
bearing_seg_lines_72034$bearing # 45 degrees is wrong. should be 225 degrees
bearing_seg_lines_72034 <- get_bearing_per_row(data_pipes_raster_3b %>% filter(id == "72034"))
bearing_seg_lines_72034$bearing # 45 degrees is wrong. should be 225 degrees
bearings_for_null_bearings_72034 <- get_bearing_per_row_2(final_null_bearings_end_point %>% filter(read_order <= 2909147, read_order >= 2909141))
data_pipes_raster_2 %>% filter(read_order <= 2909147, read_order >= 2909141)
# final_null_bearings %>% filter(read_order <= 2909147, read_order >= 2909141) #%>% filter(!is.na(read_order_new))
# bearings_for_null_bearings %>% filter(read_order <= 2909147, read_order >= 2909141)
# bearings_for_null_bearings_2 %>% filter(read_order <= 2909147, read_order >= 2909141)
# final_null_bearings  %>% filter(!is.na(read_order_new))

bearing_seg_lines_71072 <- get_bearing_per_row(data_pipes_raster_2 %>% st_sf() %>% filter(id == "71072"))
bearing_seg_lines_71072$bearing # 45 degrees is wrong. should be 225 degrees
bearing_seg_lines_71072 <- get_bearing_per_row(data_pipes_raster_3b %>% filter(id == "71072"))
bearing_seg_lines_71072$bearing # 45 degrees is wrong. should be 225 degrees


# this is for -ve..coming back empty
null_bearings %>% filter(read_order == "2909066")
subset_test <- null_bearings %>% filter(read_order == "2909067")
get_start_end_points_for_zero_elev(null_bearings %>% filter(read_order == "2909067"),not_null_bearings_nogeom)
test_bearing_grabber_1 <- merge(subset_test[i,null_bearings_columns],not_null_bearings_nogeom[,not_null_bearings_columns],
                                by = "start_point", all.x=TRUE, all.y=FALSE)
test_bearing_grabber_1$keep_col <-  "x start-x end"
test_bearing_grabber_2 <- merge(subset_test[i,null_bearings_columns],not_null_bearings_nogeom[,not_null_bearings_columns],
                                by.x = "start_point",by.y = "end_point", all.x=TRUE, all.y=FALSE)
test_bearing_grabber_2$keep_col <-  "x start-x start"
test_bearing_grabber_3 <- merge(subset_test[i,null_bearings_columns],not_null_bearings_nogeom[,not_null_bearings_columns],
                                by = "end_point", all.x=TRUE, all.y=FALSE)
test_bearing_grabber_3$keep_col <-  "x end-x start"
test_bearing_grabber_4 <- merge(subset_test[i,null_bearings_columns],not_null_bearings_nogeom[,not_null_bearings_columns],
                                by.x = "end_point",by.y = "start_point", all.x=TRUE, all.y=FALSE)
test_bearing_grabber_4$keep_col <-  "x end-x end"






# ###############
pos_bearing # everything - ones wit NA
bearing_seg_lines # everything

# get the bearing for the remaining pipes
give_pipe_nearest_bearing_attribute <- function(pos_bearing = pos_bearing,bearing_seg_lines = bearing_seg_lines){
  
  # remove columns if exist
  # pos_bearing = subset(pos_bearing, select = -c(start_point,end_point,from_geom,to_geom))

  null_bearings_r_p <- bearing_seg_lines %>% 
    as.data.frame() %>%
    anti_join(pos_bearing, by = "read_order") %>% 
    select(-geometry) %>%
    filter(is.na(bearing)) %>%
    mutate_all(as.character) %>% 
    mutate(bearing = as.numeric(bearing))
  not_null_bearings_nogeom_r_p <- pos_bearing %>% 
    # select(-start_point,-end_point,-from_geom, -to_geom) %>% 
    as.data.frame() %>%
    select(-geometry) %>%
    left_join(bearing_seg_lines[,c("read_order","end_point","from_geom","to_geom","start_point")], by = "read_order") %>%
    select(-geometry) %>%
    filter(!is.na(bearing)) %>%
    mutate_all(as.character) %>% 
    mutate(bearing = as.numeric(bearing))
  
  # get the rowcolumn where null_bearing startpoint = not_null startpoint or endpoint AND
  ## null_bearing endpoint = not_null startpoint or endpoint -------------------------------------------------------
  final_null_bearings_r_p <- get_start_end_points_for_zero_elev(null_bearings_r_p,not_null_bearings_nogeom_r_p)
  final_null_bearings_r_p$read_order %>% unique()
  final_null_bearings_r_p <- final_null_bearings_r_p %>% select(start_point, read_order, end_point, keep_col) %>% 
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
    filter(keep_col == keep_col_mode) %>% 
    select(-keep_col_mode) %>% 
    left_join(unique(data_pipes_raster_3[,c("id","read_order","geometry","end_equals_start",
                                            "start_point_depth","end_point_depth", "delta_depth")]),by = "read_order")
  
  # merge NA depth with +ve depth
  pos_bearing <- plyr::rbind.fill(bearings_for_null_bearings_2_r_p,pos_bearing)  %>% distinct()
  pos_bearing <- pos_bearing %>% st_sf()
  st_geometry(pos_bearing) <- "geometry"
  pos_bearing$geometry <- st_cast(pos_bearing$geometry, "LINESTRING")
  pos_bearing <- pos_bearing %>% 
    select(-start_point,-end_point,-from_geom, -to_geom) %>%
    st_sf() %>% st_set_crs(27700)
  
  return(pos_bearing)

}

#####
# }
# 
# take_1 <- give_pipe_nearest_bearing_attribute(pos_bearing = pos_bearing,bearing_seg_lines = bearing_seg_lines)
# take_2 <- give_pipe_nearest_bearing_attribute(pos_bearing = take_1,bearing_seg_lines = bearing_seg_lines)
# take_3 <- give_pipe_nearest_bearing_attribute(pos_bearing = take_2,bearing_seg_lines = bearing_seg_lines)
# take_4 <- give_pipe_nearest_bearing_attribute(pos_bearing = take_3,bearing_seg_lines = bearing_seg_lines)
# take_5 <- give_pipe_nearest_bearing_attribute(pos_bearing = take_4,bearing_seg_lines = bearing_seg_lines)
# take_6 <- give_pipe_nearest_bearing_attribute(pos_bearing = take_5,bearing_seg_lines = bearing_seg_lines)
# take_7 <- give_pipe_nearest_bearing_attribute(pos_bearing = take_6,bearing_seg_lines = bearing_seg_lines)
# take_8 <- give_pipe_nearest_bearing_attribute(pos_bearing = take_7,bearing_seg_lines = bearing_seg_lines)
# take_9 <- give_pipe_nearest_bearing_attribute(pos_bearing = take_8,bearing_seg_lines = bearing_seg_lines)
# take_10 <- give_pipe_nearest_bearing_attribute(pos_bearing = take_9,bearing_seg_lines = bearing_seg_lines)

rerun_this_func <- function(pos_bearing = pos_bearing,bearing_seg_lines = bearing_seg_lines,give_pipe_nearest_bearing_attribute){
  out = pos_bearing
  while(nrow(out) != nrow(bearing_seg_lines)){
    out <- give_pipe_nearest_bearing_attribute(out,bearing_seg_lines)
  }
  break
  
  return(out)
}
# try this?
out_1 = pos_bearing
while(nrow(out_1) != nrow(bearing_seg_lines)){
  out_1 <- give_pipe_nearest_bearing_attribute(out_1,bearing_seg_lines)
}

repeat


all_bearings <- rerun_this_func(pos_bearing = pos_bearing,bearing_seg_lines = bearing_seg_lines,give_pipe_nearest_bearing_attribute)

# take_4 %>% 
#   st_write("D:/take_4.shp")
# 
# 
# take_10 %>% 
#   st_write("D:/take_10.shp")




final_null_bearings = data_pipes_raster_3
col_list = "start_point"

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













