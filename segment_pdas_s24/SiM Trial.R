# send nik V-893-03



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


# FOR S24  ----------------------------------------------------------------------------------------------

# load data
link_da <- "D:/STW PDAS/Phase 2 datasets/Sewerage_Drainage_Area/"
Sewerage_Drainage_Area <- st_read(paste0(link_da,"Sewerage_Drainage_Area.shp"))
s24_assetbase_link <- "D:/STW PDAS/s24_phase_2/"
s24_assetbase0 <- st_read(paste0(s24_assetbase_link,"s24_lines_with_attrs_22102020.shp"))
# s24_assetbase0 <- st_read("C:/Users/TOsosanya/Downloads/LShape-29-23/LShape-29-23.shp")
s24_assetbase0 <- subsetter[!duplicated(subsetter$drwng__), ] # --------------------------------come back to me


# add DA
Sewerage_Drainage_Area <- st_transform(Sewerage_Drainage_Area,crs = 27700)
st_crs(s24_assetbase0) <- 27700
# s24_da <- get_intersection(s24_assetbase[1:10000,],Sewerage_Drainage_Area,"id_0","OBJECTID","ID_DA_CODE") # add DA
s24_assetbase_int <- get_intersection(s24_assetbase0,Sewerage_Drainage_Area[,c("OBJECTID","ID_DA_CODE")],"drwng__","OBJECTID")


# filter to DA - "V-893-03"
s24_assetbase0_int <- s24_assetbase_int %>% filter(ID_DA_CODE == "V-893-03") %>% 
  left_join(s24_assetbase0[,"drwng__"], by = "drwng__") %>% st_sf() %>% st_set_crs(27700)

s24_assetbase0_int %>% st_write("D:/check_subset.shp")

# FOR PDAS  ----------------------------------------------------------------------------------------------

# # load data
# s24_assetbase_link <- "D:/STW PDAS/pdas_phase_2/"
# s24_assetbase0 <- st_read(paste0(s24_assetbase_link,"pdas_assetbase_12102020.shp"))

# output folder
fold_out <- "D:/STW PDAS/Output"


# functions
get_intersection <- function(x,y,x_row.id=x_row.id,y_col.id=y_col.id){
  
  # group.id = usally the uid on the lhs, what to group the the object by and subsequently find the modal value for that group
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
  
  return(df_combine)
}
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
  x$index = 1:nrow(x)
  for (i in x$index) {
    # message detailing progress
    if(i %% messaging == 0){message(paste0("row number ",i))}
    x$geometry[[i]][[1]][4] <- ifelse(x[["geometry"]][[i]][[1]][3]== x[["geometry"]][[i]][[1]][4],
                                      as.numeric(x[["geometry"]][[i]][[1]][4]) + 0.1, x[["geometry"]][[i]][[1]][4])
    x$geometry[[i]][[1]][2] <- ifelse(x[["geometry"]][[i]][[1]][1] == x[["geometry"]][[i]][[1]][2], 
                                      as.numeric(x[["geometry"]][[i]][[1]][2]) + 0.1, x[["geometry"]][[i]][[1]][2])
    # y <- x
  }
  return(x)
}
upper_thres <- 28


# break up the multi-linestrings and then cast to points and then linestring ------------------------------
my_s24 <- s24_assetbase0_int
# my_s24_points <- my_s24 %>%
#   mutate(s24_id = row_number()) %>%
#   select(s24_id,geometry) %>%
#   group_by(s24_id) %>% st_cast("MULTIPOINT") %>%
#   as.data.frame() %>% st_sf() %>%
#   # prep for linestring
#   mutate(s24_id_ls = row_number()) %>%
#   ungroup() %>%
#   group_by(s24_id_ls) %>%
#   st_cast("LINESTRING") %>%
#   as.data.frame()

# break up the multilinestrings and then cast to linestring 
my_s24_lines <- my_s24 %>% 
  mutate(s24_id = row_number()) %>% 
  select(s24_id,geometry) %>% 
  # prep for linestring
  group_by(s24_id) %>% st_cast("LINESTRING") %>% 
  as.data.frame() %>% st_sf() %>% 
  mutate(s24_id_ls = row_number()) %>% 
  as.data.frame()

my_newpoint <- my_s24_lines %>% st_sf() %>% st_set_crs(27700)
# my_newpoint %>% st_write("D:/my_newpoint.shp")

# start parallellising from here
ncore <- 2
cl    <- makeCluster(ncore) 
registerDoParallel(cl, cores = ncore)

# break the linestrings further

my_newpoint_list <- create_lists(my_newpoint)

tic()
my_newlines <- foreach(df_list = iter(my_newpoint_list),
                       .packages = c('dplyr', 'sp', 'sf', 'polylineSplitter'), .errorhandling='stop') %dopar%
  break_to_smallest(broken_geom_2 = df_list)
my_newlines <- plyr::rbind.fill(my_newlines)
my_newlines <- my_newlines[!duplicated(my_newlines$geometry),]
toc()

# my_newlines %>% st_write("D:/my_newlines.shp")

# my_newlines <- divide_and_conquer_2(my_newpoint,your_func = break_to_smallest)

# save.image("D:/STW PDAS/RDATA/segment_lines.rdata")
#load("D:/STW PDAS/RDATA/segment_lines.rdata")

# rules
# 5.5,17.5 pdas
# 9.9,27.9 s24

# merge up smaller linestrings separately and combine with the ones that are not smaller linestrings -----------------------------
my_newlines1 <- my_newlines %>% mutate(s24_id_ls_remove = row_number())
# my_newlines_msl <- my_newlines1 %>% as.data.frame() %>% 
#   filter(length_m < 0.06 , length_m > 0.045) %>% 
#   st_sf() %>% #st_set_crs(27700) %>%
#   ungroup() %>% 
#   group_by(s24_id_ls) %>% 
#   summarise(length_m = sum(length_m),s24_id = unique(s24_id)) %>% 
#   st_cast("MULTILINESTRING")
# my_newlines_non_msl <- my_newlines1 %>% 
#   filter(length_m >= 0.06 | length_m <= 0.045) %>% 
#   select(-s24_id_ls_remove) %>% 
#   #anti_join(my_newlines_msl, by = c("s24_id_ls_remove")) %>% 
#   st_sf() %>% 
#   st_cast("MULTILINESTRING")

#my_newlines_collated <- rbind(my_newlines_msl,my_newlines_non_msl)
my_newlines_collated <- my_newlines1 %>% st_sf() %>% st_cast("MULTILINESTRING")
# my_newlines_collated %>% st_sf() %>% st_set_crs(27700) %>% st_write("D:/processed4.shp")

my_newlines2 <- my_newlines_collated %>% st_sf() %>% st_set_crs(27700) %>% 
  mutate(s24_id_ls_2 = row_number())
new_s24 <- split_lines_using_rules_2(my_newlines2)
new_s24_2 <- new_s24 %>% 
  mutate(s24_id_ls_2 = row_number()) %>%
  # filter(splits_in_meters > 0.01) %>% 
  left_join(my_newlines2[,"s24_id_ls_2"], by = "s24_id_ls_2" )


# split by the specified meters --------------------------------------------------------------

# new_s24_3 <- new_s24_2 %>% left_join(my_s24_mergedlines_2[,"s24_id"],by = "s24_id") %>% 
#   st_sf() %>% st_set_crs(27700)
# new_s24_3 <- new_s24_2[!duplicated(new_s24_2$s24_id), ]
new_s24_3 <- new_s24_2 %>% 
  as.data.frame() %>% 
  # select(-geometry) %>% 
  mutate(s24_id_ls_2 = row_number()) %>% 
  # left_join(my_newlines2[,"s24_id_ls_2"], by = "s24_id_ls_2") %>% 
  st_sf() %>% st_set_crs(27700)
new_s24_3$length_m <- st_length(new_s24_3$geometry)

# convert the units to 4d.p numeric
new_s24_3 <- new_s24_3 %>% mutate(length_m = round(as.numeric(length_m),5))

# fix ymax in the below with length > 28
new_s24_3_upper_thres <- new_s24_3 %>% filter(length_m > upper_thres)
fixed_xymax <- fix_xymax(new_s24_3_upper_thres)
fixed_xymax %>% st_write("D://fixed_xymax.shp")

# delete 0 length assets
something <- split_my_linestrings_2(fixed_xymax,"splits_in_meters",messaging = 1)


# parallellise the above ------------------------------------------------------------->
tic()
new_s24_3_list <- create_lists(fixed_xymax)
something <- foreach(df_list = iter(new_s24_3_list),
                     .packages = c('dplyr', 'sp', 'sf', 'polylineSplitter'), .errorhandling='stop') %dopar%
  
  split_my_linestrings_3(x = df_list,
                         meter_column = "splits_in_meters",
                         messaging = 1)
toc()


# write those less than Q3
fixed_xymax_0_upper_thres <- new_s24_3 %>% filter(length_m >= 0,length_m <= upper_thres) %>% 
  # st_sf() %>% #st_set_crs(27700) %>%
  # ungroup() %>%
  # group_by(s24_id_ls) %>%
  # summarise(length_m = sum(length_m),s24_id = unique(s24_id)) %>%
  # st_cast("MULTILINESTRING") %>%
  st_sf() %>% st_set_crs(27700) %>% 
  rename(new_length = length_m) %>% 
  select(s24_id, geometry, new_length)

# fixed_xymax_0_1 <- fixed_xymax %>% filter(length_m < 0.1) %>% 
#   st_sf() %>% #st_set_crs(27700) %>%
#   ungroup() %>%
#   group_by(s24_id_ls) %>%
#   summarise(length_m = sum(length_m),s24_id = unique(s24_id)) %>%
#   st_cast("MULTILINESTRING") %>% 
#   rename(new_length = length_m) %>% 
#   select(s24_id, geometry, new_length) %>% 
#   filter(new_length != 0)

my_columns <- c("s24_id","Confdnc","DIAMETE","MATERIA","YEARLAI")
atts <- my_s24 %>% 
  mutate(s24_id = row_number()) %>% as.data.frame()
something2 <- plyr::rbind.fill(something) %>% select(-id_0) %>% st_sf() %>% st_set_crs(27700)
rbind(something2,fixed_xymax_0_upper_thres) %>%
  left_join(atts[,my_columns], by = "s24_id") %>% 
  st_write("D:/merged_s24_da.shp")
# the end ---------------------------------------------------------------->
