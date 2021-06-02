
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

pts_sf0 <- data.frame(
  x = c(450389.6,450414),
  y = c(275412,275412),
  id = 1,
  id_2= c(1,2)
) %>% 
  # st_point() %>% 
  # st_geometry() %>% 
  st_as_sf(coords = c("x","y")) %>%  
  st_set_crs(27700) %>% 
  # convert points to linestrings
  group_by(id) %>% 
  summarise(id2 = sum(id_2)) %>% 
  st_cast("MULTILINESTRING")

pts_sf1 <- data.frame(
  x = c(345341.1,345341.1),
  y = c(313174.4,313174.1),
  id = 1,
  id_2= c(1,2)
) %>% 
  # st_point() %>% 
  # st_geometry() %>% 
  st_as_sf(coords = c("x","y")) %>%  
  st_set_crs(27700) %>% 
  # convert points to linestrings
  group_by(id) %>% 
  summarise(id2 = sum(id_2)) %>% 
  st_cast("MULTILINESTRING")


# apply fix_xymax
fix_xymax(pts_sf0)
fix_xymax(pts_sf1)


# from here
ifelse(st_bbox(pts_sf)$ymin == st_bbox(pts_sf)$ymax, round(as.numeric(st_bbox(pts_sf)$ymin),4) ,st_bbox(pts_sf)$ymin)
df_ymin <- round(as.numeric(st_bbox(pts_sf)$ymin),4) + (0.1)
new_pts_sf <- pts_sf
ifelse(st_bbox(pts_sf)$ymin == st_bbox(pts_sf)$ymax, df_ymin ,st_bbox(pts_sf)$ymin)


split_my_linestrings(pts_sf,2)

pts_sf[,"geometry"][[1]][[1]]

# add 0.1 to linestring with ymax and ymin being the same
new_pts_sf <- pts_sf
fix_xymax <- function(x, messaging = 1){
  for (i in 1:nrow(x)) {
  # message detailing progress
  if(i %% messaging == 0){message(paste0("row number ",i))}
    x[["geometry"]][[i]][[1]][[4]] <- ifelse(x[["geometry"]][[i]][[1]][[3]] == x[["geometry"]][[i]][[1]][[4]], 
                                             x[["geometry"]][[i]][[1]][[4]] + 0.1,x[["geometry"]][[i]][[1]][[4]])
    x[["geometry"]][[i]][[1]][[2]] <- ifelse(x[["geometry"]][[i]][[1]][[1]] == x[["geometry"]][[i]][[1]][[2]], 
                                             x[["geometry"]][[i]][[1]][[2]] + 0.1,x[["geometry"]][[i]][[1]][[2]])
    }
    return(x)
}

fix_ymax(new_pts_sf)


x = fixed_xymax
create_lists <- function(x, splits = 29,each = 1){
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- mutate(x, my_split = my_split)
  list_lists <- split(my_split2,my_split2$my_split) 
  #list_lists <- purrr::map(my_split3$data, ~ split(., func_arg)) 
  return(list_lists)
}

create_lists(fixed_xymax)






fold_out <- "D:\\STW PDAS\\Output"
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
    my_time <- as.integer(runif(1)*100000)
    
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
tic()
ncore <- 3
cl    <- makeCluster(ncore) 
registerDoParallel(cl, cores = ncore)
something <- foreach(df_list = iter(new_s24_3_list),
                     .packages = c('dplyr', 'sp', 'sf', 'polylineSplitter'), .errorhandling='stop') %dopar%
  
  split_my_linestrings_3(x = df_list,
                         meter_column = "splits_in_meters",
                         messaging = 1)

toc()





### test why 1186 does not work ----------------------------------------------------------------
qtm(final_split1 %>% filter(new_length < 1) %>%  st_sf() %>% st_set_crs(27700), basemaps = "Esri.WorldStreetMap")
final_split1 %>% filter(new_length < 1) %>% arrange(new_length)
### test ends

new_s24_3 %>% select(-splits_in_meters) %>% st_sf() %>% st_set_crs(27700) %>% st_write("D:/processed5.shp")


foreach(df_list = iter(new_s24_3_list),
        .packages = c('dplyr', 'sp', 'sf', 'polylineSplitter'), .errorhandling='stop') %dopar%
  print(i)



something %>% plyr::rbind.fill() %>% summary()


# 450192.86,275743.93 # scale 1:500
# 451170.3,274988.8
