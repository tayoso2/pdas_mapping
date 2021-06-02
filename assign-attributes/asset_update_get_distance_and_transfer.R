

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
library(ggplot2)


# set wd
setwd("D:/STW PDAS/Phase 2 datasets/descriptive stats/")

# read rds to use later
fiddle_pdas <- readRDS("fiddle_pdas.rds")
fiddle_s24 <- readRDS("fiddle_s24.rds")

# select the necessary columns
cols = c("fid", "propgroup", "pip_q_d", "geometry.x", "geometry.y")

# tic()
# my_ans <- get_one_to_one_distance(fiddle_pdas[1:1000,],fiddle_pdas[1:1000,],"geometry.x","geometry.y")
# my_ans
# toc()
# 
# lwgeom::st_geod_distance
# 
# fiddle_pdas_geom_y <- fiddle_pdas
# st_geometry(fiddle_pdas_geom_y) <- "geometry.y"
# 
# tic()
# fiddle_test <- fiddle_pdas[1:1000,]
# for (i in 1:nrow(fiddle_test)){
#   fiddle_test$length_to_centroid[[i]] <- as.numeric(st_distance(fiddle_test[["geometry.x"]][i], fiddle_test[["geometry.y"]][i]))
# }
# fiddle_test$length_to_centroid %>% summary()
# toc()
# # as.numeric(st_distance(fiddle_pdas[["geometry.x"]][1:100], fiddle_pdas[["geometry.y"]][1:100]))


# Functions -----------------------------------------------------------------

divide_and_conquer <- function(x,your_func = your_func, func_arg = func_arg,col1, col2, splits = 50,each = 1){
  # Description: Convert a dataframe to a list of dataframes and then call a function on each of them.
  #   This is to aid faster processing in R
  # x is a typically a dataframe or vector
  # your_func is the function you wish to call on each list of dataframe
  # func_arg is the argument in the function you wish to add e.g. sum(x,y,...), with sum being your_func and y the func_arg
  # splits is the number of lists you want generated
  # each is a non-negative integer. Each element of x is repeated each times.
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  my_split3 <- tidyr::nest(my_split2, - my_split) 
  models <- purrr::map(my_split3$data, ~ your_func(., func_arg, col1, col2)) 
  bind_lists <- plyr::rbind.fill(models)
  return(bind_lists)
}
create_lists <- function(x, splits = 50, each = 1) {
  # Description: This function will aid faster processing especially with for loop functions
  # x is the dataframe to split into lists
  # splits is the number of lists
  my_split <- base::rep(1:splits, each = each, length = nrow(x))
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  list_lists <- base::split(my_split2, my_split2$my_split)
  # list_lists <- purrr::map(my_split3$data, ~ split(., func_arg))
  return(list_lists)
}
create_nested_lists <- function(x, splits = 20, nested_split = 5, each = 1) {
  # Description: This function will aid faster processing especially with for loop functions
  # x is the dataframe to split into lists
  # splits is the number of lists
  # nested_split is the number of lists in each "splits" list
  my_split <- base::rep(1:splits, each = each, length = nrow(x))
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  list_lists <- base::split(my_split2, my_split2$my_split)
  
  for (i in 1:length(list_lists)){
    my_split_nested <- base::rep(1:nested_split, each = each, length = nrow(list_lists[[i]]))
    my_split2_nested <- dplyr::mutate(list_lists[[i]], my_split_nested = my_split_nested)
    nested_lists <- base::split(my_split2_nested, my_split2_nested$my_split_nested)
    if (i == 1) {
      output_final <- nested_lists
    } else {
      output_final <- rbind(output_final, nested_lists)
    }
  }
  return(as_tibble(output_final))
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
  
  # Transform x CRS to EPSG 4326 if required
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
get_the_distance_with_nested_lists <- function(x, x_geom, y_geom) {
  # x is the data with the x geometry as the active geom
  # x_geom is the geometry of the object on the lhs
  # y_geom is the geometry of the object on the rhs
  for (h in 1:length(x)){
    message(paste0("Superlist number ", h))
    x_0 <- x[[h]]
    for (i in 1:length(x_0)){
      message(paste0("List number ", i))
      x_1 <- x_0[[i]]
      y_1 <- x_0[[i]]
      x_2 <- get_one_to_one_distance(x_1,y_1,x_geom,y_geom)
      if (i == 1) {
        output_final <- x_2
      } else {
        output_final <- plyr::rbind.fill(output_final, x_2)
      }
    }
    if (h == 1) {
      final_out <- output_final
    } else{
      final_out <- plyr::rbind.fill(final_out,output_final)
    }
  }
  return(final_out)
}

# PDaS -----------------------------------------------------------------------

# make sure st is longlat is TRUE
st_is_longlat(fiddle_pdas)
fiddle_pdas_4326 <- st_transform(fiddle_pdas, crs = 4326)
st_is_longlat(fiddle_pdas_4326)

# convert geom_y to y
fiddle_pdas_4326 <- fiddle_pdas_4326 %>% st_centroid()

# calculate the distance
tic()
pdas_got_distance <- get_one_to_one_distance(fiddle_pdas_4326[,cols],
                                                        "geometry.x",
                                                        "geometry.y",
                                                        splits = 200,
                                                        centroid = TRUE)

toc()

# remove dup fids if any
pdas_got_distance <- pdas_got_distance[!duplicated(pdas_got_distance$fid), ]
fiddle_pdas <- fiddle_pdas[!duplicated(fiddle_pdas$fid), ]

# transform crs of pdas_got_distance to 27700
pdas_got_distance <- pdas_got_distance %>% 
  st_sf() %>% 
  st_transform(crs = 27700) %>% 
  as.data.frame()

# merge the other cols back
fiddle_pdas_all <- fiddle_pdas %>% 
  dplyr::rename(geometry = geometry.x) %>% 
  dplyr::select(fid, P_Type, MATERIA, DIAMETE, YEARLAI, AT_YR_C, County, length) %>% 
  as.data.frame() %>% 
  left_join(pdas_got_distance, by = "fid") %>% 
  dplyr::select(-c(geometry.x,geometry.y,my_split)) %>% 
  dplyr::rename(pipe_length = length) %>% 
  st_sf() %>% 
  # add Transfer column
  mutate(Transfer = "PDaS")

# check distance summary
fiddle_pdas_all$length_to_centroid %>% summary()


# Remove outliers -------------------------------------------------------------------------

set.seed(101)
fiddle_pdas_all_samp <- sample(fiddle_pdas_all$length_to_centroid,100000, replace = FALSE) %>% as.data.table()
ggplot(fiddle_pdas_all_samp, aes(x = .)) +
  geom_density()

# exclude outliers using IQR
iqr_p <- IQR(fiddle_pdas_all$length_to_centroid)
q1_p <- summary(fiddle_pdas_all$length_to_centroid)[2]
q3_p <- summary(fiddle_pdas_all$length_to_centroid)[5]
lower_inner_bound_p <- q1_p - (1.5*iqr_p)
upper_inner_bound_p <- q3_p + (1.5*iqr_p)
lower_outer_bound_p <- q1_p - (3*iqr_p)
upper_outer_bound_p <- q3_p + (3*iqr_p)
fiddle_pdas_all_no_outliers = fiddle_pdas_all %>% filter(length_to_centroid > lower_inner_bound_p & length_to_centroid < upper_inner_bound_p) 
## used 16.75m distance from centroid

# tag outliers as unknown i.e change the MATERIA DIAMETE YEARLAI AT_YR_C pipe_length pip_q_d length_to_centroid
unknown_col = c("MATERIA", "DIAMETE", "YEARLAI", "AT_YR_C", "pipe_length", "pip_q_d", "length_to_centroid")
unknown_func = function(x, na.rm = FALSE) (x = "unknown")
fiddle_pdas_all_outliers <- fiddle_pdas_all %>% 
  as_tibble() %>% 
  anti_join(fiddle_pdas_all_no_outliers, by = "fid") %>% 
  mutate_at(unknown_col,unknown_func) %>% 
  st_sf()

# merge outliers and non_outliers back
fiddle_pdas_all_final <- rbind(fiddle_pdas_all_outliers,fiddle_pdas_all_no_outliers)

# save the latest output
saveRDS(pdas_got_distance,"pdas_got_distance.rds")


# S24 --------------------------------------------------------------------

# make sure st is longlat is TRUE
st_is_longlat(fiddle_s24)
fiddle_s24_4326 <- st_transform(fiddle_s24, crs = 4326)
st_is_longlat(fiddle_s24_4326)

# convert geom_y to y
fiddle_s24_4326 <- fiddle_s24_4326 %>% st_centroid()

# calculate the distance
tic()
s24_got_distance <- get_one_to_one_distance(fiddle_s24_4326[,cols],
                                             "geometry.x",
                                             "geometry.y",
                                             splits = 100,
                                             centroid = TRUE)

toc()

# remove dup fids if any
s24_got_distance <- s24_got_distance[!duplicated(s24_got_distance$fid), ]
fiddle_s24 <- fiddle_s24[!duplicated(fiddle_s24$fid), ]

# transform crs of s24_got_distance to 27700
s24_got_distance <- s24_got_distance %>% 
  st_sf() %>% 
  st_transform(crs = 27700) %>% 
  as.data.frame()

# merge the other cols back
fiddle_s24_all <- fiddle_s24 %>% 
  dplyr::rename(geometry = geometry.x) %>% 
  dplyr::select(fid, P_Type, MATERIA, DIAMETE, YEARLAI, AT_YR_C, County, length) %>% 
  as.data.frame() %>% 
  left_join(s24_got_distance, by = "fid") %>% 
  dplyr::select(-c(geometry.x,geometry.y,my_split)) %>% 
  dplyr::rename(pipe_length = length) %>% 
  st_sf() %>% 
  # add Transfer column
  mutate(Transfer = "S24")

# check distance summary
fiddle_s24_all$length_to_centroid %>% summary()
fiddle_s24_all %>% arrange(desc(length_to_centroid))

# Remove outliers -------------------------------------------------------------------------

set.seed(101)
fiddle_s24_all_samp <- sample(fiddle_s24_all$length_to_centroid,100000, replace = FALSE) %>% as.data.table()
ggplot(fiddle_s24_all_samp, aes(x = .)) +
  geom_density()

# exclude outliers using IQR
iqr_p <- IQR(fiddle_s24_all$length_to_centroid)
q1_p <- summary(fiddle_s24_all$length_to_centroid)[2]
q3_p <- summary(fiddle_s24_all$length_to_centroid)[5]
lower_inner_bound_p <- q1_p - (1.5*iqr_p)
upper_inner_bound_p <- q3_p + (1.5*iqr_p)
lower_outer_bound_p <- q1_p - (3*iqr_p)
upper_outer_bound_p <- q3_p + (3*iqr_p)
fiddle_s24_all_no_outliers = fiddle_s24_all %>% filter(length_to_centroid > lower_inner_bound_p & length_to_centroid < upper_inner_bound_p)
## used 19.07m distance from centroid

# tag outliers as unknown i.e change the MATERIA DIAMETE YEARLAI AT_YR_C pipe_length pip_q_d length_to_centroid
unknown_col = c("MATERIA", "DIAMETE", "YEARLAI", "AT_YR_C", "pipe_length", "pip_q_d", "length_to_centroid")
unknown_func = function(x, na.rm = FALSE) (x = "unknown")
fiddle_s24_all_outliers <- fiddle_s24_all %>% 
  as_tibble() %>% 
  anti_join(fiddle_s24_all_no_outliers, by = "fid") %>% 
  mutate_at(unknown_col,unknown_func) %>% 
  st_sf()

# merge outliers and non_outliers back
fiddle_s24_all_final <- rbind(fiddle_s24_all_outliers,fiddle_s24_all_no_outliers)

# save the latest output
saveRDS(s24_got_distance,"s24_got_distance.rds")


# merge both pdas and s24 and save using counties
final_ans <-rbind(fiddle_pdas_all_final,fiddle_s24_all_final)

# save answers
final_ans <- final_ans %>%
  mutate(
    County = ifelse(
      County == "Worcestershire/Gloucestershire",
      "Worcestershire_Gloucestershire",
      as.character(County)
    )
  )
final_ans_chunk <- split(final_ans, final_ans$County)
for (i in 1:length(final_ans_chunk)) {
  st_write(
    final_ans_chunk[[i]],
    paste0(
      "D:/STW PDAS/Phase 2 datasets/Flag overlap/03_03_2021_chunks/",
      "props_chunk_",
      as.data.frame(final_ans_chunk[[i]][1, "County"])[[1]],
      ".shp"
    )
  )
}

# # Parallelisation
# library(parallel)
# library(doParallel)
# library(foreach)
# 
# # determine number of cores to use
# ncore <- 2
# cl    <- makeCluster(ncore)
# registerDoParallel(cl, cores = ncore)
# 
# something <- foreach(df_list = iter(fiddle_pdas_4326),
#                      .packages = c('dplyr', 'sp', 'sf'), .errorhandling='stop') %dopar%
#   split_my_linestrings_2(x = df_list[,cols],
#                          "geometry.x",
#                          "geometry.y",
#                          splits = 100,
#                          centroid = FALSE)


# end 2 -----------------------------------------

