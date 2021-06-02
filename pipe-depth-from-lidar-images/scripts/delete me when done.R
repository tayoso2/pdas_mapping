

# load("D:/STW PDAS/RDATA/pipe_direction_2.rdata")


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
my_func_2 <- function(x = x,y = y){
  for(i in 1:length(x)){
    message(paste0("Grid:", i))
    listofgrids_1 <- x %>% plyr::rbind.fill() %>% st_sf() %>%
      as.data.frame() %>%
      dplyr::select(-geometry) %>%
      filter(is.na(bearing)) %>%
      mutate_all(as.character) %>%
      mutate(bearing = as.numeric(bearing))
    listofgrids_2 <- y %>% plyr::rbind.fill() %>% st_sf() %>%
      as.data.frame() %>%
      dplyr::select(-geometry) %>%
      filter(!is.na(bearing)) %>% 
      mutate_all(as.character) %>%
      mutate(bearing = as.numeric(bearing)) 
    final_null_bearings <- get_start_end_points_for_zero_elev(listofgrids_1,listofgrids_2)#not_null_bearings_nogeom)
    final_null_bearings <- plyr::rbind.fill(final_null_bearings)
    if (i == 1) {
      final.out <- final_null_bearings
    } else{
      final.out <- plyr::rbind.fill(final.out, final_null_bearings)
    }
  }
  return(final.out)
}
create_lists <- function(x, splits = 29,each = 1){
  my_split <- base::rep(1:splits,each = each, length = nrow(x))
  my_split2 <- dplyr::mutate(x, my_split = my_split)
  list_lists <- base::split(my_split2,my_split2$my_split) 
  #list_lists <- purrr::map(my_split3$data, ~ split(., func_arg)) 
  return(list_lists)
}

# starts here -------------------------------------------------------------------
first_list <- plyr::rbind.fill(listofgrids_unknown_1[[1]])
second_list <- plyr::rbind.fill(listofgrids_unknown_1[[2]])

# determine number of cores to use
ncore <- 1
cl    <- makeCluster(ncore) 
registerDoParallel(cl, cores = ncore)


tic()
take_this_list_first <- split(first_list,first_list$ActualX)
take_this_list_second <- split(second_list,second_list$ActualX)
something <- foreach(df_list_1 = iter(take_this_list_first),df_list_2 = iter(take_this_list_second),
                     .packages = c('plyr','dplyr', 'sp', 'sf'), .errorhandling='stop') %dopar%
  my_func_2(df_list_1,df_list_2)
toc()






sugar = take_this_list_first[1:3]
nrow(sugar)
sugar[1] %>% length()
