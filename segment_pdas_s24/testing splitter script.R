

fixed_xymax_test <- fixed_xymax[726,]
tut <- fix_xymax(fixed_xymax_test)

tutu <- split_my_linestrings_2(x = tut,
                       meter_column = "splits_in_meters",
                       messaging = 1)


fixed_xymax[726,]
fixed_xymax[726,] %>% st_length()
fixed_xymax[726,"geometry"][[1]][[1]] 
fixed_xymax[["geometry"]][[726]][[1]][1] <- as.numeric(fixed_xymax[["geometry"]][[726]][[1]][1]) - 0.1
tmap_mode("view")
qtm(tut)


round(as.numeric(fixed_xymax[["geometry"]][[726]][[1]][1]),1) == round(as.numeric(fixed_xymax[["geometry"]][[726]][[1]][2]),1)

fixed_xymax[725,"geometry"][[1]][[1]]



tutul <- divide_and_conquer_2(new_pipe_3_upper_thres,fix_xymax)
tic()
tutul_list <- create_lists(new_pipe_3_upper_thres[1:50000,])
tutul <- foreach(df_list = iter(tutul_list),
                     .packages = c('dplyr', 'sp', 'sf'), .errorhandling='stop') %dopar%
  
  divide_and_conquer_2(df_list,fix_xymax)
tutul_bind <- plyr::rbind.fill(tutul)
toc()

tutula <- split_my_linestrings_2(x = tutul_bind,
                               meter_column = "splits_in_meters",
                               messaging = 1)
