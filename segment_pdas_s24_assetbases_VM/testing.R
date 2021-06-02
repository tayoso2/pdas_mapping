combine_shapefiles <- function(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/"){
  for(x in 1:length(list.files(folder))){
#  for(x in 1:1){
    folder_in <- list.files(folder)
    #folder_in_folder <- list.files(paste0(folder,folder_in[5]))
    file_list <- list.files(paste0(folder,folder_in[x]),pattern = ".shp")
    # file_list_batch_one <- file_list[0:round(length(file_list)/2,0)]
    # file_list_batch_two <- file_list[(round(length(file_list)/2) + 1):
    #                                    round(length(file_list),0)]
    
    for(y in 1:5){
      my_split <- rep(1:5,each = 1, length = length(file_list))
      data = file_list[my_split==y]
      data = paste0(folder,folder_in[x],"/",data)
      output = Reduce(plyr::rbind.fill, lapply(data, st_read))
      columns = c("fid","geometry","descriptiv","physicalLe","accuracyOf")
      output1 = output[,columns]
      output_merged <- as.data.table(output1)
      output_merged <- output_merged[output_merged$descriptiv == "Building"]
      if (y == 1) {
        output.final <- output_merged
      } else{
        output.final <- plyr::rbind.fill(output.final, output_merged)
      }
      return(output.final)
    }
    if (x == 1) {
      final.out <- output.final
    } else{
      final.out <- plyr::rbind.fill(final.out,output.final)
    }
    return(final.out)
  }
}

combined <- combine_shapefiles(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/")



combine_shapefiles <- function(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/"){
  for(x in 1:length(list.files(folder))){
    #  for(x in 1:1){
    folder_in <- list.files(folder)
    #folder_in_folder <- list.files(paste0(folder,folder_in[5]))
    file_list <- list.files(paste0(folder,folder_in[x]),pattern = ".shp")
    # file_list_batch_one <- file_list[0:round(length(file_list)/2,0)]
    # file_list_batch_two <- file_list[(round(length(file_list)/2) + 1):
    #                                    round(length(file_list),0)]
    
    if (x == 1) {
      final.out <- file_list
    } else{
      final.out <- plyr::rbind.fill(final.out,file_list)
    }
    return(final.out)
  }
}

combined <- combine_shapefiles(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/")
combined
