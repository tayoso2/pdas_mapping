library(multiplex)
library(rgdal)
library(sf)
library(dplyr)
library(magrittr)
library(foreign)
library(data.table)

# merger = read.gml("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/test/4640449-000700-30.gml", as = "array",coords = TRUE)
# merger = read.graph("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/test/4640449-000700-30.gml",format=c("gml"))
# 
# 
# merger = readOGR(dsn="C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/test/4640449-000700-30",layer = "GML")
# 
# 
# 
# one = st_read("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output/out1.shp")
# two = st_read("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output/out2.shp")
# three = st_read("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output/out3.shp")
# four = st_read("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output/out4.shp")
# five = st_read("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output/out5.shp")
# six = st_read("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output/out6.shp")
# plyr::rbind.fill(one,two,three, four, five,six)


setwd("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output")
file_list <- list.files(pattern = ".shp")
output1 <- Reduce(plyr::rbind.fill, lapply(file_list, st_read))
output_merged <- output1 %>% filter(descriptiv == "Building") 
first <- output_merged[c("fid","calculated","physicalLe")]
dim(first)

setwd("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output2")
file_list2 <- list.files(pattern = ".shp")
output2 <- Reduce(plyr::rbind.fill, lapply(file_list2, st_read))
output_merged2 <- output2 %>% filter(descriptiv == "Building") 
second <- output_merged2[c("fid","calculated","physicalLe")]
dim(second)


setwd("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output3")
file_list3 <- list.files(pattern = ".shp")
output3 <- Reduce(plyr::rbind.fill, lapply(file_list3, st_read))
output_merged3 <- output3 %>% filter(descriptiv == "Building") 
third <- output_merged3[c("fid","calculated","physicalLe")]
dim(third)

rm(output1)
rm(output2)
rm(output3)
rm(output_merged)
rm(output_merged2)
rm(output_merged3)

one_to_three <- rbind(first, second,third) 
one_to_three <- one_to_three %>% distinct()
setwd("C:/Users/TOsosanya/Downloads/")
#writeOGR(one_to_three, dsn=".", layer="merged_1_2_3", driver="ESRI Shapefile")
st_write(one_to_three[c("fid","calculated","physicalLe")],"C:/Users/TOsosanya/Downloads/merged_1_2_3.shp")

rm(first)
rm(second)
rm(third)
rm(one_to_three)

setwd("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output4")
file_list4 <- list.files(pattern = ".shp")
output4 <- Reduce(plyr::rbind.fill, lapply(file_list4, st_read))
output_merged4 <- output4 %>% filter(descriptiv == "Building") 
four <- output_merged4[c("fid","calculated","physicalLe")]
dim(four)

setwd("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output5")
file_list5 <- list.files(pattern = ".shp")
output5 <- Reduce(plyr::rbind.fill, lapply(file_list5, st_read))
output_merged5 <- output5 %>% filter(descriptiv == "Building") 
five <- output_merged5[c("fid","calculated","physicalLe")]
dim(five)

setwd("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output6")
file_list6 <- list.files(pattern = ".shp")
output6 <- Reduce(plyr::rbind.fill, lapply(file_list6, st_read))
output_merged6 <- output6 %>% filter(descriptiv == "Building") 
six <- output_merged6[c("fid","calculated","physicalLe")]
dim(six)

rm(output4)
rm(output5)
rm(output6)
rm(output_merged4)
rm(output_merged5)
rm(output_merged6)

four_to_six <- rbind(four,five,six) 
#four_to_six <- four_to_six[c("fid","calculated","physicalLe")] %>% distinct()
setwd("C:/Users/TOsosanya/Downloads/")
#writeOGR(one_to_three, dsn=".", layer="merged_1_2_3", driver="ESRI Shapefile")
st_write(four_to_six,"C:/Users/TOsosanya/Downloads/merged_4_5_6.shp")

rm(list=ls())

setwd("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output7")
file_list7 <- list.files(pattern = ".shp")
output7 <- Reduce(plyr::rbind.fill, lapply(file_list7, st_read))
output_merged7 <- output7 %>% filter(descriptiv == "Building") 
seven <- output_merged7[c("fid","calculated","physicalLe")]
dim(seven)

setwd("C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/output8")
file_list8 <- list.files(pattern = ".shp")
output8 <- Reduce(plyr::rbind.fill, lapply(file_list8, st_read))
output_merged8 <- output8 %>% filter(descriptiv == "Building") 
eight <- output_merged8[c("fid","calculated","physicalLe")]
dim(eight)

eight_tibble <- eight %>% as_tibble() %>% distinct()
eight_tibble_to_df <- eight_tibble[,1:3] %>% as.data.frame()

eight_geom <- eight %>% distinct()

eight_geom_data <- cbind(eight_tibble_to_df,eight_geom)

output8$geometry %>% unique()

# ------------------------------------------------------------------------------



combine_shapefiles <- function(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/"){
  #  for(x in 1:length(list.files(folder))){
  for(x in 1:2){
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
    }
    if (x == 1) {
      final.out <- output.final
    } else{
      final.out <- plyr::rbind.fill(final.out,output.final)
    }
  }
  return(final.out)
}

combined_1_2 <- combine_shapefiles(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/")
combined_1_2 <- st_sf(combined_1_2)
st_crs(combined_1_2) <- 4326
# combined_1_2 <- combined_1_2[!st_is_empty(combined_1_2),,drop=FALSE]
# st_write(combined_1_2,"C:/Users/TOsosanya/Downloads/combined_1_2.csv")
saveRDS(combined_1_2,"C:/Users/TOsosanya/Downloads/combined_1_2.rds")





#### ----------------------------------------

combine_shapefiles <- function(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/"){
  #  for(x in 1:length(list.files(folder))){
  for(x in 3:4){
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
    }
    if (x == 3) {
      final.out <- output.final
    } else{
      final.out <- plyr::rbind.fill(final.out,output.final)
    }
  }
  return(final.out)
}

combined_3_4 <- combine_shapefiles(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/")
combined_3_4 <- st_sf(combined_3_4)
st_crs(combined_3_4) <- 4326
# combined_3_4 <- combined_3_4[!st_is_empty(combined_3_4),,drop=FALSE]
# st_write(combined_3_4,"C:/Users/TOsosanya/Downloads/combined_3_4.csv")
saveRDS(combined_3_4,"C:/Users/TOsosanya/Downloads/combined_3_4.rds")

combine_shapefiles <- function(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/"){
  #  for(x in 1:length(list.files(folder))){
  for(x in 1:2){
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
    }
    if (x == 1) {
      final.out <- output.final
    } else{
      final.out <- plyr::rbind.fill(final.out,output.final)
    }
  }
  return(final.out)
}

combined_1_2 <- combine_shapefiles(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/")
combined_1_2 <- st_sf(combined_1_2)
st_crs(combined_1_2) <- 4326
# combined_1_2 <- combined_1_2[!st_is_empty(combined_1_2),,drop=FALSE]
# st_write(combined_1_2,"C:/Users/TOsosanya/Downloads/combined_1_2.csv")
saveRDS(combined_1_2,"C:/Users/TOsosanya/Downloads/combined_1_2.rds")





#### ----------------------------------------

combine_shapefiles <- function(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/"){
  #  for(x in 1:length(list.files(folder))){
  for(x in 5:6){
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
    }
    if (x == 5) {
      final.out <- output.final
    } else{
      final.out <- plyr::rbind.fill(final.out,output.final)
    }
  }
  return(final.out)
}

combined_5_6 <- combine_shapefiles(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/")
combined_5_6 <- st_sf(combined_5_6)
st_crs(combined_5_6) <- 4326
# combined_5_6 <- combined_5_6[!st_is_empty(combined_5_6),,drop=FALSE]
# st_write(combined_5_6,"C:/Users/TOsosanya/Downloads/combined_5_6.csv")
saveRDS(combined_5_6,"C:/Users/TOsosanya/Downloads/combined_5_6.rds")


#### ----------------------------------------

combine_shapefiles <- function(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/"){
  #  for(x in 1:length(list.files(folder))){
  for(x in 7:8){
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
    }
    if (x == 7) {
      final.out <- output.final
    } else{
      final.out <- plyr::rbind.fill(final.out,output.final)
    }
  }
  return(final.out)
}

combined_7_8 <- combine_shapefiles(folder = "C:/Users/TOsosanya/Downloads/mm_nongeochunked_4640449_FULL/")
combined_7_8 <- st_sf(combined_7_8)
st_crs(combined_7_8) <- 4326
# combined_7_8 <- combined_7_8[!st_is_empty(combined_7_8),,drop=FALSE]
st_write(combined_7_8,"C:/Users/TOsosanya/Downloads/combined_7_8.shp")
saveRDS(combined_7_8,"C:/Users/TOsosanya/Downloads/combined_7_8.rds")
