
# Packages ----------------------------------------------------------------

library(party)
library(dplyr)
library(sf)
library(tmap)
library(rgdal)
library(tictoc)
library(tidyr)


# Predict attributes of proxy-road-sewer
# Look at the attributes of the nearest neighbour sewer main
# + could maybe use area/location as predictor
# + could maybe use nearby property ages as indicators of yearlaid (should do some analysis to see if there are existing trends in this)
# add length bands


# read building age data, year_lookup and rdata ----------------------------------------------------------------





# add the diameter and material of the cloest pipe to another using "mapped_sewer_bd_age_length-------------------------------------------------------------
mapped_sewer_bd_age_length_geom <- mapped_sewer_bd_age_length %>% 
  select(Tag,Bnd_M) %>%
  distinct() %>% 
  left_join(sewer[,"Tag"], by = "Tag")
mapped_sewer_bd_age_length_geom <- st_as_sf(mapped_sewer_bd_age_length_geom)
st_crs(mapped_sewer_bd_age_length_geom) <- 27700


mapped_sewer_bd_age_length %>% dim()
mapped_sewer_bd_age_length$Tag %>% unique()

st_crs(mapped_sewer_bd_age_length) <- 27700
mapped_sewer_bd_age_length <- as.data.frame(mapped_sewer_bd_age_length)
get_nearest_features_nomessage <- function(x, y, pipeid, id, feature) {
  
  # pipeid is id of x
  # id is id of y
  # feature is attribute to be added to x from y
  # function needs some improvement
  
  # Determine CRSs
  message(paste0(i))
  
  
  # Transform y CRS to x CRS if required
  if (st_crs(x) != st_crs(y)) {
    message(paste0(
      "Transforming y coordinate reference system to ESPG: ",
      st_crs(x)$epsg
    ))
    y <- st_transform(y, st_crs(x))
  }
  
  # Get rownumber of x
  x$rowguid <- seq.int(nrow(x))
  y$rowguid <- seq.int(nrow(y))
  
  # edit column names
  colnames(x)[which(names(x) == pipeid)] <- "id"
  colnames(y)[which(names(y) == id)] <- "id"
  colnames(y)[which(names(y) == feature)] <- "feature"
  
  # get centroid
  x <- st_centroid(x)
  y <- st_centroid(y)
  
  # get nearest features
  nearest <- st_nearest_feature(x,y)
  x$index <- nearest
  y2 <- y[,c("id", "feature", "rowguid")]
  st_geometry(y2) <- NULL
  x2 <- merge(x, as.data.frame(y2), by.x = "index", by.y = "rowguid")
  output <- x2
  
  # revert column names
  colnames(output)[which(names(output) == "id.x")] <- pipeid
  colnames(output)[which(names(output) == "id.y")] <- id
  colnames(output)[which(names(output) == "feature")] <- feature
  output <- output[,c(id)]
  st_geometry(output) <- NULL
  return(output)
}
st_nearest_feature_i <- function(x,y){
  # Determine CRSs
  message(paste0(i))
  # run the god damn thing
  output <- st_nearest_feature(x,y)
  }
FindNearest <- function(x, y, y.name = "y") {
  # Determine CRSs
  message(paste0("x Coordinate reference system is ESPG: ", st_crs(x)$epsg))
  message(paste0("y Coordinate reference system is ESPG: ", st_crs(y)$epsg))
  
  # Transform y CRS to x CRS if required
  if (st_crs(x) != st_crs(y)) {
    message(paste0(
      "Transforming y coordinate reference system to ESPG: ",
      st_crs(x)$epsg
    ))
    y <- st_transform(y, st_crs(x))
  }
  
  # Get rownumber of x
  x$rowguid <- seq.int(nrow(x))

  
  # Compute distance matrix
  dist.matrix <- st_distance(x, y)
  
  # Select y features which are shortest distance
  nearest.rows <- apply(dist.matrix, 1, which.min)
  # Determine shortest distances
  nearest.distance <-
    dist.matrix[cbind(seq(nearest.rows), nearest.rows)]
  
  # Report distance units
  distance.units <- deparse_unit(nearest.distance)
  message(paste0("Distance unit is: ", distance.units))
  
  # Build data table of nearest features and add row index
  nearest.features <- y[nearest.rows,]
  nearest.features$distance <- nearest.distance
  nearest.features$x.rowguid <- x$rowguid
  nearest.features$index <- rownames(nearest.features)
  nearest.features$index <- sub("\\..*", "", nearest.features$index)
  
  # Remove geometries
  st_geometry(x) <- NULL
  st_geometry(nearest.features) <- NULL
  
  # Prepend names to y columns
  names(nearest.features) <- paste0(y.name, ".", names(nearest.features))
  
  # Bind datatables and return
  #output <- cbind(x, nearest.features)
  output <- nearest.features
  return(output)
}


mapped_sewer_bd_age_length_geom$sewer_index <- seq.int(nrow(mapped_sewer_bd_age_length_geom))
my_geom <- mapped_sewer_bd_age_length_geom

#start here
mapped_sewer_bd_age_length_geom <- my_geom # %>% filter(sewer_index != "269218")
#mapped_sewer_bd_age_length_geom <- mapped_sewer_bd_age_length_geom %>% mutate(Tag = as.integer(as.character(Tag)))

rm(use_me_ans)
rm(use_me)
rm(result)
mapped_sewer_bd_age_length_geom_test = mapped_sewer_bd_age_length_geom %>% 
  select(Tag,sewer_index,Bnd_M)
mapped_sewer_bd_age_length_geom_test <- st_centroid(mapped_sewer_bd_age_length_geom_test)
mapped_sewer_bd_age_length_geom$sewer_index
tic()
for (i in mapped_sewer_bd_age_length_geom_test$sewer_index){
  mapped_sewer_bd_age_length_geom_testing <- mapped_sewer_bd_age_length_geom_test %>% rename(sewer_index_2 = sewer_index) %>% rename(Bnd_M_2 = Bnd_M)
  # mapped_sewer_bd_age_length_geom_test$Tag_3[i] <- get_nearest_features_nomessage(mapped_sewer_bd_age_length_geom_test[i, ],
  #                                                                mapped_sewer_bd_age_length_geom_testing[-mapped_sewer_bd_age_length_geom$sewer_index[i], ],
  #                                                                "sewer_index","sewer_index_2","Bnd_M_2")
  mapped_sewer_bd_age_length_geom_test$Tag_3[[i]] <- st_nearest_feature_i(mapped_sewer_bd_age_length_geom_test[i, ],
                                              mapped_sewer_bd_age_length_geom_testing[-mapped_sewer_bd_age_length_geom$sewer_index[i], ])
}
mapped_sewer_bd_age_length_geom_test
toc()

st_geometry(mapped_sewer_bd_age_length_geom_test) <- NULL
mapped_sewer_bd_age_length_geom_test %>% as.data.table()
mapped_sewer_bd_age_length_geom_test %>% filter(sewer_index == "269217")
mapped_sewer_bd_age_length_geom_test %>% filter(Tag_3 == "269217")
mapped_sewer_bd_age_length_geom_test %>% summary()






get_nearest_within <- function(mapped_sewer_bd_age_length_geom){
  for (i in mapped_sewer_bd_age_length_geom$sewer_index){
    mapped_sewer_bd_age_length_geom_test = NULL
    mapped_sewer_bd_age_length_geom_test = mapped_sewer_bd_age_length_geom
    # add 2 to all
    use_me <- mapped_sewer_bd_age_length_geom_test[-i,c("Tag","Bnd_M")]
    use_me <- use_me %>% rename(Tag_2 = Tag) %>% rename(Bnd_M_2 = Bnd_M)
    use_me_ans[i,] <- get_nearest_features_nomessage(mapped_sewer_bd_age_length_geom_test[i,],use_me,"Tag","Tag_2","Bnd_M_2")
  }
  print(use_me_ans)
}

get_nearest_within(mapped_sewer_bd_age_length_geom)


for (i in mapped_sewer_bd_age_length_geom$sewer_index) {
    
    mapped_sewer_bd_age_length_geom_test = NULL
    mapped_sewer_bd_age_length_geom_test = unique(mapped_sewer_bd_age_length_geom[,c("Tag","Bnd_M")])
    mapped_sewer_bd_age_length_geom_test <- mapped_sewer_bd_age_length_geom_test %>% filter(!is.na(Tag))
    # add 2 to all
    use_me <- mapped_sewer_bd_age_length_geom_test[-i,c("Tag","Bnd_M")]
    use_me <- use_me %>% rename(Tag_2 = Tag) %>% rename(Bnd_M_2 = Bnd_M)
    
    mapped_sewer_bd_age_length_geom_test <- st_centroid(mapped_sewer_bd_age_length_geom_test)
    use_me <- st_centroid(use_me)
    
    use_me_ans[i,] <- get_nearest_features_nomessage(mapped_sewer_bd_age_length_geom_test[i,],use_me,"Tag","Tag_2","Bnd_M_2")
}
st_geometry(use_me_ans) <- NULL
use_me_ans_2 <- use_me_ans %>% left_join(mapped_sewer_bd_age_length_geom[,c("Tag","Bnd_D")], by = "Tag_2" = "Tag")

























rm(myList)
rm(myList)
fjunctionlist <- me_3$Fjunction %>% unique() %>% as.character() 
myList_0 <- me_3 %>% distinct() %>% filter(Tjunction %in% fjunctionlist)

for (i in 1:nrow(myList_0)) {
  myList_0$indexoo <- seq.int(nrow(myList_0))
  Fjunction_i <- myList_0$Fjunction[1]
  Tag_i <- myList_0$Tag[1]
  myList <- myList_0 %>% filter(Tjunction==Fjunction_i)
  myList_i <- myList
  myList_i[1,] <- myList
  myList_i[1,"Fjunction_i"] <- Fjunction_i
  myList_i[1,"Tag_i"] <- Tag_i
}

get_junction_nodes <- function(myList_0){
  for(i in myList_0$index) {
  message(paste0(i))
  
  myList_0$index <- seq.int(nrow(myList_0))
  Fjunction_i <- myList_0$Fjunction[i]
  Tag_i <- myList_0$Tag[i]
    myList <- myList_0 %>% filter(Tjunction==Fjunction_i)
    myList_i <- myList
    myList_i[i,] <- ifelse(myList[1,1]>0,myList,0)
    myList_i[i,"Fjunction_i"] <- Fjunction_i
    myList_i[i,"Tag_i"] <- Tag_i
    if(i == 1){
      out <- myList_i
    }
      else{
        out_2 <- rbind(out,myList_i)
      }
  }
    return(out_2)
}
my_node <- get_junction_nodes(myList_0)
my_node


me_3 %>% filter(Tjunction == "400238140")
myList_0 %>% filter(Tjunction == "400238140")
myList_0 %>% filter(Tjunction == "400238141")
