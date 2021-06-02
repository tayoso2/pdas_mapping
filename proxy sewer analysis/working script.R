# get the nearest mapped sewer mains to sewerln -----------------------------------------------

st_crs(proxy_sewer) <- 27700
st_crs(sewerln_shp) <- 27700
sewerln_shp <- sewerln_shp %>% rename(Tag_sewerln = Tag)

get_nearest_features_xycentroid <- function(x, y, pipeid, id, feature) {
  
  # pipeid is id of x
  # id is id of y
  # feature is attribute to be added to x from y
  # function needs some improvement
  
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
  y$rowguid <- seq.int(nrow(y))
  
  # edit column names
  colnames(x)[which(names(x) == pipeid)] <- "id"
  colnames(y)[which(names(y) == id)] <- "id"
  colnames(y)[which(names(y) == feature)] <- "feature"
  
  # convert both x and y to centroid
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
  return(output)
}
find_one_to_one_distance <- function(x, y, x.id, y.id) {
  
  # x.id is id of x
  # y.id is id of y
  # feature is attribute to be added to x from y
  
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
  
  # pre-process x and y
  x2 <- x
  st_geometry(x) <- NULL
  x$index <- seq.int(nrow(x))
  y2 <- as.data.frame(x[,c(y.id,"index")]) %>% inner_join(y[,y.id], by = y.id)
  # y2 <- merge(as.data.frame(x[,c(y.id,"index")]), y[,y.id], by = y.id,all.x=FALSE, all.y=FALSE)
  y2 <- st_as_sf(y2, crs = 27700)
  y2 <- st_centroid(y2)
  
  # Compute index:index distance matrix
  for (i in 1:nrow(x2)){
    x2$length_to_centroid[[i]] <- st_distance(x2[i, x.id], y2[i, y.id])
  }
  x3 <- x2[,c(x.id,y.id,"length_to_centroid")]
  return(x3)
}

# apply above fxns to find the distance between the nearest proxy sewer and the sewerln_with_attributes.shp

proxy_near_sewerln <- get_nearest_features_xycentroid(proxy_sewer,sewerln_shp,"Tag","Tag_sewerln","Fjunction")
#find_one_to_one_distance(proxy_near_sewerln[1:10,],sewerln_shp,"Tag","Tag_sewerln")
proxy_near_sewerln_two <- find_one_to_one_distance(proxy_near_sewerln,sewerln_shp,"Tag","Tag_sewerln")
proxy_near_sewerln_three <- proxy_near_sewerln_2 %>% filter(length_to_centroid <= 20)


# remove the tag ids from the "sewer" data above and save the new sewer dataset.
trunc_sewer <- proxy_near_sewerln_three %>% anti_join(sewer, by = "Tag")


proxy_near_sewerln2 <- proxy_near_sewerln
st_geometry(proxy_near_sewerln) <- NULL
sugar <- as.data.frame(proxy_near_sewerln[,c("index","Tag_sewerln")]) %>% inner_join(sewerln_shp, by = "Tag_sewerln")
sugar <- st_as_sf(sugar, crs = 27700)
sugar <- st_centroid(sugar)

proxy_near_sewerln_2 <- find_one_to_one_distance(proxy_near_sewerln2[,"Tag"],sugar[,"Tag_sewerln"])
proxy_near_sewerln_3 <- proxy_near_sewerln_2 %>% filter(length_to_centroid <= 20)