s24path1 <- "D:/ag/ModalandNearestNeighbour/S24withnn/S24withnn_merged.shp"

s241 <- st_read(s24path1, stringsAsFactors = F)

s241_length <- st_length(s241$geometry)
summary(s241_length)

PdasPath2 <- "D:/ag/ModalandNearestNeighbour/PDASwithnn/PDASwithnn_merged.shp"

pdas2 <- st_read(PdasPath2, stringsAsFactors = F)

pdas2_length <- st_length(pdas2$geometry)
summary(pdas2_length)

summary(my_length)
pdas_assetbase %>% 
  select(fid,drwng__,index) %>% mutate(id_0 = row_number()) %>% 
  filter(drwng__ == "osgb1000021391250")

#########################################


library(dplyr)
library(sf)
library(caret)
library(data.table)
library(rgdal)

library(party)
library(mlbench)

library(lwgeom)
library(foreach)
library(tictoc)

library(readr)

#####
pdas_assetbase_link <- "G:/Downloads/pdas_phase_2/"
pdas_assetbase <- st_read(paste0(pdas_assetbase_link,"pdas_assetbase_12102020.shp"))
modal_atts <- readRDS("modal_atts.rds")


# functions
endorstart <- function(x, type = "end") {
  if(type == "end") {
    return(st_endpoint(x))
  } else {
    return(st_startpoint(x))
  }
}
giveneighattrs <- function(s24s, n24s) {
  st_crs(s24s) <- 27700
  st_crs(n24s) <- 27700
  s24filled <- s24s 
  
  s24filled[ , c("n1_D", "n1_M", "n1_Y",
                 "n2_D", "n2_M", "n2_Y",
                 "PURPOSE",
                 "Unbnd_D", "Unbnd_M", "Unbnd_Y",
                 "Tag")] <- "NoMatch"
  
  for(i in  1:dim(s24s)[1]) {
    
    for(type in c("end", "start")) {
      
      intersectindex <- s24s[i, ] %>% 
        endorstart(type) %>% 
        st_buffer(0.2, nQuadSegs = 1, endCapStyle = "SQUARE") %>% 
        st_intersects(n24s)
      
      if (length(intersectindex[[1]]) > 0) {
        s24filled[i, ] <- s24s[i, ] %>% 
          bind_cols(n24s[intersectindex[[1]][1], c("n1_D", "n1_M", "n1_Y",
                                                   "n2_D", "n2_M", "n2_Y",
                                                   "Type",
                                                   "Unbnd_D", "Unbnd_M", "Unbnd_Y",
                                                   "Tag")] %>% 
                      st_drop_geometry() %>% 
                      rename(NghTag = Tag))
        break
      }
    }
  }
  return(s24filled)
}
giveneighattrs2 <- function(s24s, n24s) {
  
  if (dim(s24s)[1] == 0) {
    return(NULL)
  }
  
  s24s 
  
  n24s <- n24s %>% 
    mutate(index = row_number())
  
  a <- st_endpoint(s24s) %>% 
    st_buffer(2.5, nQuadSegs = 1, endCapStyle = "SQUARE") %>% 
    st_intersects(n24s)
  
  b <- st_startpoint(s24s) %>% 
    st_buffer(2.5, nQuadSegs = 1, endCapStyle = "SQUARE") %>% 
    st_intersects(n24s)
  
  # See if there are connectinos on either the end point or the start point
  # Do a join onto those points then using the VALUE key
  # Also record if it;s joining on the start or end point
  
  mapply(function(a, b) c("endpoint" = a[1], "startpoint" = b[1], "value" = c(a,b)[1]) %>% 
           bind_rows, a, b, SIMPLIFY = F) %>% 
    bind_rows() %>%
    mutate(connectionend = ifelse(!is.na(endpoint), "endpoint",
                                  ifelse(!is.na(startpoint), "startpoint", "none"))) %>% 
    select(-c(endpoint, startpoint)) %>% 
    bind_cols(s24s) %>% 
    left_join(n24s %>% 
                st_drop_geometry() %>% 
                select(c("n2_D", "n2_M", "n2_Y",
                         "Tag", "index")) %>% 
                rename(value = index),
              by = "value")
}
get_one_to_one_distance <- function(x, y, x.id, y.id) {
  
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
  x$x_index <- seq.int(nrow(x))
  y2 <- as.data.frame(x[,c(y.id,"x_index")]) %>% inner_join(y[,y.id], by = y.id)
  # y2 <- merge(as.data.frame(x[,c(y.id,"x_index")]), y[,y.id], by = y.id,all.x=FALSE, all.y=FALSE)
  y2 <- st_as_sf(y2, crs = 27700)
  
  # Compute x_index:x_index distance matrix
  for (i in 1:nrow(x2)){
    x2$length_to_centroid[[i]] <- st_distance(x2[i, x.id], y2[i, y.id])
  }
  x3 <- x2[,c(x.id,y.id,"length_to_centroid")]
  return(x3)
}
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
divide_and_conquer <- function(x,your_func = your_func, func_call = func_call, splits = 100,each = 1){
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- mutate(x, my_split = my_split)
  my_split3 <- tidyr::nest(my_split2, - my_split) 
  models <- purrr::map(my_split3$data, ~ your_func(., func_call))
  bind_lists <- plyr::rbind.fill(models)
  return(bind_lists)
}

# test on one
pdas_assetbase_sample <- pdas_assetbase %>% #[124600:124700,] %>% 
  #filter(drwng__ == "osgb1000021391250") %>% 
  st_cast("LINESTRING") %>% 
  select(fid,drwng__,index) %>% mutate(id_0 = row_number())
# pdas_assetbase_sample_2 <- st_endpoint(pdas_assetbase_sample)
# linestring_intersection <- giveneighattrs2(pdas_assetbase_sample,modal_atts) %>% #,"id_0","Tag")
#   st_sf() %>% st_set_crs(27700)
# st_crs(modal_atts) <- 27700
# the_index <- st_nearest_feature(linestring_intersection,modal_atts)
# my_n24 <- modal_atts %>% filter(row_number() == the_index) %>% as.data.frame()
# my_intersection <- st_intersects(linestring_intersection,my_n24)

# my_endpoint <- endorstart(pdas_assetbase_sample)
# my_endpoint$index <- 1
# 
# the_index <- st_nearest_feature(pdas_assetbase_sample,modal_atts)
# 
# my_n24 <- modal_atts %>% filter(row_number() == the_index)
# 
# giveneighattrs(pdas_assetbase_sample,modal_atts)
# giveneighattrs2(my_endpoint,modal_atts) %>% as.data.frame()
st_crs(pdas_assetbase_sample) <- 27700
st_crs(modal_atts) <- 27700
which_is_nearest <- giveneighattrs2(pdas_assetbase_sample,modal_atts) %>% as.data.frame()
which_is_nearest <- which_is_nearest %>% mutate(id_0 = row_number()) %>% st_sf() %>% st_set_crs(27700)
#
linestring_intersection <- giveneighattrs2(pdas_assetbase_sample,modal_atts) %>% #,"id_0","Tag")
  filter(connectionend == "endpoint") %>% 
  st_sf() %>% st_set_crs(27700)
st_crs(modal_atts) <- 27700
# the_index <- divide_and_conquer(linestring_intersection,st_nearest_feature,modal_atts)
my_n24 <- modal_atts %>% #filter(row_number() == the_index) %>% 
  mutate(id_0 = row_number()) %>% 
  as.data.frame()
#
my_n24 <- my_n24 %>% st_sf %>% st_set_crs(27700)
#my_df <- get_intersection(which_is_nearest,my_n24,"id_0","Tag")
the_int <- divide_and_conquer(which_is_nearest[1:100,],st_intersects,my_n24) %>% as.data.frame()
the_int <- st_intersects(which_is_nearest,my_n24) %>% as.data.frame()

my_ans <- which_is_nearest %>% 
  inner_join(the_int, by = c("id_0"="row.id"))
my_ans <- my_ans[!duplicated(my_ans$id_0), ] %>% as.data.frame() %>% 
  select(fid,drwng__,index,id_0,Tag,n2_M,n2_D,n2_Y, col.id) %>% distinct() %>% 
  filter(!is.na(n2_M,!is.na(n2_D)))



# my_ans %>% filter(drwng__ == "osgb1000021391250")







tmap::tmap_mode("view")
qtm(which_is_nearest[1,],basemaps = "Esri.WorldStreetMap") + qtm(my_n24,basemaps = "Esri.WorldStreetMap")
qtm(which_is_nearest[2,],basemaps = "Esri.WorldStreetMap") + qtm(my_n24,basemaps = "Esri.WorldStreetMap")
qtm(which_is_nearest[3,],basemaps = "Esri.WorldStreetMap") + qtm(my_n24,basemaps = "Esri.WorldStreetMap")
qtm(which_is_nearest[4,],basemaps = "Esri.WorldStreetMap") + qtm(my_n24,basemaps = "Esri.WorldStreetMap")
