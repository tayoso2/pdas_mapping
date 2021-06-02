remove_anomalies_cat_pred <- function(x,y,j){
  #for (y in 1:length(x))
  # x is the model training data
  # y is the new data you want to predict on
  # j is the number of columns
  for (i in 1:j)
    y <- na.omit(y)
    x_cols <- x[,j] %>% unlist()
    y_cols <- y[,j] %>% unlist()
    "%ni%" <- Negate("%in%")
    j_col <- colnames(y)[j]
    y_filtered <- y[y[,j_col] %ni% x_cols,]
    y_filtered_2 <- y %>% anti_join(y_filtered, by = j_col)
    return(y_filtered_2)
}

did_it_work <- remove_anomalies_cat_pred(new_x,new_y,6)

new_x <- traindata.base[,c("P_Type","res_buil_1", "County", "Tp_prps",
                  "Year",  "aprx_pstcd_2ch")]
new_y <- s24_clusters_5[,c("P_Type", "res_buil_1", "County", "Tp_prps",
                  "Year",  "aprx_pstcd_2ch")]

new_y <- na.omit(new_y)
new_x_cols <- new_x[,6] %>% unlist() %>% unique()
new_y_cols <- new_y[,6] %>% unlist() %>% unique()
"%ni%" <- Negate("%in%")
j_col <- colnames(new_y)[6]
new_y_filtered <- new_y[new_y[,j_col] %ni% new_x_cols,] # solve me
new_y_filtered_2 <- new_y %>% anti_join(new_y_filtered, by = j_col)
new_y_filtered_2






#########################################################################














original_list_2ch <- traindata.base %>% select(aprx_pstcd_2ch) %>% unlist()
original_list_3ch <- traindata.base %>% select(aprx_pstcd_3ch) %>% unlist()
original_list_4ch <- traindata.base %>% select(aprx_pstcd_4ch) %>% unlist()
original_list_os <- traindata.base %>% select(os_no_of_props) %>% unlist()
original_list_soil <- traindata.base %>% select(SIMPLE_DES) %>% unlist()

list_2ch <- s24_clusters_5 %>% select(aprx_pstcd_2ch) %>% unlist()
list_3ch <- s24_clusters_5 %>% select(aprx_pstcd_2ch) %>% unlist()
list_4ch <- s24_clusters_5 %>% select(aprx_pstcd_4ch) %>% unlist()
list_os <- s24_clusters_5 %>% select(os_no_of_props) %>% unlist()
list_soil <- s24_clusters_5 %>% select(SIMPLE_DES) %>% unlist()

"%ni%" <- Negate("%in%")
ni_2ch_s24_clusters_5 <- s24_clusters_5 %>% filter(aprx_pstcd_2ch %ni% original_list_2ch)
ni_3ch_s24_clusters_5 <- s24_clusters_5 %>% filter(aprx_pstcd_3ch %ni% original_list_3ch)
ni_4ch_s24_clusters_5 <- s24_clusters_5 %>% filter(aprx_pstcd_4ch %ni% original_list_4ch)
ni_os_s24_clusters_5 <- s24_clusters_5 %>% filter(os_no_of_props %ni% original_list_os)
ni_soil_s24_clusters_5 <- s24_clusters_5 %>% filter(SIMPLE_DES %ni% original_list_soil)

# list_2ch <- x %>% anti_join(p, by = "a") %>% select(aprx_pstcd_2ch) %>% unlist()
# list_soil <- x %>% anti_join(p, by = "a") %>% select(SIMPLE_DES) %>% unlist()

s24_clusters_6 <- s24_clusters_5 %>% anti_join(ni_2ch_s24_clusters_5, by = "aprx_pstcd_2ch") %>% 
  anti_join(ni_3ch_s24_clusters_5, by = "aprx_pstcd_3ch") %>% 
  anti_join(ni_4ch_s24_clusters_5, by = "aprx_pstcd_4ch") %>% 
  anti_join(ni_os_s24_clusters_5, by = "os_no_of_props") %>% 
  anti_join(ni_soil_s24_clusters_5, by = "SIMPLE_DES") %>% 
  as_tibble() %>% 
  mutate(mdn_pst_y = as.factor(as.character(Year)),
         aprx_pstcd_2ch = as.factor(aprx_pstcd_2ch),
         aprx_pstcd_3ch = as.factor(aprx_pstcd_3ch),
         res_buil_1 = as.factor(res_buil_1),
         Tp_prps = as.factor(Tp_prps),
         SIMPLE_DES = as.factor(SIMPLE_DES),
         County = as.factor(County),
         Tp_prps = as.factor(Tp_prps),
         Year = as.factor(as.character(Year)),
         P_Type = as.factor(P_Type)) %>% 
  distinct()





# SPLIT PROXIES #######################################

install.packages("devtools")
devtools::install_github("jmt2080ad/polylineSplitter")
library(polylineSplitter)

# load data
proxy_sewer_network <- st_read("G:/Desktop/STW PDaS/Phase 2 datasets/proxy_sewer_network/proxy_sewer_network.shp")

# sf
one_geom <- s24_pipegroups_geometry_labelled_v3_geom[1,]
st_length(one_geom)

# sp
nc_sp <- as_Spatial(one_geom$geometry)
st_sf(nc_sp)
spTransform(nc_sp, CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84"))
# spTransform(nc_sp, CRS("+proj=tmerc +lat_0=0 +lon_0=27 +k=1 +x_0=5500000 +y_0=0 +ellps=krass +units=m +no_defs"))
# gauss <- coordinates(nc_sp)
# gauss <- gauss[[1]][[1]]
# colnames(gauss) <- c('x', 'y')
# gauss_sf <- st_as_sf(as.data.frame(gauss), coords = c("x", "y"), crs = 4326)

# split it
splitted <- splitLines(nc_sp,2)
proj4string(splitted) <- CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84")
splitted_sf <- st_as_sf(as(splitted, "SpatialLines"))

# split L shaped linestrings
l_2_line <- proxy_sewer_network
l_2_line_points <- l_2_line %>% select(Tag,geometry) %>% group_by(Tag) %>% st_cast("MULTIPOINT") %>% as.data.frame() %>% st_sf()
l_2_line_mergedlines <- st_cast(l_2_line_points, "MULTILINESTRING")

# 
# l_2_line_split <- lwgeom::st_split(l_2_line, l_2_line_points)
# s24_pipegroups_geometry_labelled_v3_geom[1,]$geometry[1][1][[1]] # 1 linestring
# l_2_line$geometry[1][1][[1]] # 4 linestrings
# l_2_line_points$geometry[1][1][[1]]
# l_2_line_split$geometry[1][1][[1]]
# qtm(l_2_line,basemaps = "Esri.WorldStreetMap") + tm_basemap("Esri.WorldStreetMap")
# qtm(l_2_line_mergedlines[3,],basemaps = "Esri.WorldStreetMap") + tm_basemap("Esri.WorldStreetMap")

# create function which does the above -------------------------------------------------------------------------------

split_my_linestrings <- function(x,meters){
  x$id_0 <- seq.int(nrow(x))
  for (i in 1:nrow(x)) {
    message(paste0("row number ",i))
    # sp
    x1_sp <- as_Spatial(x$geometry[i])
    
    # split it
    x1_splitted <- polylineSplitter::splitLines(x1_sp, meters)
    proj4string(x1_splitted) <- CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84")
    x1_splitted_sf <- st_as_sf(as(x1_splitted, "SpatialLines"))
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
divide_and_conquer <- function(x,your_func = your_func, func_call = func_call, splits = 100,each = 1){
  my_split <- rep(1:splits,each = each, length = nrow(x))
  my_split2 <- mutate(x, my_split = my_split)
  my_split3 <- tidyr::nest(my_split2, - my_split) 
  tic()
  models <- purrr::map(my_split3$data, ~ your_func(., func_call)) #lengthy processing time - 6.5 hrs
  toc()
  bind_lists <- plyr::rbind.fill(models)
  return(bind_lists)
}

divide_and_conquer(l_2_line_mergedlines,split_my_linestrings,2,1000)

tic()
got_my_segment <- split_my_linestrings(l_2_line_mergedlines[1:1000,],2) %>% st_sf(.,crs = 27700)
toc()
qtm(got_my_segment,basemaps = "Esri.WorldStreetMap") + tm_basemap("Esri.WorldStreetMap")

# using purr to nest
my_split <- rep(1:1000,each = 1, length = nrow(l_2_line_mergedlines))
my_split2 <- mutate(l_2_line_mergedlines, my_split = my_split)
my_split3 <- tidyr::nest(my_split2, - my_split) 
tic()
models <- purrr::map(my_split3$data, ~ split_my_linestrings(., 2)) #lengthy processing time - 6.5 hrs
toc()
segmented <- plyr::rbind.fill(models)
segmented <- st_sf(segmented)
st_crs(segmented) <- 27700
segmented$geometry <- st_cast(segmented$geometry,"MULTILINESTRING")
st_write(segmented,"segmented.shp")





# THE END ----------------------------------------------------------------------------------------------------------------

split_my_linestrings(s24_pipegroups_geometry_labelled_v3_geom[c(1,3),],2)
split_my_linestrings(s24_pipegroups_geometry_labelled_v3_geom[2,],0.5)
split_my_linestrings(l_2_line_mergedlines[3,],2)

tmap_mode("view")
qtm(got_my_segment,basemaps = "Esri.WorldStreetMap") + tm_basemap("Esri.WorldStreetMap")
qtm(s24_pipegroups_geometry_labelled_v3_geom[2,],basemaps = "Esri.WorldStreetMap") + tm_basemap("Esri.WorldStreetMap")
qtm(s24_pipegroups_geometry_labelled_v3_geom[3,],basemaps = "Esri.WorldStreetMap") + tm_basemap("Esri.WorldStreetMap")



no1 <- split_my_linestrings(s24_pipegroups_geometry_labelled_v3_geom[1,],2)
no2 <- split_my_linestrings(s24_pipegroups_geometry_labelled_v3_geom[3,],2)
rbind(no1,no2)


# test function above ---------------------------------------------------
test = s24_pipegroups_geometry_labelled_v3_geom[1:3,]
rm(test_sp)
for (i in 1:nrow(test)) {
  # sp
  test_sp <- split_my_linestrings(test[i,],2)
}  
  # split it
  test_splitted <- splitLines(test_sp,2)
  proj4string(test1_splitted) <- CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84")
  test1_splitted_sf <- st_as_sf(as(test1_splitted, "SpatialLines"))
}


s24_pipegroups_geometry_labelled_v3_geom1 <- s24_pipegroups_geometry_labelled_v3_geom[1,]

# sp
s24_pipegroups_geometry_labelled_v3_geom1_sp <- as_Spatial(s24_pipegroups_geometry_labelled_v3_geom1$geometry)

# split it
s24_pipegroups_geometry_labelled_v3_geom1_splitted <- splitLines(s24_pipegroups_geometry_labelled_v3_geom1_sp,2)
proj4string(s24_pipegroups_geometry_labelled_v3_geom1_splitted) <- CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84")
s24_pipegroups_geometry_labelled_v3_geom1_splitted_sf <- st_as_sf(as(s24_pipegroups_geometry_labelled_v3_geom1_splitted, "SpatialLines"))














####


df<- data.frame(id = getSpPPolygonsIDSlots(nc_sp))
row.names(df) <- getSpPPolygonsIDSlots(nc_sp)

ob <- SpatialPolygons(splitted,proj4string=CRS("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84"))# Your SpatialPolygons Object
splitted_df <- SpatialPolygonsDataFrame(ob,data=as.data.frame("yourData"),proj4string=CRS("+proj= aea > +ellps=GRS80 +datum=WGS84"))

# plot sf
tmap_mode("view")
qtm(nc_sp,basemaps = "Esri.WorldStreetMap") + tm_basemap("Esri.WorldStreetMap")

# plot sp
tmap_mode("view")
qtm(nc_sp,basemaps = "Esri.WorldStreetMap") + tm_basemap("Esri.WorldStreetMap")


# stackoverflow

s1 <- rbind(c(0,3),c(0,4),c(1,5),c(2,5))
s2 <- rbind(c(0.2,3), c(0.2,4), c(1,4.8), c(2,4.8))
s3 <- rbind(c(0,4.4), c(0.6,5))
mls <- st_multilinestring(list(s1,s2,s3))

# calculate overall length
st_length(mls)

# split multilinestring into component linestring objects
mls_points <- st_cast(mls, "MULTIPOINT")
mls_split <- st_split(mls, mls_points)

plot(mls)
plot(mls_points, add = TRUE, col = "red")


