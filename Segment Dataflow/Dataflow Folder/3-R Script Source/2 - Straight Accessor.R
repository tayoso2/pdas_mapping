# All packages are loaded as part of "3 - Chunker.R"

# centroids = b[[1]]
# n24pipes = n24

notionalfunction <- function(centroids, n24pipes) {
  
  # Convert to sfdf's
  centroids <- centroids %>% 
    st_as_sf 
  n24pipes <- n24pipes %>% 
    st_as_sf 
  
  # Get nearest neighbour and line obviously
  
  centroidtosewer <- findnearestneighbourandgetline(centroids = centroids,
                                                    n24pipes = n24pipes,
                                                    crs = 27700)
  # Pull some attributes from nearest neighbour
  # centroidtosewer <- getattributesandlength(s24pdaslines = centroidtosewer,
  #                        n24pipes = n24pipes,
  #                        )
  
  
  return(centroidtosewer)
  
  # Redo FOUL and SURFACE ---------------------------------------------------
  
  # Do lines again for foul / surface
  nonfoulsurface <- centroidtosewer %>% 
    filter((ConNghTyp != "F" & ConNghTyp != "S") | is.na(ConNghTyp))
  
  # Anything that came up as FOUL, get it's SURFACE counterpart.
  foulsewers_keep <- centroidtosewer %>%
    filter(ConNghTyp == "F") 
  
  foulsewers <- foulsewers_keep %>% 
    st_drop_geometry() %>% 
    select(fid) %>% 
    left_join(centroids)
  
  if(dim(foulsewers)[1] > 0) {
    
    foulsewers <- foulsewers  %>% 
      st_as_sf
    
    if(dim(n24pipes %>% 
           filter(Type == "F"))[1] == 0) {
      newfoul <- NULL
      break
    } else {
      
      newsurface <- findnearestneighbourandgetline(centroids = foulsewers,
                                                   n24pipes = n24pipes %>% 
                                                     filter(Type == "S"),
                                                   crs = 27700)
      
      newsurface <- getattributesandlength(s24pdaslines = newsurface,
                                           n24pipes = n24pipes %>% 
                                             filter(Type == "S"))
    }
  } else {
    newsurface <- NULL
  }
  
  # Anything that came up as SURFACE, get it's FOUL counterpart.
  
  surfacesewers_keep <- centroidtosewer %>%
    filter(ConNghTyp == "F") 
  
  surfacesewers <- surfacesewers_keep %>% 
    st_drop_geometry() %>% 
    select(fid) %>% 
    left_join(centroids)
  
  if(dim(surfacesewers)[1] > 0) {
    
    surfacesewers <- surfacesewers %>% 
      st_as_sf
    
    if(dim(n24pipes %>% 
           filter(Type == "F"))[1] == 0) {
      newfoul <- NULL
      break
      
    } else {
      newfoul <- findnearestneighbourandgetline(centroids = surfacesewers,
                                                n24pipes = n24pipes %>% 
                                                  filter(Type == "F"),
                                                crs = 27700)
      
      newfoul <- getattributesandlength(s24pdaslines = newfoul,
                                        n24pipes = n24pipes %>% 
                                          filter(Type == "F"))
    }
  } else {
    newfoul <- NULL
  }
  
  
  return(rbind(nonfoulsurface, newsurface, newfoul, foulsewers_keep, surfacesewers_keep))
  
}

# Build boundary of property group
# Return the list of Convex Hull by each propsgroup
# properties: Multi-Polygon or Polygon
getPropertyBoundary <- function(properties) {
  # get all points in the polygon
  # only take the first group to avoid the well and polygons in the properties
  properties$geometry <- st_cast(properties$geometry, "MULTIPOINT")[1]
  
  # find the point to build convex hull
  #convex_points <- chull(st_coordinates(properties$geometry)[, 1], st_coordinates(properties$geometry)[, 2])
  properties$convex_points <- lapply(properties$geometry, chull)
  
  # TODO: rewrite this for iteration to apply function if possible
  bboxes <- st_sfc() %>%
    st_set_crs(27700)
  for(i in 1:nrow(properties)){
    boundary <- st_coordinates(properties$geometry[i])[properties$convex_points[[i]], ]
    boundary <- rbind(boundary, boundary[1,])
    boundary <- st_zm(st_polygon(list(boundary)),drop = TRUE)
    bboxes[i] <- boundary
  }
  
  return(bboxes)
  
}


# Get the shortest line connect 2 polygons
# Then get the midpoint of this line
getMidPointBetweenPolygons <- function(polygon1, polygon2){
  return(st_centroid(st_nearest_points(polygon1, polygon2)))
}



# Copy from matlib library ------------------------------------------------


len <- function(X) {
  if (!is.numeric(X)) stop("X must be numeric")
  if (is.vector(X)) X <- matrix(X, ncol=1)
  sqrt(colSums(X^2))
}

r2d <- function(radius){
  return(radius * 180 / pi)
}

get_angle <- function(x, y, degree = TRUE) {
  theta <- acos(x %*% y / (len(x) * len(y)))
  if(degree) theta <- r2d(theta)
  theta
}


# Copy from matlib library ------------------------------------------------



# polygon1, polygon2
getPipeRadiusBetweenPolygon <- function(polygon1, polygon2){
  near_points <- st_nearest_points(poly1, poly2)
  
}

# get the shortest distance between 2 polygons, or polygon and linestring
getShortestDistance <- function(polygon1, polygon2){
  near_points <- st_nearest_points(poly1, poly2)
  
}


getSewerSystem <- function(nearest_sewer_type){
  # decide the position based on sewer type
  if(nearest_sewer_type == "S"){
    # surface water 
    type <- "separate"
  } else if(nearest_sewer_type == "F"){
    # foul pipes
    type <- "separate"
  } else{
    # combined system
    #position <- "back"
    type <- "combined"
  }
  
  return(type)
}

getSewerPosition <- function(nearest_sewer_type){
  # decide the position based on sewer type
  if(nearest_sewer_type == "S"){
    # surface water 
    position <- "infront"
  } else if(nearest_sewer_type == "F"){
    # foul pipes
    position <- "back"
  } else{
    # combined system
    position <- "back"
  }
  
  return(position)
}

getPipesAcrossCentriod <- function(centroids){
  pipesUnderProps <- centroids %>%
    filter(n()>1) %>%
    mutate(geometry = st_union(geometry)) %>%
    mutate(geometry = st_cast(geometry, 'LINESTRING')) %>%
    distinct(.keep_all = T)
  return(pipesUnderProps)
}


getFlowDirection <- function(pipesUnderProps, nearest_sewer){
  # To determine the flow direction
  endpoint <- st_cast(pipesUnderProps, "MULTIPOINT")
  endpoint <- as.matrix(endpoint$geometry[[1]], col=2)
  bearing_1n <- (360 + (90 - ((180/pi)*atan2(endpoint[nrow(endpoint), 2] - endpoint[1,2], 
                                             endpoint[nrow(endpoint), 2] - endpoint[1,1])))) %% 360
  bearing_n1 <- (360 + (90 - ((180/pi)*atan2(endpoint[1,2] - endpoint[nrow(endpoint), 2], 
                                             endpoint[1,1] - endpoint[nrow(endpoint), 2])))) %% 360
  
  
  #n24_t <- n24 %>% filter(!is.na(bearing))
  
  nearest_sewer_bearing <- nearest_sewer$bearing
  angle_1 <- sin((bearing_1n - nearest_sewer_bearing) * pi/180 - pi/2)
  angle_2 <- sin((bearing_n1 - nearest_sewer_bearing) * pi/180 - pi/2)
  
  if (is.na(angle_1) | is.na(angle_2)) {
    direction <- "left"
  } else
    
    if (angle_1 < angle_2) {
      direction <- "left"
    } else{
      direction <- "right"
    }
  
  return(direction)
}


getEndPoints <- function(pipesUnderProps){
  endpoint <- st_cast(pipesUnderProps, "MULTIPOINT")
  endpoint <- as.matrix(endpoint$geometry[[1]], col=2)
  return(endpoint)
}

# -------------------------------------------------------------------------


# Draw L-Shape by Group ---------------------------------------------------


# -------------------------------------------------------------------------

# buildings: building polygon
# n24: (sorry for the bad naming) sewer main 
# type: sewer main type, the valid values are:
# C: combined sewer (default or invalid value)
# F: foul sewer
# S: surface water 
# position: pipe goes behind/infront the property group, the valid values are:
# back: go behind (default or invalid value)
# infront: go infront (Surface water)
# distance: the buffer distance of boundary of property group

drawLShape <- function(buildings, n24, group_id, distance = 1){
  
  #print(group_id)
  
  # if n24 or buildings are empty, stop
  if(is.null(n24)) {
    return()
  } else if(is.null(buildings)){
    return()
  }
  
  # validating buildings
  buildings <- st_as_sf(buildings) %>% 
    st_set_crs(27700) %>%
    filter(drwng__ == group_id) %>%
    st_buffer(0)
  
  # save the origin version for separate system
  #n24_origin <- n24
  
  # validating sewers
  n24 <- st_as_sf(n24)%>%
    mutate(Tp_purpose = ifelse(is.na(Tp_purpose),"F",as.character(Tp_purpose)), Tp_purpose = as.factor(Tp_purpose)) %>% # TO added this
    filter(Tp_purpose %in% c("C", "F")) %>%
    st_set_crs(27700) %>%
    filter(!is.na(Tp_purpose)) # %>%
    # filter(!is.na(bearing))
  
  # Get centroids of each buildings
  centroids <- st_centroid(buildings)
  
  # Make sure only keep the centroids with specific group_id
  #centroids <- centroids[order(centroids$fid),]
  
  # To link the pipes to sewer
  long_distance<- 300
  
  # draw pipes across centroids
  if(nrow(centroids)==1){
    # if only 1 centroid in drawing group
    # draw line from centriod to closeset sewer main with C/F type and return
    
    pipesUnderProps <- centroids[1,] 
    centroids_to_sewer <- st_nearest_points(n24[st_nearest_feature(pipesUnderProps, n24), ], pipesUnderProps)
    clips <- st_difference(centroids_to_sewer, buildings[1,])
    if(length(clips)==0){
      pipesUnderProps$geometry <- centroids_to_sewer
    } else{
      pipesUnderProps$geometry <- clips
    }
    
    pipesUnderProps <- pipesUnderProps %>%
      mutate(direction = "Single",
             type = "Single",
             position = "Single",
             nearest_sewer_type = "Single"
      )
    
    return(pipesUnderProps)
    
  } else if(nrow(centroids)<1){
    # if drawing group is empty , return NULL
    # return(st_set_crs(st_sf(st_sfc()), 27700))
    return(NULL)
  } else{
    # draw lines across the properties 
    pipesUnderProps <- getPipesAcrossCentriod(centroids)
  }
  
  # search for the nearest sewer main 
  nearest_sewer <- n24[st_nearest_feature(pipesUnderProps, n24), ]
  
  # Get the left most and right most point of pipes across properties
  endpoint <- getEndPoints(pipesUnderProps =  pipesUnderProps)
  
  # group property polygon by each group
  buildingblock <-  buildings %>%
    mutate(geometry = st_union(geometry))%>%
    distinct(.keep_all = T) %>%
    st_set_crs(27700) %>%
    st_buffer(0)
  
  # get the largest distance between centriods and border
  boundary <- getPropertyBoundary(buildingblock) %>% 
    st_buffer(0)
  
  # buffer the convex hull by given distance
  boundary_buffer <- st_buffer(boundary, distance)
  
  # To determine the flow direction
  direction <- getFlowDirection(pipesUnderProps, nearest_sewer)
  
  # draw line to border based on direction
  if(direction == "left"){
    if(nrow(centroids) > 1){
      
      radius <- atan2((endpoint[1,2] - endpoint[2,2]), (endpoint[1,1] - endpoint[2,1]))
      new_point <- st_point(endpoint[1, ]) + c(long_distance * cos(radius), long_distance * sin(radius) ) 
      extend_line <- st_nearest_points(st_point(endpoint[1, ]), new_point)%>%
        st_set_crs(27700)
      
      # get the line from pipe across properties to boundary
      line_to_border <- st_intersection(extend_line , boundary_buffer)
      
    } else{
      line_to_border <- st_nearest_points(centroids$geometry, boundary_buffer)
    }
    
  }else{
    if(nrow(centroids) > 1){
      radius <- atan2((endpoint[nrow(endpoint), 2] - endpoint[nrow(endpoint) -1,2]), 
                      (endpoint[nrow(endpoint) , 1] - endpoint[nrow(endpoint) -1,1]))
      new_point <- st_point(endpoint[nrow(endpoint), ]) + c(long_distance * cos(radius), long_distance * sin(radius) ) 
      extend_line <- st_nearest_points(st_point(endpoint[nrow(endpoint), ]), new_point)%>%
        st_set_crs(27700)
      # get the line from pipe across properties to boundary
      line_to_border <- st_intersection(extend_line , boundary_buffer)
    }else{
      line_to_border <- st_nearest_points(centroids$geometry, boundary_buffer)
    }
    
  }
  
  # combine the extended line to pipes across centroids
  nearest_sewer <- n24[st_nearest_feature(line_to_border, n24), ]
  to_combine <- data.frame(geometry = c(pipesUnderProps$geometry, line_to_border))
  pipesUnderProps$geometry <- st_combine(to_combine$geometry)
  
  
  shortest_distance <- line_to_border
  
  # find the cloest sewer main close to the chosen end point
  nearest_sewer <- n24[st_nearest_feature(line_to_border, n24), ]
  
  # slope of the shortest line, to find shift distance
  angle_shortest_1 <- radius * 180 / pi + 90
  angle_shortest_2 <- radius * 180 / pi - 90
  
  # get the interesection point from line_to_border to property boundary
  midpoint <- st_cast(line_to_border, "POINT")[2]
  
  
  # find the nearest sewer of midpoint and draw line
  #nearest_sewer <- n24[st_nearest_feature(midpoint, n24), ]$geometry
  #print(st_nearest_feature(midpoint, n24))
  nearest_sewer_type <- nearest_sewer$Tp_purpose
  if(is.na(nearest_sewer_type)){
    nearest_sewer_type<-"NA"
  }
  
  midpoint_to_sewer <- st_nearest_points(midpoint, nearest_sewer)
  to_use <- as.matrix(midpoint_to_sewer[[1]], col=2)
  
  # to find the direction (back/infront) to move 
  # move front
  midpoint_to_sewer_radius <- atan2((to_use[2, 2] - to_use[1,2]), 
                                    (to_use[2, 1] - to_use[1,1]))
  # move back
  sewer_to_midpoint_radius <- atan2((to_use[1, 2] - to_use[2,2]), 
                                    (to_use[1, 1] - to_use[2,1]))
  
  
  # get the type and position based on the type of nearest sewer main (C/F/S)
  type <- getSewerSystem(nearest_sewer_type = nearest_sewer_type)
  position <- getSewerPosition(nearest_sewer_type = nearest_sewer_type)
  
  
  # find the intersection point of the farest property 
  intersections <- st_difference(pipesUnderProps, buildingblock) %>%
    st_cast("MULTIPOINT") %>%
    mutate(geometry = st_union(geometry)) %>%
    distinct(.keep_all = T)
  
  intersections <- as.matrix(intersections$geometry[[1]], col=2)
  
  
  if(position == "back"){
    radius_intersect <- sewer_to_midpoint_radius
  } else{
    radius_intersect <- midpoint_to_sewer_radius
  }
  
  v_extension <- c(long_distance * cos(radius_intersect), long_distance * sin(radius_intersect))
  
  # Draw lines from the second last interestion point to boundary
  if(direction == "right"){
    if(nrow(intersections) > 2){
      #radius_intersect <- getExtensionRadius(position, radius_i, angle_shortest_1, angle_shortest_2)
      radius_i <- atan2((intersections[2,2] - intersections[3,2]), (intersections[2,1] - intersections[3,1]))
      pt <- st_point(intersections[2, ])
    } else{
      radius_i <- atan2((intersections[1,2] - intersections[2,2]), (intersections[1,1] - intersections[2,1]))
      pt <- st_point(intersections[1, ])
    }
    
    r1 <- (radius_i * 180/pi + 90) * pi/180
    r2 <- (radius_i * 180/pi - 90) * pi/180
    v_intersect_1 <- c(long_distance * cos(r1), long_distance * sin(r1))
    v_intersect_2 <- c(long_distance * cos(r2), long_distance * sin(r2))
    a1 <- get_angle(v_extension, v_intersect_1)
    a2 <- get_angle(v_extension, v_intersect_2)
    if(a1<a2){
      r_cut <- r1
    }else{
      r_cut <- r2
    }
    
    new_point_intersect <- pt + c(long_distance * cos(r_cut), long_distance * sin(r_cut) ) 
    extend_line_intersect <- st_nearest_points(pt, new_point_intersect)%>%
      st_set_crs(27700)
    line_to_border_intersect <- st_intersection(extend_line_intersect , boundary_buffer)
    
  } else{
    if(nrow(intersections) > 2){
      radius_i <- atan2((intersections[nrow(intersections) - 1,2] - intersections[nrow(intersections) - 2,2]), 
                        (intersections[nrow(intersections) - 1,1] - intersections[nrow(intersections) - 2,1]))
      pt <- st_point(intersections[nrow(intersections) - 1, ])
    } else{
      radius_i <- atan2((intersections[nrow(intersections),2] - intersections[nrow(intersections) - 1,2]), 
                        (intersections[nrow(intersections),1] - intersections[nrow(intersections) - 1,1]))
      pt <- st_point(intersections[nrow(intersections), ])
      
    }
    r1 <- (radius_i * 180/pi + 90) * pi/180
    r2 <- (radius_i * 180/pi - 90) * pi/180
    v_intersect_1 <- c(long_distance * cos(r1), long_distance * sin(r1))
    v_intersect_2 <- c(long_distance * cos(r2), long_distance * sin(r2))
    a1 <- get_angle(v_extension, v_intersect_1)
    a2 <- get_angle(v_extension, v_intersect_2)
    if(a1<a2){
      r_cut <- r1
    }else{
      r_cut <- r2
    }
    
    new_point_intersect <- pt + c(long_distance * cos(r_cut), long_distance * sin(r_cut) ) 
    extend_line_intersect <- st_nearest_points(pt, new_point_intersect)%>%
      st_set_crs(27700)
    line_to_border_intersect <- st_intersection(extend_line_intersect , boundary_buffer)
  }
  
  
  pt2 <- pt + c(long_distance * cos(r_cut), long_distance * sin(r_cut) ) 
  pt3 <- pt - c(long_distance * cos(r_cut), long_distance * sin(r_cut) ) 
  
  intersect_l <- st_nearest_points(pt2,pt3) %>% st_set_crs(27700)
  
  # cut the edges
  boundary_buffer_edges <- st_cast(boundary_buffer, "LINESTRING")
  
  #  create a polygon to cut the boundary as new pipe
  surrounding <- st_union(extend_line_intersect, extend_line) %>%
    st_cast("MULTIPOINT") %>%
    st_union() %>%
    st_convex_hull() %>%
    st_buffer(0)
  
  pipes_to_use <- st_intersection(surrounding, boundary_buffer_edges)
  
  # clip <- st_difference(boundary_buffer, intersect_l) %>%
  #   st_cast("LINESTRING")
  # # cut by extend line from pipes across properteis to boarder
  # if(st_intersects(clip[1], st_buffer(extend_line, 1), sparse=FALSE)){
  #   clip2<- st_difference(clip[1], extend_line) %>%
  #     st_cast("LINESTRING")
  # } else{
  #   clip2<- st_difference(clip[2], extend_line) %>%
  #     st_cast("LINESTRING")
  # }
  # # cut by extend line from interestion to the boundary 
  # if(st_intersects(clip2[1], st_buffer(extend_line_intersect, 1), sparse=FALSE)){
  #   pipes_to_use <- clip2[1]
  # } else{
  #   pipes_to_use <- clip2[2]
  # }
  
  final_pipe<- st_union(pipes_to_use, midpoint_to_sewer) 
  
  pipesUnderProps$geometry <- final_pipe
  
  to_export <- pipesUnderProps %>%
    st_as_sf %>%
    st_set_crs(27700) %>% 
    st_simplify(dTolerance = 0.0000000001) %>%
    mutate(direction = direction,
           type = type,
           position = position,
           nearest_sewer_type = nearest_sewer_type
    )
  
  return(to_export)
  
}


performGeomOperations <- function(df, grouping, type = "multipolygon") {
  
  # This speeds up doing geometry operations on dataframes that have
  # a VERY large amount of groups.
  # Does some string operations to slice up the df into it's groups
  
  # Hardcoded to group by pipegroup currently
  # Also hardcoded to convert to linestrings (could be anything though)
  
  # The two functions for either linestring or to do multipols
  
  restoreLineString <- function(stringhere) {
    
    ## Function to conver to linestring (this could be anything)
    
    stringhere %>%     
      stri_replace_all(regex = 'c\\(|\\)', replacement =  '') %>% 
      #stri_replace(regex = '\\)', replacement = '') %>% 
      stri_split(regex = ", ", simplify = T) %>% 
      as.numeric() %>% 
      matrix(ncol = 2, byrow = FALSE) %>% 
      st_linestring()
    
  }
  
  restoreMultiPol <- function(stringhere) {
    
    ## Function to conver to linestring (this could be anything)
    
    eval(parse(text=stringhere)) %>% 
      lapply(function(x) x %>% 
               lapply(function(x) x %>%
                        as.numeric() %>% 
                        matrix(ncol = 2, byrow = FALSE))) %>% 
      st_multipolygon()
    
  }
  
  
  # # # Start doing it
  
  crsinfo <- st_crs(df)
  
  ## Split up
  splittest <- df %>% 
    select(!!sym(grouping), geometry) %>% 
    as_tibble() %>% 
    mutate(geometry = as.character(geometry)) %>% 
    group_by(!!sym(grouping)) %>% 
    group_split()
  
  
  
  if(type == "linestring") {
    
    ## Apply above function
    print("Converting text into linestrings")
    testresults <- pblapply(splittest, function(x) {
      
      x$geometry %>% 
        lapply(restoreLineString) %>% 
        st_sfc
    } 
    )
  } else if (type == "multipolygon"){
    
    ## Apply above function
    print("Converting text into mps")
    testresults <- pblapply(splittest, function(x) {
      
      x$geometry %>% 
        lapply(restoreMultiPol) %>% 
        st_sfc
    } 
    )
  }
  
  
  ## Now do the combine and centroid on each list element
  print("Combining geometry and then getting the centroid (2 steps)")
  centroidofeachgroup <- pblapply(testresults, st_combine) %>% 
    pblapply(st_centroid)
  
  
  # Join on our processed groups to their group names (everything should be in ORDER)
  # This we just want to flatten into the groups
  splittest2 <- splittest %>% 
    lapply(function(x) dplyr::slice(x, 1)
    ) %>% 
    rbindlist()
  
  # concat all the centroids and then assign to geometry col
  splittest2$geometry <- do.call(c, centroidofeachgroup)
  
  # Make into sf object and then set crs
  splittest2 <- splittest2 %>% 
    as_tibble() %>%
    st_as_sf() %>% 
    st_set_crs(crsinfo)
  
  # Return it
  splittest2
  
}

# draw T shape across the semi detached properties
drawTShapeSemiDetached <- function(centroids, n24, position = "middle", distance=10, chunksize = 1, fold_out){
  # direction 
  if (is.null(centroids)) {
    return()
  } else if(is.null(n24)) {
    return()
  }
  
  centroids <- isSameGrid(centroids)
  ActualX <- getActualX(centroids)
  ActualY <- getActualY(centroids)
  
  
  #maxi <- ceiling(dim(centroids)[1] / 100)
  
  # find the semi deteched properties and draw the pipe across the property
  pipesUnderProps <-  centroids %>%
    group_by(propgroup) %>%
    filter(n()>1) %>%
    mutate(geometry = st_union(geometry)) %>%
    mutate(geometry = st_cast(geometry, 'LINESTRING')) %>%
    select(-fid, -index) %>%
    distinct(.keep_all = T) 
  
  
  # shift the pipes across the centroid
  # move points infront / behind property
  slope<- (sapply(pipesUnderProps$geometry, "[[", 1) - sapply(pipesUnderProps$geometry, "[[", 2)) / 
    (sapply(pipesUnderProps$geometry, "[[", 3) - sapply(pipesUnderProps$geometry, "[[", 4))
  radius <- atan(slope)
  
  pipesUnderProps$geometry <- pipesUnderProps$geometry - t(matrix(c(-distance * sin(radius), distance * cos(radius)), ncol = 2))
  
  # get the midpoint of pipes
  midpoints <- pipesUnderProps
  midpoints$geometry <- st_centroid(midpoints$geometry) %>%
    st_set_crs(27700)
  
  # find the nearest sewer to the midpoint, and connect them
  nearest_sewer <- n24[st_nearest_feature(midpoints, n24), ]$geometry
  nearest_point <- mapply(st_nearest_points, midpoints$geometry, nearest_sewer)
  midpoints$geometry <- nearest_point
  
  # combine the pipes
  to_export <- rbind_list( pipesUnderProps, midpoints) %>%
    st_as_sf %>%
    st_set_crs(27700) %>% 
    st_simplify(dTolerance = 0.0000000001)
  
  
  # generate pipes across 2 centroids
  to_export %>% 
    st_write(paste0(fold_out, "/TShape-", position, "-", distance, ".shp"))
  
  
  return(to_export)
  
}

# draw T-shape pipes behind properties by given 2 super group
# group_ids: c(group_id_1, group_id_2)
drawTShapeByGroup <- function(buildings, centroids, n24, group_ids, distance=1, chunksize = 1, fold_out){
  # direction 
  if (is.null(centroids)) {
    return()
  } else if(is.null(n24)) {
    return()
  } else if(is.null(buildings)){
    return()
  }
  
  centroids <- isSameGrid(centroids)
  ActualX <- getActualX(centroids)
  ActualY <- getActualY(centroids)
  
  centroids<- centroids %>%
    filter(propgroup %in% group_ids) 
  
  # draw lines across all centroids
  pipesUnderProps <-  centroids %>%
    #filter(propgroup %in% group_ids) %>%
    #group_by(propgroup) %>%
    mutate(geometry = st_union(geometry)) %>%
    mutate(geometry = st_cast(geometry, 'MULTILINESTRING')) %>%
    select(-fid, -index) %>%
    distinct(.keep_all = T) 
  
  
  # group property polygon by each group
  buildingblock <-  buildings %>%
    filter(propgroup %in% group_ids) %>%
    group_by(propgroup) %>%
    mutate(geometry = st_union(geometry))%>%
    distinct(.keep_all = T) %>%
    st_set_crs(27700)
  
  # get the largest distance between centriods and border
  # TODO: calcuate the max distance by given radius
  boundary <- getPropertyBoundary(buildingblock)
  # boundaryCentriods <- st_centroid(boundary)
  
  #pipesUnderProps$geometry <- pipesUnderProps$geometry - t(matrix(c(-distance * sin(radius), distance * cos(radius)), ncol = 2))
  
  
  # find the mid point between 2 properties
  polygon1 <- boundary[1]
  polygon2 <- boundary[2]
  
  shortest_distance <- st_geometry(st_nearest_points(polygon1, polygon2))
  
  # slope of the shortest line, to find shift distance
  slope_shortest <- (shortest_distance[[1]][4] -  shortest_distance[[1]][3])/  (shortest_distance[[1]][2] -  shortest_distance[[1]][1])
  angle_shortest_1 <- atan(slope_shortest) * 180 / pi + 90
  angle_shortest_2 <- atan(slope_shortest) * 180 / pi - 90
  
  midpoint <- getMidPointBetweenPolygons(polygon1, polygon2)
  
  # find the nearest sewer of midpoint and draw line
  nearest_sewer <- n24[st_nearest_feature(midpoint, n24), ]$geometry
  midpoint_to_sewer <- st_nearest_points(midpoint, nearest_sewer)
  
  # connect midpoint to pipes across properties
  slope_midpoint_to_sewer <- (midpoint_to_sewer[[1]][4] - midpoint_to_sewer[[1]][3]) / (midpoint_to_sewer[[1]][2] - midpoint_to_sewer[[1]][1])
  # get the direction of midpoint to sewer 
  direction <- slope_midpoint_to_sewer/abs(slope_midpoint_to_sewer)
  radius <- atan(slope_midpoint_to_sewer)
  # choose the correct direction to extend 
  if( abs(180*radius/pi - angle_shortest_1) > abs(180*radius/pi - angle_shortest_2)){
    angle_to_extend <- angle_shortest_1
  }else{
    angle_to_extend <- angle_shortest_2
  }
  
  # set a super long distance to find a maximum intersetion
  long_distance <- 1000
  # generate the endpoint of extended line on opposite direction
  radius <- angle_to_extend * pi / 180
  
  # generate extended lines for each centroid 
  # ...and find the longest intersection line as the distance to move
  d_to_move <- 0
  boundary <- boundary %>% st_combine()
  for(centroid in centroids$geometry){
    # get the extended line
    extended_endpoint <-   centroid + c(direction * long_distance * sin(radius), long_distance * cos(radius) * direction) 
    extend_line <-st_nearest_points(centroid, extended_endpoint) %>% st_set_crs(27700)
    
    # calculate the length of intersection and remove the unit
    line_length <- as.double(st_length(st_intersection(extend_line, boundary)))
    # save the max length
    if(line_length > d_to_move){
      d_to_move <- line_length
    }
    
  }
  
  # shift the pipe across centroids by calcuated distance and given buffer
  d_to_move <- d_to_move + distance
  pipesUnderProps$geometry <- pipesUnderProps$geometry + 
    c(direction * d_to_move * sin(radius), d_to_move * cos(radius) * direction) 
  pipesUnderProps$geometry <- st_set_crs(pipesUnderProps$geometry, 27700)
  # connect midpoint to the shifted pipes
  midpoint_to_pipes <- st_nearest_points(midpoint, pipesUnderProps$geometry)
  
  
  
  # combine all pipes
  segments <- data.frame(geometry = c(pipesUnderProps$geometry, midpoint_to_pipes, midpoint_to_sewer))
  pipesUnderProps$geometry <- st_combine(segments$geometry)
  return(pipesUnderProps)
  
}



# Testing Area ------------------------------------------------------------


# n24sample <- "I:/Projects/Client/1883/Analytics/LineScript/PipeAges/WholeAssetBase/Sample/Biggern24.shp"
# 
# buildingsample <- "I:/Projects/Client/1883/Analytics/LineScript/PipeAges/WholeAssetBase/Sample/BiggerBuildings.shp"
# 
# shapeinfo <- loadin(buildingpath = buildingsample,
#                     n24path = n24sample,
#                     crs = 27700)
# 
# resultsareinhere <- notionalfunction(centroids = shapeinfo[["Centroids"]][1:100, ],
#                                      n24pipes = shapeinfo[["n24"]])
# 
# resultsareinhere

# getattributesandlength(resultsareinhere, shapeinfo[["n24"]])


