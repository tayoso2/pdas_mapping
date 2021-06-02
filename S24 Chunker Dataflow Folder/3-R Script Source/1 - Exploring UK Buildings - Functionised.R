# All packages are loaded as part of "3 - Chunker.R"


# Data load ---------------------------------------------------------------

loadin <- function(buildingpath, n24path, crs = 27700) {
  
  # Load in buildings
  buildings <- st_read(buildingpath, stringsAsFactors = FALSE) %>% 
    st_zm() # remove z coordinate
  st_crs(buildings) <- crs
  
  # Create building centroids
  buildingcentroids <- st_centroid(buildings)
  
  # Load in n24's
  n24 <- st_read(n24path, stringsAsFactors = FALSE) %>% 
    st_zm() # remove z coordinate
  st_crs(n24) <- crs
  
  listypop <- list(buildings, buildingcentroids, n24)
  names(listypop) <- c("Buildings", "Centroids", "n24")
  
  return(listypop)
}



# Base Functions ----------------------------------------------------------

findnearestneighbourandgetline <- function(centroids, n24pipes, crs = 27700) {
  
  # centroids should be a series of coordinates for what a "building centroid" is
  # n24pipes should be n24 pipes (lines) with the YEAR attribute given by a column (currently hardcoded to 
  # Unbnd_Y)
  
  
  # This will return the index of the nearest feature between the centroid of a building and the nearest n24 pipe
  
  fake_pdas_indices <- st_nearest_feature(centroids, n24pipes)
  
  # Assign the nearest n24 index to each pipe from calculated above
  # Also give every centroid of each building an index

  centroids <- centroids %>% 
    mutate(nearest_n24 = fake_pdas_indices,
           centroid_index = 1:dim(centroids)[1])

  # This is what I told Will to do
  # Assign a line matching centroid to nearest pipe

  # This si much faster
  
  return(centroids %>% 
    mutate(geometry = 
      st_nearest_points(centroids, 
                        fake_pdas_indices %>% 
                          as_tibble() %>% 
                          left_join(n24pipes %>% 
                                      mutate(value = row_number())) %>% 
                          st_as_sf,
                        pairwise = T)
    ) %>% 
    st_as_sf)

  
  # return(centroids)
  
  
}


# Attribute Grabbing ------------------------------------------------------

getattributesandlength <- function(s24pdaslines, n24pipes, nearestindexcol = "nearest_n24") {
  

  
  s24pdaslines %>% 
    left_join(n24pipes %>% 
                st_drop_geometry() %>% 
                select(c("Unbnd_Y", "Unbnd_M", "Unbnd_D", "Tag", "road_flag", "Type", "index")) %>% 
                rename(    "ConNghYea" = Unbnd_Y,
                           "ConNghMat" = Unbnd_M,
                           "ConNghDia" = Unbnd_D,
                           "ConNghTag" = Tag,
                           "road_flg"  = road_flag,
                           "ConNghTyp" = Type),
              by = c("nearest_n24" = "index"))
  
  
}



# Basic Line Drawing ------------------------------------------------------

extractcoordinates <- function(spatialdf, pointnames = c("1","2"), textcol = FALSE, extractcol = "tempgeometry") {
  
  # Insert doc about what the following regex expressions do
  
  if (st_geometry_type(spatialdf)[1] == "LINESTRING" | 
      st_geometry_type(spatialdf)[1] == "MULTILINESTRING") {
    
    print("Detected LINESTRING geometry")
    regexvector <- "(?:\\()(\\d+\\.\\d+|\\d+) (\\d+\\.\\d+|\\d+), (\\d+\\.\\d+|\\d+) (\\d+\\.\\d+|\\d+)"
    geotype <- "LINESTRING"
    cols <- c(paste0("x-",pointnames[1]),
              paste0("y-",pointnames[1]),
              paste0("x-",pointnames[2]),
              paste0("y-",pointnames[2]))
    
  } else if (st_geometry_type(spatialdf)[1] == "POINT" |
             st_geometry_type(spatialdf)[1] == "MULTIPOINT") {
    
    print("Detected POINT geometry")
    regexvector <- "(?:\\()(\\d+\\.\\d+|\\d+) (\\d+\\.\\d+|\\d+)"
    geotype <- "POINT"
    cols <- c(paste0("x-",pointnames[1]),
              paste0("y-",pointnames[1]))
    
  } else {
    
    print("Can't proceed as the geometry is not linestring or point")
    return(NULL)
    
  }
  
  if (textcol == F) {
    # Turn the geometry into text
    spatialdf$tempgeometry <- st_as_text(spatialdf$geometry) 
    
    spatialdf <- spatialdf %>% 
      rename(!!sym(extractcol) := tempgeometry) %>% 
      select(-geometry)
  }
  
  spatialdf %>% 
    as_tibble() %>% # must first actually turn it into a tibble to extract anything 
    extract(!!sym(extractcol), 
            into = cols,
            regex = regexvector) %>%   # extract!
    mutate_at(vars(cols), as.numeric)
  
}

extractcoordinatesnongeom <- function(df, pointnames = c("1","2"), extractcol = "tempgeometry") {
  
  # Insert doc about what the following regex expressions do
  
  if (str_detect(df[1, extractcol], "LINESTRING") == TRUE) {
    
    print("Detected LINESTRING geometry")
    regexvector <- "(?:\\()(\\d+\\.\\d+|\\d+) (\\d+\\.\\d+|\\d+), (\\d+\\.\\d+|\\d+) (\\d+\\.\\d+|\\d+)"
    geotype <- "LINESTRING"
    cols <- c(paste0("x-",pointnames[1]),
              paste0("y-",pointnames[1]),
              paste0("x-",pointnames[2]),
              paste0("y-",pointnames[2]))
    
  } else if (str_detect(df[1, extractcol], "POINT") == TRUE) {
    
    print("Detected POINT geometry")
    regexvector <- "(?:\\()(\\d+\\.\\d+|\\d+) (\\d+\\.\\d+|\\d+)"
    geotype <- "POINT"
    cols <- c(paste0("x-",pointnames[1]),
              paste0("y-",pointnames[1]))
    
  } else {
    
    print("Can't proceed as the geometry is not linestring or point")
    return(NULL)
    
  }
  
  
  df %>% 
    as_tibble() %>% # must first actually turn it into a tibble to extract anything 
    extract(!!sym(extractcol), 
            into = cols,
            regex = regexvector)  %>%   # extract!
    mutate_at(vars(cols), as.numeric)
  
  
}

createnewlinecoords <- function(df, gradcoords = c("x-1","x-2","y-1","y-2"), invgrad = F, centroidcoords = c("x-1", "y-1"), newcoordsname = "newcoords", length = 100) {
  
  # This will create a line in whatever directino you want (by calculating a gradient)
  # Grad coords will give gradient
  # Centroid coords give where the line starts from
  # Length tells you how long to make it (depends on CRS)
  
  # Calculate the gradient and how much x / y values will change
  df <- df %>% 
    rowwise %>% 
    mutate_at(c(gradcoords), as.numeric) %>% 
    mutate(gradient = (!!sym(gradcoords[4]) - !!sym(gradcoords[3])) / (!!sym(gradcoords[2]) - !!sym(gradcoords[1]))) %>% 
    mutate(gradient = ifelse(invgrad == T, -1/gradient, gradient)) %>% 
    mutate(changex = sqrt((length^2)/((gradient^2)+1)), # Work out change in x / y based on what r you want to move away
           changey = changex * gradient) %>% 
    ungroup
  # If this is gonna be positive or negativeeeeee GOES HERE
  
  # Create the new coordinates for the end of the line
  df %>% 
    mutate(!!sym(paste0("x-", newcoordsname)) := !!sym(centroidcoords[1]) + changex,
           !!sym(paste0("y-", newcoordsname)) := !!sym(centroidcoords[2]) + changey)
  
  
}

createline <- function(df, crs = 27700, ...) {
  
  
  makeline3 <- function(...) {
    
    # dots must be divisible vy 4
    
    dots <- unlist(list(...))
    
    st_linestring(matrix(dots, length(dots) / 2, 2))
  }
  
  # Create actual linestring - to be passed to pmap
  
  dots <- unlist(list(...))
  #print(dots)
  
  cols <- c(paste0("x-",dots), paste0("y-", dots))
  #print(cols)
  
  # Create the line by passing through centroid coords and new coords
  reduceddf <- df %>%
    select(cols)
  
  
  
  repeats <- paste0("..", seq(1,length(dots)*2 -1, 1), ",", collapse = "")
  
  repeats2 <- paste0(repeats,"..",length(dots)*2)
  
  f <- paste0("pmap(reduceddf, ~makeline3(",repeats2,"))", collapse = "")
  
  t <- as.lazy(f, environment())
  
  lazy_eval(t) %>% 
    st_as_sfc(crs = crs)
  
  
}

findoverlap <- function(linesdf, buildingsdf) {
  
  # Note these should be in order by their ID's (row 1 from lines = row 1 from buildings)
  
  # Intersection function for pmap (could do anon function tbh)
  intersects <- function(geom1, geom2) {
    if (st_intersects(geom1, geom2, sparse = F)) {
      st_intersection(geom1, geom2)
    } else {
      geom1
    }
  }
  
  # This assumes that building geometry is in polygon form and must be converted to linestrings
  intersectionpoints <- pmap(.l = list(linesdf, st_cast(buildingsdf, "MULTILINESTRING")$geometry), .f = intersects) %>% 
    st_as_sfc(crs = 27700)
  
  # is.na(st_dimension(intersectionpoints))
  
  
  intersectionpoints
  
}

# 
# shapeinfo <- loadin(buildingpath = "H:/Projects/PDaS/PipeAges/AssetShape/Belgrave St Barts Academy Buildings.shp",
#                      n24path = "H:/Projects/PDaS/PipeAges/AssetShape/Belgrave St Barts Academy N24.shp",
#                      crs = 27700)
# 
# 
# # Get centroid to sewer line
# centroidtosewer <- findnearestneighbourandgetline(centroids = shapeinfo[["Centroids"]],
#                                                n24pipes = shapeinfo[["n24"]],
#                                                crs = 27700)
# 
# 
# centroidtosewercoords <- extractcoordinates(spatialdf = centroidtosewer)
# 
# # First intercept with polygon
# newlinecoords <- createnewlinecoords(df = centroidtosewercoords)
# newlinecoords$buildingcross <- findoverlap(linesdf = createline(newlinecoords, crs = 27700, "1", "2"), buildingsdf = shapeinfo[["Buildings"]])
# 
# newlinecoords$buildingcross <- st_as_text(newlinecoords$buildingcross)
# maindf <- extractcoordinatesnongeom(df = newlinecoords, pointnames = c("buildcross-1"), extractcol = "buildingcross")
# 
# # Second intercept with polygon
# 
# newlinecoords2 <- createnewlinecoords(df = centroidtosewercoords, invgrad = T, newcoordsname = "newcoords2")
# newlinecoords2$buildingcross <- findoverlap(linesdf = 
#                                               createline(newlinecoords2, 27700, "1", "newcoords2"),
#                                             buildingsdf = shapeinfo[["Buildings"]])
# newlinecoords2$buildingcross <- st_as_text(newlinecoords2$buildingcross) 
# maindf <- maindf %>% 
#   bind_cols(extractcoordinatesnongeom(df = newlinecoords2, pointnames = c("buildcross-2"), extractcol = "buildingcross") %>% 
#               select_at(vars(contains("buildcross-2"))))
# 
# # Draw newa dogs leg
# 
# # Need to be able to add 2 on to the length (crossover + 2)
# 
# maindf <- createnewlinecoords(df = maindf, gradcoords = c("x-1","x-2","y-1","y-2"), invgrad = F, centroidcoords = c("x-buildcross-1", "y-buildcross-1"), newcoordsname = "3", length = 2)
# maindf <- createnewlinecoords(df = maindf, gradcoords = c("x-1","x-2","y-1","y-2"), invgrad = T, centroidcoords = c("x-buildcross-2", "y-buildcross-2"), newcoordsname = "perp", length = 2)
# 
# maindf <- maindf %>% 
#   mutate(`x-4` = `x-3` + (`x-3` - `x-perp`),
#          `y-4` = `y-3` + (`y-3` - `y-perp`))
# 
# 
# 
# 
# 
# 
# 
# 
# linescalculated <- createline(df = maindf, crs = 27700, "1", "3", "4")
# 
# 
# 
# ## PLOTTING CHECKS
# 
# 
# st_crs(linescalculated) <- 27700
# 
# #st_write(linescalculated, "wtfisgoingon.shp")
# 
# # Check by doing a colour coded plot
# #plot(linescalculated[1:dim(linescalculated)[1], ])
# 
# # This allows tmap to work
# tmap_mode('view')
# # This plots it onto tmap with the correct basemap
# qtm(st_cast(shapeinfo[["Buildings"]][272,], "MULTILINESTRING"),
#     basemaps = "Esri.WorldStreetMap")
# 
# qtm(linescalculated,
#     basemaps = "Esri.WorldStreetMap")


