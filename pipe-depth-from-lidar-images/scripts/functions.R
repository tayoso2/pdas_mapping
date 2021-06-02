validate_shape <- function(path){
  # Tests for mandatory shape files in a given folder given the ".shp" file name
  
  # An Esri Shape file is actualy a set of several files, three of which are mandatory:
  #
  #   1. Shape File  - (shp) - Contains the geometry features themselves
  #   2. Shape Index - (shx) - Positional index of the geometry objects for quick searching
  #   3. Attributes  - (dbf) - Table of attribute data for the geometry objects
  #   
  #   There are up to ten other files providing secondary information, such as the
  #   geometry projection (prj), that could be present. This function only tests
  #   for the mandatory files.
  #
  # args
  #   path   A character scalar. The full path of the Shape File (shp) to validate
  #
  # returns
  #   A logical scalar. Does the folder contain the mandatory shape files?
  #
  mandatory  <- c(".shp", ".shx", ".dbf")
  
  fold_shape <- base::dirname(path)
  file_shape <- tools::file_path_sans_ext(base::basename(path))
  file_shape <- base::paste0(file_shape, mandatory)
  path_shape <- base::file.path(fold_shape, file_shape)
  
  base::all(base::sapply(path_shape, base::file.exists))
}

east_north_ngr <- function(east, north, digits = 4, space = FALSE){
  # Converts a pair of easting and northing coordinates to the 100km Ordnance
  # Survey National Grid reference (NGR)
  #
  # Args
  #   east     Numeric scalar. Distance east of OSNG36 false origin
  #   north    Numeric scalar. Distance north of OSNG36 false origin
  #   digits   Numeric scalar. Precision of the grid reference output
  #   space    Logical scalar. Include white space in NGR output?
  #
  # Returns
  #   Character scalar
  #
  # Example
  #   east_north_ngr(651409, 313177, 8, TRUE)    # "TG 5140 1317"
  #   east_north_ngr(651409, 313177, 4, FALSE)   # "TG5113"
  #
  # Note
  #   The 'digits' parameter sets the total width of the numeric component of
  #     the final grid reference, so digits = 4 produces 'AB 12 34' while
  #     digits = 8 produces 'AB 1234 5678'. It is not related to the precision
  #     of the individual easting and northing coordinates
  #
  # Check for correct precision
  if(digits > 16 | digits %% 2 != 0){
    stop("Invalid precision, ", digits, ", must be even and in the range 0 to 16")
  }
  
  # Remove any decimal points
  e <- base::floor(east)
  n <- base::floor(north)
  
  # Get 100km grid reference letters
  e100 <- base::floor(e / 100000)
  n100 <- base::floor(n / 100000)
  gr1  <- (19 - n100) - (19 - n100) %% 5 + floor((e100 + 10) / 5)               # 18 using example coords
  gr2  <- ((19 - n100) * 5) %% 25 + e100 %% 5                                   #  6 using example coords
  # Need to add 1 because R index starts at 1 not zero
  gr1  <- gr1 + 1
  gr2  <- gr2 + 1
  # Correct for missing 'I' in grid references
  if(gr1 > 7) gr1 <- gr1 + 1
  if(gr2 > 7) gr2 <- gr2 + 1
  
  # Remove 100km grid references and set precision
  e <- base::floor((e %% 100000) / (10 ^ (5 - (digits / 2))))
  n <- base::floor((n %% 100000) / (10 ^ (5 - (digits / 2))))
  
  # Add leading zeros where needed to maintain reference width
  e <- base::formatC(e, width = digits / 2, flag = "0")
  n <- base::formatC(n, width = digits / 2, flag = "0")
  
  # Format grid reference
  ngr <- base::paste0(LETTERS[gr1], LETTERS[gr2])
  if(space){
    ngr <- base::paste(ngr, e, n)
  }else{
    ngr <- base::paste0(ngr, e, n)
  }
  return(ngr)
}

bbox_to_poly <- function(geo){
  # Converts bounding box to rectangular polygon
  #
  # Args
  #   geo   sfc geometry object
  #
  # Returns
  #   sfc geometry object
  #
  require(sf)
  bbox      <- sf::st_bbox(geo)
  
  east_min  <- base::as.integer(bbox$xmin)
  east_max  <- base::as.integer(bbox$xmax)
  north_min <- base::as.integer(bbox$ymin)
  north_max <- base::as.integer(bbox$ymax)
  
  mat_poly  <- base::matrix(c(east_min, north_min,
                              east_max, north_min,
                              east_max, north_max,
                              east_min, north_max,
                              east_min, north_min),
                            ncol = 2, byrow = TRUE)
  list_poly <- base::list(mat_poly)
  
  geo_poly  <- sf::st_polygon(list_poly)
  geo_poly  <- sf::st_sfc(geo_poly)
  
  return(geo_poly)
}

make_point <- function(xy, crs = 27700){
  # Creates an sfc POINT object from a pair of coordinates
  #
  # Args
  #   xy   Numeric vector containing 2 values
  #
  # Returns
  #   sfc POINT object
  #
  point <- sf::st_point(xy)
  point <- sf::st_sfc(point)
  point <- sf::st_as_sf(point)
  
  names(point)           <- "geometry"
  sf::st_geometry(point) <- "geometry"
  sf::st_crs(point)      <- crs
  
  return(point)
}

poly_contains_point <- function(point, polygons){
  # Check if a POINT is contained by any of a set of POLYGON
  #
  # TODO: Vectorise this function so you don't need the 'rowwise' modifier
  #
  # Args
  #   point      sf geometry objects
  #   polygons   set of sf geometry objects in data.frame
  #
  # Returns
  #   Numeric scalar
  #
  tmp <- sf::st_contains(polygons, point) 
  tmp <- base::as.numeric(tmp)
  idx <- base::which(!base::is.na(tmp))
  
  if(base::length(idx) == 0){
    return(NA_integer_)
  }else{
    return(base::min(idx))
  }
}