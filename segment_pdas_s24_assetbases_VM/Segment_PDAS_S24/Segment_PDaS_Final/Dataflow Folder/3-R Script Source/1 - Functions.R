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
# dependent functions
splitLines <- function(spobj,
                       dist,
                       start = T,
                       sf = F) {
  xydf <- coordBuild(spobj)
  if (start == F) {
    xydf <- xydf[rev(rownames(xydf)), ]
  }
  spoints <- split(xydf, dist)
  linelist <- list()
  lineslist <- list()
  id <- 1
  if (!sf) {
    j <- 1
    for (i in 1:(nrow(spoints) - 1)) {
      linelist[j] <- Line(spoints[c(i, i + 1), c(1:2)])
      j = j + 1
      if (spoints[i + 1, 3] == 1) {
        lineslist[id] <- Lines(linelist, ID = id)
        id = id + 1
        linelist <- list()
        j = 1
      }
    }
    return(SpatialLinesDataFrame(SpatialLines(lineslist), data = data.frame(id = 0:(
      length(lineslist) - 1
    ))))
  } else {
    start <- 1
    for (i in 1:(nrow(spoints) - 1)) {
      if (spoints[i + 1, 3] == 1) {
        lineslist[[id]] <-
          sf::st_linestring(as.matrix(spoints[c(start:(i + 1)), c(1:2)], ncol = 2))
        id <- id + 1
        start <- i + 1
      }
    }
    return(sf::st_sf(
      id = 1:length(lineslist),
      geom = sf::st_sfc(lineslist)
    ))
  }
}


coordBuild <- function(spobj) {
  if ("LINESTRING" %in% class(spobj)) {
    return(setNames(data.frame(sf::st_zm(
      sf::st_coordinates(spobj)
    )), c("x", "y")))
  }
  if (class(spobj) %in% c("SpatialLinesDataFrame",    "SpatialLines")) {
    coords <-
      lapply(spobj@lines, function(x)
        lapply(x@Lines, function(y)
          y@coords))
    coords <- ldply(coords, data.frame)
    names(coords) <- c("x", "y")
    return(coords)
  }
  if (class(spobj) %in% c("SpatialPolygonsDataFrame", "SpatialPolygons")) {
    coords <-
      lapply(spobj@polygons, function(x)
        lapply(x@Polygons, function(y)
          y@coords))
    coords <- ldply(coords, data.frame)
    names(coords) <- c("x", "y")
    return(coords)
  }
  if (class(spobj) == "data.frame") {
    if (all(c("x", "y") %in% tolower(names(spobj)))) {
      return(spobj[c("x", "y")])
    }
    else{
      stop("Dataframe provided does not have x, y columns")
    }
  }
  stop(
    "Class of spatial argument is not supported. Need SpatialLinesDataFrame or SpatialPolygonsDataFrame"
  )
}


split <- function(xydf, dist){
  modck <- function(change, mod){
    if(change<0){
      return(mod*-1)
    }
    else{
      return(mod)
    }
  }
  x <- c()
  y <- c()
  end <- c()
  rem <- 0
  for(i in 1:nrow(xydf)){
    if(i == 1){
      x <- c(x, xydf$x[i])
      y <- c(y, xydf$y[i])
      end <- c(end,1)
    }
    if(i != 1){
      cx <- xydf$x[i] - xydf$x[i-1]
      cy <- xydf$y[i] - xydf$y[i-1]
      if(cx & cy != 0){
        len <- sqrt((cx^2) + (cy^2)) + rem
        segs <- len %/% dist
        if(segs == 0){
          rem <- len
        }
        else{
          m <- cx/cy
          ymod <- dist / (sqrt((m^2)+1))
          xmod <- ymod * abs(m)
          yremsub <- rem / (sqrt((m^2)+1))
          xremsub <- yremsub * abs(m)
          xmod <- modck(cx, xmod)
          ymod <- modck(cy, ymod)
          xremsub <- modck(cx, xremsub)
          yremsub <- modck(cy, yremsub)
          xnew <- seq(xydf$x[i-1] - xremsub, xydf$x[i-1] + (xmod * segs), by = xmod)[-1]
          ynew <- seq(xydf$y[i-1] - yremsub, xydf$y[i-1] + (ymod * segs), by = ymod)[-1]
          if(length(xnew) != length(ynew)){
            if(abs(length(xnew) - length(ynew)) > 1) stop("Error found in new sequence. Code needs to be reviewed...")
            if(length(xnew) < length(ynew)){
              xnew <- c(xnew, xydf$x[i-1] + (xmod * segs))
            } else {
              ynew <- c(ynew, xydf$y[i-1] + (ymod * segs))
            }
          }
          rem<-sqrt((xydf$x[i] - tail(xnew,1))^2 + (xydf$y[i] - tail(ynew,1))^2)
          x <- c(x, xnew)
          y <- c(y, ynew)
          end <- c(end, rep(1, length(xnew)))
        }
      }
      if(cx != 0 & cy == 0){
        len <- cx + rem
        segs <- len %/% dist
        if(segs == 0){
          rem <- len
        }
        else{
          xmod <- dist
          ymod <- 0
          xmod <- modck(cx, xmod)
          xremsub <- modck(cx, xremsub)
          yremsub <- 0     
          xnew <- seq(xydf$x[i-1] - rem, xydf$x[i-1] + (xmod * segs), by = xmod)[-1]
          ynew <- rep(xydf$y[i-1], segs)
          rem <- xydf$x[i] - tail(xnew,1)
          x <- c(x, xnew)
          y <- c(y, ynew)
          end <- c(end, rep(1, length(xnew)))
        }
      }
      if(cx == 0 & cy != 0){
        len <- cy + rem
        segs <- len %/% dist
        if(segs == 0){
          rem <- len
        }
        else{
          xmod <- 0
          ymod <- dist
          xmod <- modck(cx, xmod)
          xremsub <- modck(cx, xremsub)
          yremsub <- 0    
          xnew <- rep(xydf$x[i-1], segs) 
          ynew <- seq(xydf$y[i-1] - rem, xydf$y[i-1] + (ymod * segs), by = ymod)[-1]
          rem <- xydf$y[i] - tail(ynew,1)
          x <- c(x, xnew)
          y <- c(y, ynew)
          end <- c(end, rep(1, length(ynew)))
        }
      }
      x <- c(x, xydf$x[i])
      y <- c(y, xydf$y[i])
      if(i != nrow(xydf)){
        end <- c(end, 0)    
      }
      else{
        end <- c(end, 1) 
      }
    }
  }
  return(data.frame(x = x, y = y, end = end))
}
