# Helper ------------------------------------------------------------------

rollmeTibble <- function(df1, values, col1, method) {
  
  df1 <- df1 %>% 
    mutate(merge = !!sym(col1)) %>% 
    as.data.table()
  
  values <- enframe(values, name = NULL) %>% 
    mutate(band = row_number()) %>% 
    mutate(merge = as.numeric(value)) %>% 
    as.data.table()
  
  setkeyv(df1, c('merge'))
  setkeyv(values, c('merge'))
  
  Merged = values[df1, roll = method]
  
  Merged
}



# This is from sf because we won't update the TEs

st_geometry_type_LOL = function(x, by_geometry = TRUE) {
  x = st_geometry(x)
  f = if (by_geometry)
    vapply(x, function(y) class(y)[2], "")
  else
    substring(class(x)[1], 5)
  factor(f, levels =
           c("GEOMETRY",
             "POINT",
             "LINESTRING",
             "POLYGON",
             "MULTIPOINT",
             "MULTILINESTRING",
             "MULTIPOLYGON",
             "GEOMETRYCOLLECTION",
             "CIRCULARSTRING",
             "COMPOUNDCURVE",
             "CURVEPOLYGON",
             "MULTICURVE",
             "MULTISURFACE",
             "CURVE",
             "SURFACE",
             "POLYHEDRALSURFACE",
             "TIN",
             "TRIANGLE"))
}





# Piece generation --------------------------------------------------------

sortShapes <- function(sfdf, XCount = 10, YCount = 10, bandsX = NULL, bandsY = NULL, buffer = 300) {
  
  # XCount = number of X bands
  # YCount = number of Y bands
  #
  # bandsX = Coordinates at the left most edge of each band in the X plane
  # bandsY = same
  #
  # buffer = increase the dimensions of the outer edges by this for any OVERLAYING shapes
  
  # Create indices to join back onto later
  sfdf <- sfdf %>% 
    mutate(index = row_number())
  
  # Don't calc centroids if they already exist
  if(st_geometry_type_LOL(sfdf, by_geometry = FALSE) == "POINT") {
    testcoords <- st_coordinates(sfdf)
  } else {
    testcoords <- st_centroid(sfdf) %>% 
      st_coordinates()
  }

  # Give coordinates an index to join on later
  tibbyt <- testcoords %>% 
    as_tibble() %>% 
    mutate(index = row_number())
  
  # Get the max / min coords
  # If you haven't supplied bands from another grid, the first part of the if
  # will calculate them
  if(is.null(bandsX)) {
    coords <- tibbyt %>% 
      mutate(maxX = max(X),
             minX = min(X),
             maxY = max(Y),
             minY = min(Y)) %>% 
      slice(1) %>% 
      select(maxX, minX, maxY, minY)
    
    # Increments
    incrementX <- (coords$maxX - coords$minX) / XCount
    incrementY <- (coords$maxY - coords$minY) / YCount
    
    # Bands
    bandsX <- vector()
    for(i in 1:XCount) {
      bandsX[i] <- coords$minX + (incrementX * (i-1))
    }
    
    bandsY <- vector()
    for(i in 1:YCount) {
      bandsY[i] <- coords$minY + (incrementY * (i-1))
    }
  
  
  } else {
    # If you're using passed bands, add a bit on to each end to make sure
    # you don't cut anything out from the overlay
    bandsX[1] <- bandsX[1] - buffer
    bandsX[length(bandsX)] <- bandsX[length(bandsX)] + buffer
    
    bandsY[1] <- bandsY[1] - buffer
    bandsY[length(bandsY)] <- bandsY[length(bandsY)] + buffer
  }

  


  # Rolling join X and Y coords into bands for the grid
  tibbyt <- rollmeTibble(tibbyt, bandsX, "X", Inf) %>% 
    rename(Xband = band)
  
  tibbyt <- rollmeTibble(tibbyt, bandsY, "Y", Inf) %>% 
    rename(Yband = band)
  
  tibbyt <- tibbyt %>%
    arrange(index) %>% 
    select(Xband, Yband, X, Y) %>% 
    bind_cols(sfdf)
  
  return(list(tibbyt, bandsX, bandsY))
  
}

createPiece <- function(grid, Xpos, Ypos, XCount, YCount, buffer = 300, expand = TRUE) {
  
  # grid = grid pieces
  # Xpos = x coord of grid piece
  # Ypos = above
  # XCount = number of X bands
  # YCount = number of Y bands
  # expand = make it a bit bigger using surrounding pieces, usually used only for the overlay
  

    if(dim(grid[[Xpos]][[Ypos]])[1] == 0) {
      
      return(createPieceNoBuffer(grid, Xpos, Ypos, XCount, YCount, expand, expandcount = 1))
      
    }

  
  # Get every grid piece in a 3x3 grid around the centre piece
  gridpieces <- expand.grid((Xpos-1):(Xpos+1), (Ypos-1):(Ypos+1))
  # Filter out any which are out of bounds / the original piece
  gridpieces <- gridpieces %>% 
    as_tibble() %>%
    rename(X = Var1, Y = Var2) %>% 
    filter(X >= 1 & Y >= 1 & X <= XCount & Y <= YCount) %>% 
    filter(!(X == Xpos & Y == Ypos))
  
  # Get the min / max coords of the centre piece
  bounds <- grid[[Xpos]][[Ypos]] %>% 
    mutate(maxX = max(X),
           minX = min(X),
           maxY = max(Y),
           minY = min(Y)) %>% 
    slice(1) %>% 
    select(maxX, minX, maxY, minY)
  
  # Init a list
  surroundingpieces <- list()
  
  # Iterate over the surrounding pieces and pull out a little bit of them
  # Only do this if you WANT to expand it
  if(expand == TRUE) {
    for(row in 1:dim(gridpieces)[1]) {
      
      X <- gridpieces[row, "X"] %>% 
        pull()
      Y <- gridpieces[row, "Y"] %>% 
        pull()
      
      Xdiff <- X - Xpos
      Ydiff <- Y - Ypos
      
      if(Xdiff == 1) {
        surroundingpieces[[row]]  <- grid[[X]][[Y]] %>% 
          filter((X < bounds$maxX + buffer) & (X > bounds$maxX))
      } else if(Xdiff == -1) {
        surroundingpieces[[row]]  <- grid[[X]][[Y]] %>% 
          filter((X > bounds$minX - buffer) & (X < bounds$minX))
      } else {
        surroundingpieces[[row]] <- grid[[X]][[Y]]
      }
      
      if(Ydiff == 1) {
        surroundingpieces[[row]]  <- surroundingpieces[[row]] %>% 
          filter((Y < bounds$maxY + buffer) & (Y > bounds$maxY))
      } else if(Ydiff == -1) {
        surroundingpieces[[row]]  <- surroundingpieces[[row]] %>% 
          filter((Y > bounds$minY - buffer) & (Y < bounds$minY))
      } else {
        surroundingpieces[[row]] <- surroundingpieces[[row]]
      }
    }
  }
  
  # Put the centre piece in place
  surroundingpieces[["Actual"]] <- grid[[Xpos]][[Ypos]]
  
  # Join those badboys up
  alljoinedup <- rbind_list(surroundingpieces)
  alljoinedup %>% 
    mutate(ActualX = Xpos,
           ActualY = Ypos)
  
}

createPieceNoBuffer <- function(grid, Xpos, Ypos, XCount, YCount, expandcount = 1, expand = TRUE) {
  
  # grid = grid pieces
  # Xpos = x coord of grid piece
  # Ypos = above
  # XCount = number of X bands
  # YCount = number of Y bands
  # expand = make it a bit bigger using surrounding pieces, usually used only for the overlay
  
  
  # Get every grid piece in a 3x3 grid around the centre piece
  gridpieces <- expand.grid((Xpos-expandcount):(Xpos+expandcount), (Ypos-expandcount):(Ypos+expandcount))
  # Filter out any which are out of bounds / the original piece
  gridpieces <- gridpieces %>% 
    as_tibble() %>%
    rename(X = Var1, Y = Var2) %>% 
    filter(X >= 1 & Y >= 1 & X <= XCount & Y <= YCount) %>% 
    filter(!(X == Xpos & Y == Ypos))
  
  
  # Init a list
  surroundingpieces <- list()
  
  # Iterate over the surrounding pieces and pull out a little bit of them
  # Only do this if you WANT to expand it
  if(expand == TRUE) {
    for(row in 1:dim(gridpieces)[1]) {
      
      X <- gridpieces[row, "X"] %>% 
        pull()
      Y <- gridpieces[row, "Y"] %>% 
        pull()
      
      surroundingpieces[[row]] <- grid[[X]][[Y]]
      }
    }
  
  # Put the centre piece in place
  # surroundingpieces[["Actual"]] <- grid[[Xpos]][[Ypos]]
  
  # Join those badboys up
  alljoinedup <- rbind_list(surroundingpieces)
  
  if(dim(alljoinedup)[1] == 0) {
    return(createPieceNoBuffer(grid, Xpos, Ypos, XCount, YCount, expand, expandcount = (expandcount + 1)))
  } else {
  return(alljoinedup %>% 
    mutate(ActualX = Xpos,
           ActualY = Ypos))
  }

  
}

slicegridtoList <- function(expandedgrid, gridlocs, XCount = 10, YCount = 10, buffer = 300, expand = T) {
  
  # This will put separate them out into a list format (X)
  Xlist <- lapply(1:XCount, function(x) expandedgrid %>% 
                             filter(Xband == x))
  
  # Does the same as above, but for each X create 10 Y's in a list format
  XYlist <- list()
  for(i in 1:XCount) {
    XYlist[[i]] <- lapply(1:YCount, function(x) Xlist[[i]] %>% 
                                     filter(Yband == x)) 
  }

  # Only create the new grid with these X values
  Xs <- gridlocs %>% 
    distinct(Xband) %>% 
    pull()
  
  # This is going to expand the grid pieces slightly
  bloatedgrid <- list()
  
  # Init the list for each X
  for(X in Xs) {
    bloatedgrid[[X]] <- list()
  }
  
  # Fill in every grid ref you're supposed to
  for(combo in 1:dim(gridlocs)[1]) {
    X <- gridlocs[combo, "Xband"]
    Y <- gridlocs[combo, "Yband"]
    bloatedgrid[[X]][[Y]] <- createPiece(grid = XYlist,
                                         Xpos = X,
                                         Ypos = Y,
                                         XCount = XCount, YCount = YCount,
                                         buffer = buffer,
                                         expand = expand)
  
  
  }
  
  bloatedgrid
}


# Runme -------------------------------------------------------------------


# extras = list(shapeinfo[["n24"]])


createGrid <- function(mainshapes, ..., XCount = 10, YCount = 10, buffer = 300, mainexpand = T, extrasexpand = T,
                       removeNull = T, report = F) {
  
  # These are any other things that you want to cut based on the mainshapes df
  extras <- list(...)
  
  
  # This first bit is for the MAIN DF
  # This will split the shapes into a Xcount x YCount grid and assign them an index based on
  # geography
  shapesongridlist <- sortShapes(sfdf = mainshapes, XCount = XCount, YCount = YCount)
  shapesongrid <- shapesongridlist[[1]]
  bandsX <- shapesongridlist[[2]]
  bandsY <- shapesongridlist[[3]]
  
  # This is to keep the lists consistent with each other
  # We will only get the grid pieces that contain data in the main shapes
  gridrefswithstuff <- shapesongrid %>% 
    distinct(Xband, Yband) %>% 
    arrange(Xband, Yband)
  
  # This will convert the above expanded DF to a Xcount x YCount list with each element expanded
  # if requested
  
  mainshapes <- slicegridtoList(expandedgrid = shapesongrid,
                                gridlocs = gridrefswithstuff,
                                XCount = XCount, YCount = YCount,
                                buffer = buffer,
                                expand = mainexpand) %>% 
    unlist(recursive = FALSE)
  
  
  
  # This is for extra shapes, it'll put them into the same grid as above but
  # slightly expanded (+/-300m on all edges)
  if (length(extras) > 0) {
    for (listelement in 1:length(extras)) {
      # Maybe check if it's a dataframe here
      extras[[listelement]] <- sortShapes(sfdf = extras[[listelement]],
                                          bandsX = bandsX,
                                          bandsY = bandsY,
                                          XCount = XCount, YCount = YCount,
                                          buffer = buffer)[[1]] %>% 
        slicegridtoList(gridlocs = gridrefswithstuff, 
                        XCount = XCount, YCount = YCount,
                        buffer = buffer,
                        expand = extrasexpand) %>% 
        unlist(recursive = FALSE)
    }
  }
  
  if(removeNull == T) {
    mainshapes <- mainshapes[-which(sapply(mainshapes, is.null))] 
    extras <- lapply(extras, function(x) x[-which(sapply(x, is.null))])
  }
  
  returnme <- list(mainshapes, unlist(extras, recursive = F))
  
  if(report == T) {
    sizereport(listofgrids = returnme)
  }
  
  return(returnme)
  
}

sizereport <- function(listofgrids) {
  
  #SIZE 0(MEMORY) CHECK OF EACH ELEMENT
  for(listgrid in listofgrids) {
    
    information <- lapply(listgrid, function(x) tibble(elements = dim(x)[1], `size (mb)` = (object.size(x) / 1e6))) %>% 
      bind_rows()
  
    print(information %>% 
      arrange(desc(`size (mb)`)))
    print(paste0("Total size: ", object.size(listgrid) / 1e6, " mb"))
    print(summary(information$`size (mb)`))
  }
  
}


