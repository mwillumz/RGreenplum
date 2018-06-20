#' Greenplum driver
#'
#' This driver never needs to be unloaded and hence `dbUnload()` is a
#' null-op.
#'
#' @export
#' @import methods RPostgres DBI
#' @examples
#' library(RPostgres)
#' RGreenplum::Greenplum()
Greenplum <- function() {
  new("GreenplumDriver")
}


#' GreenplumDriver and methods.
#'
#' @keywords internal
#' @export
setClass("GreenplumDriver", contains = "PqDriver")

#' @rdname GreenplumDriver-class
setMethod("dbUnloadDriver", "GreenplumDriver", function(drv, ...) {
  NULL
})

#' Greenplum dbIsValid method
#' @param dbObj database object
#' @param ... other arguments for method
#' @rdname GreenplumResult-class
setMethod("dbIsValid", "GreenplumDriver", function(dbObj, ...) {
  TRUE
})

setMethod("show", "GreenplumDriver", function(object) {
  cat("<GreenplumDriver>\n")
})
