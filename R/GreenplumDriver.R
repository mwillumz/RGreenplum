#' Greenplum driver
#'
#' This driver never needs to be unloaded and hence `dbUnload()` is a
#' null-op.
#'
#' @export
#' @import methods DBI RPostgres
#' @examples
#' library(DBI)
#' RGreenplum::Greenplum()
Greenplum <- function() {
  new("GreenplumDriver")
}


#' GreenplumDriver and methods.
#'
#' @keywords internal
#' @export
setClass("GreenplumDriver", contains = "PqDriver")

#' @export
#' @rdname GreenplumDriver-class
setMethod("dbUnloadDriver", "GreenplumDriver", function(drv, ...) {
  NULL
})

#' @export
#' @rdname GreenplumResult-class
setMethod("dbIsValid", "GreenplumDriver", function(dbObj, ...) {
  TRUE
})

setMethod("show", "GreenplumDriver", function(object) {
  cat("<GreenplumDriver>\n")
})
