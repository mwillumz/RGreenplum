#' @include GreenplumDriver.R
NULL

#' GreenplumConnection and methods.
#'
#' @export
#' @keywords internal
setClass("GreenplumConnection",
         contains = "PqConnection"
)

#' Connect to a Greenplum database.
#'
#' Manually disconnecting a connection is not necessary with RGreenplum, but
#' still recommended;
#' if you delete the object containing the connection, it will be automatically
#' disconnected during the next GC with a warning.
#'
#' @param drv `RGreenplum::Greenplum()`
#' @param dbname Database name. If `NULL`, defaults to the user name.
#'   Note that this argument can only contain the database name, it will not
#'   be parsed as a connection string (internally, `expand_dbname` is set to
#'   `false` in the call to
#'   [`PQconnectdbParams()`](https://www.postgresql.org/docs/9.6/static/libpq-connect.html)).
#' @param user,password User name and password. If `NULL`, will be
#'   retrieved from `PGUSER` and `PGPASSWORD` envvars, or from the
#'   appropriate line in `~/.pgpass`. See
#'   <http://www.postgresql.org/docs/9.6/static/libpq-pgpass.html> for
#'   more details.
#' @param host,port Host and port. If `NULL`, will be retrieved from
#'   `PGHOST` and `PGPORT` env vars.
#' @param service Name of service to connect as.  If `NULL`, will be
#'   ignored.  Otherwise, connection parameters will be loaded from the pg_service.conf
#'   file and used.  See <http://www.postgresql.org/docs/9.6/static/libpq-pgservice.html>
#'   for details on this file and syntax.
#' @param ... Other name-value pairs that describe additional connection
#'   options as described at
#'   <http://www.postgresql.org/docs/9.6/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS>
#' @param bigint The R type that 64-bit integer types should be mapped to,
#'   default is [bit64::integer64], which allows the full range of 64 bit
#'   integers.
#' @inheritParams RPostgres::dbConnect
#' @export
setMethod("dbConnect", "GreenplumDriver",
          function(drv, dbname = NULL,
                   host = NULL, port = NULL, password = NULL, user = NULL, service = NULL, ...,
                   bigint = c("integer64", "integer", "numeric", "character")) {

            opts <- unlist(list(dbname = dbname, user = user, password = password,
                                host = host, port = as.character(port), service = service, client_encoding = "utf8", ...))
            if (!is.character(opts)) {
              stop("All options should be strings", call. = FALSE)
            }
            bigint <- match.arg(bigint)

            if (length(opts) == 0) {
              ptr <- RPostgers:::connection_create(character(), character())
            } else {
              ptr <- RPostgres:::connection_create(names(opts), as.vector(opts))
            }

            con <- new("GreenplumConnection", ptr = ptr, bigint = bigint)
            dbExecute(con, "SET TIMEZONE='UTC'")
            con
          })
