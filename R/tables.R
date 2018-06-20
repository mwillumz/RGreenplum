#' Greenplum dbWriteTable method
#' @export
#' @param conn a [GreenplumConnection-class] object
#' @param name a character string specifying a table name. Names will be
#' automatically quoted so you can use any sequence of characters, not just
#' any valid bare table name.
#' @param value A data.frame to write to the database.
#' @param row.names Either TRUE, FALSE, NA or a string
#' @param overwrite a logical specifying whether to overwrite an existing table or not. Its default is FALSE.
#' @param append a logical specifying whether to append to an existing table in the DBMS. Its default is FALSE.
#' @param field.types character vector of named SQL field types where the names are the names of new table's columns.
#' @param temporary If TRUE, will generate a temporary table statement.
#' @param distributed_by Distribution columns for new table. NULL for random distribution.
#' @param copy If TRUE, data will be copied to remote database
#' @param ... Other arguments used by individual methods.
#' @rdname greenplum-tables
setMethod("dbWriteTable", c("GreenplumConnection", "character", "data.frame"),
          function(conn, name, value, ..., row.names = FALSE, overwrite = FALSE, append = FALSE,
                   field.types = NULL, temporary = FALSE, distributed_by = NULL, copy = TRUE) {

            if (is.null(row.names)) row.names <- FALSE
            if ((!is.logical(row.names) && !is.character(row.names)) || length(row.names) != 1L)  {
              stop("`row.names` must be a logical scalar or a string")
            }
            if (!is.logical(overwrite) || length(overwrite) != 1L || is.na(overwrite))  {
              stop("`overwrite` must be a logical scalar")
            }
            if (!is.logical(append) || length(append) != 1L || is.na(append))  {
              stop("`append` must be a logical scalar")
            }
            if (!is.logical(temporary) || length(temporary) != 1L)  {
              stop("`temporary` must be a logical scalar")
            }
            if (overwrite && append) {
              stop("overwrite and append cannot both be TRUE")
            }
            if (append && !is.null(field.types)) {
              stop("Cannot specify field.types with append = TRUE")
            }

            found <- dbExistsTable(conn, name)
            if (found && !overwrite && !append) {
              stop("Table ", name, " exists in database, and both overwrite and",
                   " append are FALSE", call. = FALSE)
            }
            if (found && overwrite) {
              dbRemoveTable(conn, name)
            }

            if (!found || overwrite) {
              if (!is.null(field.types)) {
                if (is.null(names(field.types)))
                  types <- structure(field.types, .Names = colnames(value))
                else
                  types <- field.types
              } else {
                types <- value
              }
              sql <- sqlCreateTable(conn, name, if (is.null(field.types)) value else types,
                                    row.names = row.names, temporary = temporary,
                                    distributed_by = distributed_by)
              DBI::dbExecute(conn, sql)
            }

            if (nrow(value) > 0) {
              value <- sqlData(conn, value, row.names = row.names, copy = copy)
              if (!copy) {
                sql <- DBI::sqlAppendTable(conn, name, value)
                DBI::dbExecute(conn, sql)
              } else {
                fields <- dbQuoteIdentifier(conn, names(value))
                sql <- paste0(
                  "COPY ", dbQuoteIdentifier(conn, name),
                  " (", paste(fields, collapse = ", "), ")",
                  " FROM STDIN"
                )
                RPostgres:::connection_copy_data(conn@ptr, sql, value)
              }
            }

            invisible(TRUE)
          }
)

#' Greenplum dbExistsTable method
#' @export
#' #' @param conn a [GreenplumConnection-class] object
#' @param name a character string specifying a table name. Names will be
#' automatically quoted so you can use any sequence of characters, not just
#' any valid bare table name.
#' @param ... Other arguments used by individual methods.
#' @rdname greenplum-tables
setMethod("dbExistsTable", c("GreenplumConnection", "character"), function(conn, name, ...) {
  stopifnot(length(name) == 1L)
  name <- dbQuoteIdentifier(conn, name)

  # Convert to identifier
  id <- dbUnquoteIdentifier(conn, name)[[1]]@name
  exists_table(conn, id)
})

exists_table <- function(conn, id) {
  table <- dbQuoteString(conn, id[["table"]])

  query <- paste0(
    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.tables WHERE table_name = ",
    table
  )

  if ("schema" %in% names(id)) {
    query <- paste0(
      query,
      "AND ",
      "table_schema = ",
      dbQuoteString(conn, id[["schema"]])
    )
  } else {
    query <- paste0(
      query,
      "AND ",
      "(table_schema = ANY(current_schemas(false)) OR table_type = 'LOCAL TEMPORARY')"
    )
  }

  dbGetQuery(conn, query)[[1]] >= 1
}

