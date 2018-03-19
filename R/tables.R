
#' @export
#' @rdname postgres-tables
setMethod("dbWriteTable", c("GreenplumConnection", "character", "data.frame"),
          function(conn, name, value, ..., row.names = FALSE, overwrite = FALSE, append = FALSE,
                   field.types = NULL, temporary = FALSE, distributed_by = NULL, copy = TRUE) {

            if (is.null(row.names)) row.names <- FALSE
            if ((!is.logical(row.names) && !is.character(row.names)) || length(row.names) != 1L)  {
              stopc("`row.names` must be a logical scalar or a string")
            }
            if (!is.logical(overwrite) || length(overwrite) != 1L || is.na(overwrite))  {
              stopc("`overwrite` must be a logical scalar")
            }
            if (!is.logical(append) || length(append) != 1L || is.na(append))  {
              stopc("`append` must be a logical scalar")
            }
            if (!is.logical(temporary) || length(temporary) != 1L)  {
              stopc("`temporary` must be a logical scalar")
            }
            if (overwrite && append) {
              stopc("overwrite and append cannot both be TRUE")
            }
            if (append && !is.null(field.types)) {
              stopc("Cannot specify field.types with append = TRUE")
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
              dbExecute(conn, sql)
            }

            if (nrow(value) > 0) {
              value <- sqlData(conn, value, row.names = row.names, copy = copy)
              if (!copy) {
                sql <- sqlAppendTable(conn, name, value)
                dbExecute(conn, sql)
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

