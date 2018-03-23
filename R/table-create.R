#' Greenplum sqlCreateTable method
#' @rdname hidden_aliases
#' @inheritParams DBI::sqlCreateTable
#' @param con A database connection.
#' @param table Name of the table. Escaped with
#'   [dbQuoteIdentifier()].
#' @param fields Either a character vector or a data frame.
#'
#'   A named character vector: Names are column names, values are types.
#'   Names are escaped with [dbQuoteIdentifier()].
#'   Field types are unescaped.
#'
#'   A data frame: field types are generated using
#'   [dbDataType()].
#' @param temporary If `TRUE`, will generate a temporary table statement.
#' @param distributed_by Distribution columns for new table. NULL for random distribution.
#' @param ... Other arguments used by individual methods.
#' @export
setMethod("sqlCreateTable", signature("GreenplumConnection"),
          function(con, table, fields, row.names = NA, temporary = FALSE,
                   distributed_by = NULL, ...) {
            table <- dbQuoteIdentifier(con, table)

            if (is.data.frame(fields)) {
              fields <- sqlRownamesToColumn(fields, row.names)
              fields <- vapply(fields, function(x) DBI::dbDataType(con, x), character(1))
            }

            field_names <- dbQuoteIdentifier(con, names(fields))
            field_types <- unname(fields)
            fields <- paste0(field_names, " ", field_types)

            distribution <- if(is.null(distributed_by)){
              "DISTRIBUTED RANDOMLY"
            } else{
              paste0("(",
                     paste(distributed_by, collapse = ", "),
                     ")")
            }

            SQL(paste0(
              "CREATE ", if (temporary) "TEMPORARY ", "TABLE ", table, " (\n",
              "  ", paste(fields, collapse = ",\n  "), "\n)\n ",
              distribution
            ))
          }
)
