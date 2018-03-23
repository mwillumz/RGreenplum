#' Greenplum sqlCreateTable method
#' @rdname hidden_aliases
#' @inheritParams DBI::sqlCreateTable
#' @param distributed_by Distribution columns for new table. NULL for random distribution.
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
