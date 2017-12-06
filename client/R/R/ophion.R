#' @export
ophion <- function(host) {
  structure(list(), class = "ophion", host = host)
}

#' @export
print.ophion <- function(x) {
  print(paste("host:", attr(x, "host"), sep = " "))
}

#' @export
query <- function(o) {
  class(o) <- c("ophion.query", "ophion")
  attr(o, "query") <- list()
  o
}

#' @export
print.ophion.query <- function(x) {
  print(attr(x, "query"))
}

append.ophion.query <- function(x, values, after = length(x)) {
  q <- attr(x, "query")
  after <- length(q)
  q[[after + 1]] <- values  
  attr(x, "query") <- q
  x
}

#' @export
render <- function(q) {
  jsonlite::toJSON(attr(q, "query"), auto_unbox = T, simplifyVector = F)
}

#' @export
execute <- function(q) {
  body <- render(q)
  print(attributes(q))
  response <- httr::POST(url = paste(attr(q, "host"), "/vertex/query", sep = ""),
                         body = body,
                         httr::content_type_json(),
                         httr::verbose())
  httr::content(response, type="application/json", as="parsed")
}

#' @export
vertex <- function(gid, host) {
  response <- httr::POST(url = paste(host, "/vertex/find/", gid, sep = ""),
                         httr::add_headers("Content-Type"="application/json",
                                           "Accept"="application/json"),
                         httr::verbose())
  httr::content(response)
}

#' @export
incoming <- function(q,  labels=NULL) {
  if (length(labels) == 0) {
    append.ophion.query(q, list("in" = list()))
  } else if (length(args) == 1) {
    append.ophion.query(q, list("in" = list("labels" = list(labels))))
  } else {
    append.ophion.query(q, list("in" = list("labels" = labels)))
  }
}

#' @export
outgoing <- function(q, labels=NULL) {
  if (length(args) == 0) {
    append.ophion.query(q, list("out" = list()))
  } else if (length(args) == 1) {
    append.ophion.query(q, list("out" = list("labels" = list(labels))))
  } else {
    append.ophion.query(q, list("out" = list("labels" = labels)))
  }
}

#' @export
in_edge <- function(q, labels=NULL) {
  if (length(labels) == 0) {
    append.ophion.query(q, list("inEdge" = list()))
  } else if (length(args) == 1) {
    append.ophion.query(q, list("inEdge" = list("labels" = list(labels))))
  } else {
    append.ophion.query(q, list("inEdge" = list("labels" = labels)))
  }
}

#' @export
out_edge <- function(q, labels=NULL) {
  if (length(labels) == 0) {
    append.ophion.query(q, list("outEdge" = list()))
  } else if (length(args) == 1) {
    append.ophion.query(q, list("outEdge" = list("labels" = list(labels))))
  } else {
    append.ophion.query(q, list("outEdge" = list("labels" = labels)))
  }
}

#' @export
in_vertex <- function(q) {
  append.ophion.query(q, list("inVertex" = TRUE))
}

#' @export
out_vertex <- function(q) {
  append.ophion.query(q, list("outVertex" = TRUE))
}

#' @export
mark <- function(q, label) {
  if (is.list(label)) {
    append.ophion.query(q, list("as" = list("labels" = label)))
  } else {
    append.ophion.query(q, list("as" = list("labels" = list(label))))
  }
}

#' @export
select <- function(q, labels) {
  if (length(labels) <= 1) {
    append.ophion.query(q, list("select" = list("labels" = list(labels))))
  } else {
    append.ophion.query(q, list("select" = list("labels" = labels)))
  }
}

#' @export
by <- function(q, label) {
  append.ophion.query(q, list("by" = list("key" = label)))
}

#' @export
label <- function(q) {
  append.ophion.query(q, list("by" = c()))
}

#' @export
values <- function(q, labels) {
  if (length(labels) <= 1) {
    append.ophion.query(q, list("values" = list("labels" = list(labels))))
  } else {
    append.ophion.query(q, list("values" = list("labels" = labels)))
  }
}

#' @export
limit <- function(q, l) {
  append.ophion.query(q, list("limit" = l))
}

#' @export
order <- function(q, key, asc) {
  append.ophion.query(q, list("order" = list("key" = key, "ascending" = asc)))
}

#' @export
range <- function(q, begin, end) {
  append.ophion.query(q, list("range" = list("lower" = begin, "upper" = end)))
}

#' @export
count <- function(q) {
  append.ophion.query(q, list("count" = TRUE))
}

#' @export
dedup <- function(q) {
  append.ophion.query(q, list("dedup" = list()))
}

#' @export
path <- function(q) {
  append.ophion.query(q, list("path": TRUE))
}

#' @export
aggregate <- function(q, label) {
  append.ophion.query(q, list("aggregate" = label))
}

#' @export
group <- function(q, bys) {
  append.ophion.query(q, list("group" = list("bys" = lapply(bys, function(x){ list("key" = x) }))))
}

#' @export
group_count <- function(q, label) {
  if (length(label) == 0) {
    append.ophion.query(q, list("groupCount", c()))
  } else {
    append.ophion.query(q, list("groupCount", list("key" = label)))
  }
}

wrap_value <- function(value) {
  v <- value

  if (length(value) > 1) {
    v <- lapply(value, wrap_value)
  } else if (is.numeric(value)) {
    if (value == round(value)) {
      v <-  list("n" = value)
    } else {
      v <-  list("r" = value)
    }

  } else if (is.character(value)) {
    v = list("s" = value)
  }

  v
}

#' @export
satisfies <- function(q, condition) {
   append.ophion.query(q, list("is" = wrap_value(condition)))
}

#' @export
has <- function(q, key, value= NULL) {
  outer <-  list("key" = key)
  if (!is.null(value)) {
    v <-  wrap_value(value)
    if (is.list(value)) {
      outer <- append(outer, list("condition" = v))
    } else {
      outer <- append(outer, list("value" = v))
    }
  }
  append.ophion.query(q, list("has" = outer))
}

#' @export
has_not <- function(q, key) {
  append.ophion.query(q, list("hasNot" = key))
}

#' @export
match <- function(q, queries) {
  if (length(queries) == 1) {
    queries <- list(queries)
  }
  append.ophion.query(q, list("match", list("queries" = queries)))
}

#' @export
search_vertex <- function(q, search, term=NULL) {
  if (is.null(term)) {
    opts <- list("search" = args[1])
  } else {
    opts <- list("term" = args[1], "search" = args[2])
  }
  append.ophion.query(q, list("searchVertex" = opts))
}

#' @export
search_edge <- function(q,  search, term=NULL) {
  if (is.null(term)) {
    opts <- list("search" = args[1])
  } else {
    opts <- list("term" = args[1], "search" = args[2])
  }
  append.ophion.query(q, list("searchEdge" = opts))
}
