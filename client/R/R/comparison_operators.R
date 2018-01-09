#' @export
eq <- function(v) {
  list("eq" = v)
}

#' @export
neq <- function(v) {
  list("neq" = v)
}

#' @export
gt <- function(v) {
  list("gt" = v)
}

#' @export
gte <- function(v) {
  list("gte" = v)
}

#' @export
lt <- function(v) {
  list("lt" = v)
}

#' @export
lte <- function(v) {
  list("lte" = v)
}

#' @export
between <- function(lower, upper) {
  list("between" = list("lower" = lower, "upper" = upper))
}

#' @export
inside <- function(lower, upper) {
  list("inside" = list("lower" = lower, "upper" = upper))
}

#' @export
outside <- function(lower, upper) {
  list("outside" = list("lower" = lower, "upper" = upper))
}

#' @export
within <- function(values) {
  if (length(values) == 0) {
    values <- list()
  }
  if (length(values) == 1) {
    values <- list(values)
  }
  list("within" = values)
}

#' @export
without <- function(values) {
  if (length(values) == 0) {
    values <- list()
  }
  if (length(values) == 1) {
    values <- list(values)
  }
  list("without" = values)
}
