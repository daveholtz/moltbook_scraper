# utils.R - Shared utility functions for Moltbook analysis

library(DBI)
library(RSQLite)
library(tidyverse)

# --- Paths ---
# When running from analysis/R directory
get_output_path <- function(subdir = NULL) {
  base <- "../output"
  if (!is.null(subdir)) file.path(base, subdir) else base
}

# --- Database connection ---
connect_db <- function(db_path = NULL) {
  if (is.null(db_path)) {
    # Use readonly copy if main db is locked
    if (file.exists("/tmp/moltbook_readonly.db")) {
      db_path <- "/tmp/moltbook_readonly.db"
    } else {
      # Try relative path from analysis/R directory
      db_path <- "../../moltbook.db"
    }
  }
  dbConnect(RSQLite::SQLite(), db_path)
}

# --- Gini coefficient ---
gini <- function(x) {
  x <- sort(x[x > 0])
  n <- length(x)
  if (n == 0) return(NA_real_)
  sum((2 * seq_len(n) - n - 1) * x) / (n * sum(x))
}

# --- Text cleaning ---
clean_text <- function(text) {
  text %>%
    str_to_lower() %>%
    str_replace_all("[^a-z0-9\\s]", " ") %>%
    str_squish()
}

# --- Distinct-N metric (lexical diversity) ---
distinct_n <- function(tokens, n = 1) {
  if (n == 1) {
    unique_tokens <- length(unique(tokens))
    total_tokens <- length(tokens)
  } else {
    # Create n-grams
    ngrams <- slider::slide_chr(tokens, ~paste(.x, collapse = " "), .before = 0, .after = n - 1, .complete = TRUE)
    ngrams <- ngrams[!is.na(ngrams)]
    unique_tokens <- length(unique(ngrams))
    total_tokens <- length(ngrams)
  }
  if (total_tokens == 0) return(NA_real_)
  unique_tokens / total_tokens
}

# --- Jaccard similarity ---
jaccard_similarity <- function(set1, set2) {
  intersection <- length(intersect(set1, set2))
  union <- length(union(set1, set2))
  if (union == 0) return(0)
  intersection / union
}

# --- Power law fitting (simple MLE for exponent) ---
estimate_power_law_exponent <- function(x, xmin = 1) {
  x <- x[x >= xmin]
  n <- length(x)
  if (n == 0) return(NA_real_)
  1 + n / sum(log(x / xmin))
}

# --- Theme for publication-ready plots ---
theme_paper <- function(base_size = 14) {
  theme_minimal(base_size = base_size) +
    theme(
      panel.grid.minor = element_blank(),
      panel.grid.major = element_line(color = "gray90"),
      axis.line = element_line(color = "black", linewidth = 0.3),
      axis.ticks = element_line(color = "black", linewidth = 0.3),
      plot.title = element_text(face = "bold", hjust = 0, size = rel(1.2)),
      plot.subtitle = element_text(color = "gray40", size = rel(0.9)),
      axis.title = element_text(size = rel(1.0)),
      axis.text = element_text(size = rel(0.9)),
      strip.text = element_text(size = rel(0.95), face = "bold"),
      legend.position = "bottom",
      legend.text = element_text(size = rel(0.85))
    )
}

# --- Clean labels helper (for axis/legend labels) ---
clean_label <- function(x) {
  x %>%
    str_replace_all("_", " ") %>%
    str_to_title()
}

# --- Save figure helper ---
save_figure <- function(plot, name, width = 8, height = 6, dpi = 300) {
  path <- file.path(get_output_path("figures"), paste0(name, ".pdf"))
  ggsave(path, plot, width = width, height = height, dpi = dpi)
  message("Saved: ", path)

  # Also save PNG for quick viewing
  path_png <- file.path(get_output_path("figures"), paste0(name, ".png"))
  ggsave(path_png, plot, width = width, height = height, dpi = dpi)

  invisible(plot)
}

# --- Save table helper ---
save_table <- function(df, name, format = "markdown") {
  path <- file.path(get_output_path("tables"), paste0(name, ".md"))

  if (format == "markdown") {
    writeLines(knitr::kable(df, format = "markdown"), path)
  } else if (format == "latex") {
    path <- file.path(get_output_path("tables"), paste0(name, ".tex"))
    writeLines(knitr::kable(df, format = "latex", booktabs = TRUE), path)
  }

  message("Saved: ", path)
  invisible(df)
}
