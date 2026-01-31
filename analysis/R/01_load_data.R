# 01_load_data.R - Load Moltbook data from SQLite into R dataframes
#
# Run this script first to create the data objects used by other scripts.
# Outputs: saves processed dataframes to analysis/data/ for quick reloading.
#
# NOTE: Uses SNAPSHOT data for reproducibility. Data is filtered to the
# snapshot timestamp and vote/karma values come from snapshot tables.

source("utils.R")

library(DBI)
library(RSQLite)
library(tidyverse)
library(lubridate)

# --- Connect to database ---
message("Connecting to database...")
con <- connect_db()

# --- Use fixed snapshot timestamp for reproducibility ---
# Available snapshots: 00:54:50, 01:51:56, 11:30:20, 17:08:17
# Using 11:30:20 which has complete data for posts, comments, and agents
SNAPSHOT_TIME <- "2026-01-31 11:30:20"

agent_snapshot_time <- SNAPSHOT_TIME
post_snapshot_time <- SNAPSHOT_TIME
comment_snapshot_time <- SNAPSHOT_TIME

message(sprintf("Using agent snapshot from: %s", agent_snapshot_time))
message(sprintf("Using post snapshot from: %s", post_snapshot_time))
message(sprintf("Using comment snapshot from: %s", comment_snapshot_time))

# --- Load snapshot tables ---
message("Loading agent snapshots...")
agent_snapshots <- dbGetQuery(con, sprintf("
  SELECT agent_name, karma, follower_count, following_count
  FROM agent_snapshots
  WHERE scraped_at = '%s'
", agent_snapshot_time)) %>% as_tibble()

message("Loading post snapshots...")
post_snapshots <- dbGetQuery(con, sprintf("
  SELECT post_id, upvotes, downvotes, comment_count
  FROM post_snapshots
  WHERE scraped_at = '%s'
", post_snapshot_time)) %>% as_tibble()

message("Loading comment snapshots...")
comment_snapshots <- dbGetQuery(con, sprintf("
  SELECT comment_id, upvotes, downvotes
  FROM comment_snapshots
  WHERE scraped_at = '%s'
", comment_snapshot_time)) %>% as_tibble()

# --- Load base tables and filter to snapshot ---
message("Loading agents...")
agents <- dbReadTable(con, "agents") %>%
  as_tibble() %>%
  mutate(
    created_at = ymd_hms(created_at),
    first_seen_at = ymd_hms(first_seen_at),
    last_updated_at = ymd_hms(last_updated_at)
  ) %>%
  # Only keep agents that have snapshot data (INNER join)
  select(-karma, -follower_count, -following_count) %>%
  inner_join(agent_snapshots, by = c("name" = "agent_name"))

message("Loading posts...")
posts <- dbReadTable(con, "posts") %>%
  as_tibble() %>%
  mutate(
    created_at = ymd_hms(created_at),
    first_seen_at = ymd_hms(first_seen_at),
    last_updated_at = ymd_hms(last_updated_at)
  ) %>%
  # Only keep posts that have snapshot data (INNER join)
  select(-upvotes, -downvotes, -comment_count) %>%
  inner_join(post_snapshots, by = c("id" = "post_id"))

message("Loading comments...")
comments <- dbReadTable(con, "comments") %>%
  as_tibble() %>%
  mutate(
    created_at = ymd_hms(created_at),
    first_seen_at = ymd_hms(first_seen_at),
    last_updated_at = ymd_hms(last_updated_at)
  ) %>%
  # Only keep comments that have snapshot data (INNER join)
  select(-upvotes, -downvotes) %>%
  inner_join(comment_snapshots, by = c("id" = "comment_id"))

message("Loading submolts...")
submolts <- dbReadTable(con, "submolts") %>%
  as_tibble() %>%
  mutate(
    created_at = ymd_hms(created_at),
    first_seen_at = ymd_hms(first_seen_at),
    last_updated_at = ymd_hms(last_updated_at),
    last_activity_at = ymd_hms(last_activity_at)
  ) %>%
  # Filter to submolts that existed at snapshot time
  filter(created_at <= ymd_hms(post_snapshot_time))

# --- Close connection ---
dbDisconnect(con)

# --- Compute derived fields ---
message("Computing derived fields...")

# Agent activity counts
agent_activity <- bind_rows(
  posts %>% count(author_name, name = "post_count"),
  comments %>% count(author_name, name = "comment_count")
) %>%
  group_by(author_name) %>%
  summarise(
    post_count = sum(post_count, na.rm = TRUE),
    comment_count = sum(comment_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    total_activity = post_count + comment_count
  ) %>%
  rename(name = author_name)

agents <- agents %>%
  left_join(agent_activity, by = "name") %>%
  mutate(
    post_count = replace_na(post_count, 0),
    comment_count = replace_na(comment_count, 0),
    total_activity = replace_na(total_activity, 0)
  )

# Submolt activity counts
submolt_activity <- posts %>%
  count(submolt_name, name = "post_count") %>%
  rename(name = submolt_name)

submolts <- submolts %>%
  left_join(submolt_activity, by = "name") %>%
  mutate(post_count = replace_na(post_count, 0))

# --- Summary statistics ---
message("\n=== Dataset Summary ===")
message(sprintf("Agents: %d", nrow(agents)))
message(sprintf("Posts: %d", nrow(posts)))
message(sprintf("Comments: %d", nrow(comments)))
message(sprintf("Submolts: %d", nrow(submolts)))
message(sprintf("Date range: %s to %s",
                min(posts$created_at, na.rm = TRUE),
                max(posts$created_at, na.rm = TRUE)))
message(sprintf("Active agents (>=1 post/comment): %d",
                sum(agents$total_activity > 0)))

# --- Save processed data ---
message("\nSaving processed data...")
dir.create("../data", showWarnings = FALSE)

saveRDS(agents, "../data/agents.rds")
saveRDS(posts, "../data/posts.rds")
saveRDS(comments, "../data/comments.rds")
saveRDS(submolts, "../data/submolts.rds")

message("Done! Data saved to analysis/data/")

# --- Quick reload function for other scripts ---
load_processed_data <- function() {
  list(
    agents = readRDS("../data/agents.rds"),
    posts = readRDS("../data/posts.rds"),
    comments = readRDS("../data/comments.rds"),
    submolts = readRDS("../data/submolts.rds")
  )
}
