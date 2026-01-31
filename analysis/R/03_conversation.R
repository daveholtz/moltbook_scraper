# 03_conversation.R - Conversation dynamics analysis
#
# Analyzes:
# - Thread depth (reply chain lengths)
# - Branching patterns (replies per comment/post)
# - Thread shapes (deep chains vs wide trees)
# - Response timing patterns

source("utils.R")

library(tidyverse)
library(scales)

# --- Load data ---
message("Loading processed data...")
posts <- readRDS("../data/posts.rds")
comments <- readRDS("../data/comments.rds")

# =============================================================================
# THREAD DEPTH ANALYSIS
# =============================================================================
message("\n=== Thread Depth Analysis ===")

# Build parent-child relationships
# A comment's depth = 1 + depth of its parent
# Root comments (parent_id is NA or equals post_id) have depth 1

compute_thread_depths <- function(comments_df) {
  # Initialize depths
  comments_df <- comments_df %>%
    mutate(depth = NA_integer_)

  # Root comments (no parent or parent is the post itself)
  root_mask <- is.na(comments_df$parent_id)
  comments_df$depth[root_mask] <- 1L

  # Iteratively compute depths for children
  max_iter <- 100  # Safety limit
  for (i in seq_len(max_iter)) {
    # Find comments whose parent has a known depth
    parent_depths <- comments_df %>%
      filter(!is.na(depth)) %>%
      select(id, parent_depth = depth)

    updates <- comments_df %>%
      filter(is.na(depth)) %>%
      inner_join(parent_depths, by = c("parent_id" = "id")) %>%
      mutate(depth = parent_depth + 1L) %>%
      select(id, depth)

    if (nrow(updates) == 0) break

    # Apply updates
    comments_df <- comments_df %>%
      left_join(updates, by = "id", suffix = c("", "_new")) %>%
      mutate(depth = coalesce(depth_new, depth)) %>%
      select(-depth_new)
  }

  comments_df
}

message("Computing comment depths...")
comments <- compute_thread_depths(comments)

# Depth statistics
depth_stats <- comments %>%
  filter(!is.na(depth)) %>%
  summarise(
    mean_depth = mean(depth),
    median_depth = median(depth),
    max_depth = max(depth),
    p90_depth = quantile(depth, 0.9),
    p99_depth = quantile(depth, 0.99)
  )

message(sprintf("Mean thread depth: %.2f", depth_stats$mean_depth))
message(sprintf("Median thread depth: %.0f", depth_stats$median_depth))
message(sprintf("Max thread depth: %d", depth_stats$max_depth))
message(sprintf("90th percentile: %.0f", depth_stats$p90_depth))

# Per-post max depth
post_depths <- comments %>%
  filter(!is.na(depth)) %>%
  group_by(post_id) %>%
  summarise(
    max_depth = max(depth),
    n_comments = n(),
    .groups = "drop"
  )

message(sprintf("\nPosts with comments: %d", nrow(post_depths)))
message(sprintf("Mean max depth per post: %.2f", mean(post_depths$max_depth)))

# Plot: Thread depth distribution
p_depth <- comments %>%
  filter(!is.na(depth)) %>%
  count(depth) %>%
  ggplot(aes(x = depth, y = n)) +
  geom_col(fill = "steelblue", alpha = 0.8) +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Comment Depth Distribution",
    subtitle = sprintf("Mean = %.1f, Max = %d, N = %s comments",
                       depth_stats$mean_depth, depth_stats$max_depth, comma(nrow(comments))),
    x = "Reply depth (1 = direct reply to post)",
    y = "Number of comments"
  ) +
  theme_paper()

save_figure(p_depth, "fig3_thread_depth", width = 8, height = 5)

# Plot: Max depth per post
p_post_depth <- post_depths %>%
  count(max_depth, name = "n_posts") %>%
  ggplot(aes(x = max_depth, y = n_posts)) +
  geom_col(fill = "darkgreen", alpha = 0.8) +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Maximum Thread Depth per Post",
    subtitle = sprintf("N = %s posts with comments", comma(nrow(post_depths))),
    x = "Maximum reply depth",
    y = "Number of posts"
  ) +
  theme_paper()

save_figure(p_post_depth, "fig3b_post_max_depth", width = 8, height = 5)

# =============================================================================
# BRANCHING PATTERNS
# =============================================================================
message("\n=== Branching Pattern Analysis ===")
# How many replies does each comment receive?

reply_counts <- comments %>%
  filter(!is.na(parent_id)) %>%
  count(parent_id, name = "n_replies")

# Merge with comments to get branching factor for each comment
comments_with_replies <- comments %>%
  left_join(reply_counts, by = c("id" = "parent_id")) %>%
  mutate(n_replies = replace_na(n_replies, 0))

branch_stats <- comments_with_replies %>%
  summarise(
    mean_replies = mean(n_replies),
    median_replies = median(n_replies),
    max_replies = max(n_replies),
    pct_with_replies = mean(n_replies > 0) * 100
  )

message(sprintf("Mean replies per comment: %.2f", branch_stats$mean_replies))
message(sprintf("Comments with ≥1 reply: %.1f%%", branch_stats$pct_with_replies))

# Direct replies to posts
direct_replies <- comments %>%
  filter(is.na(parent_id)) %>%
  count(post_id, name = "n_direct")

posts_with_comments <- posts %>%
  left_join(direct_replies, by = c("id" = "post_id")) %>%
  mutate(n_direct = replace_na(n_direct, 0))

message(sprintf("Mean direct replies per post: %.2f", mean(posts_with_comments$n_direct)))

# Plot: Branching factor distribution
p_branch <- comments_with_replies %>%
  filter(n_replies > 0) %>%
  count(n_replies, name = "count") %>%
  ggplot(aes(x = n_replies, y = count)) +
  geom_col(fill = "coral", alpha = 0.8) +
  scale_x_continuous(limits = c(0, 25)) +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Comment Reply Distribution",
    subtitle = sprintf("Among comments with ≥1 reply (%.1f%% of all comments)",
                       branch_stats$pct_with_replies),
    x = "Number of replies received",
    y = "Number of comments"
  ) +
  theme_paper()

save_figure(p_branch, "fig_branching_factor", width = 8, height = 5)

# =============================================================================
# THREAD SHAPE CLASSIFICATION
# =============================================================================
message("\n=== Thread Shape Analysis ===")

# Classify threads by shape:
# - "deep_chain": max_depth > median, width (direct replies) < median
# - "wide_tree": max_depth < median, width > median
# - "balanced": both above or both below median
# - "minimal": few comments

thread_shapes <- post_depths %>%
  left_join(direct_replies, by = c("post_id")) %>%
  mutate(n_direct = replace_na(n_direct, 0)) %>%
  mutate(
    depth_high = max_depth > median(max_depth),
    width_high = n_direct > median(n_direct),
    shape = case_when(
      n_comments < 5 ~ "minimal",
      depth_high & !width_high ~ "deep_chain",
      !depth_high & width_high ~ "wide_tree",
      depth_high & width_high ~ "active_branching",
      TRUE ~ "shallow_sparse"
    )
  )

shape_summary <- thread_shapes %>%
  count(shape) %>%
  mutate(pct = n / sum(n) * 100)

print(shape_summary)

# Clean shape labels for publication
shape_label_map <- c(
  "minimal" = "Minimal",
  "deep_chain" = "Deep Chain",
  "wide_tree" = "Wide Tree",
  "active_branching" = "Active Branching",
  "shallow_sparse" = "Shallow Sparse"
)

p_shapes <- shape_summary %>%
  mutate(shape_clean = shape_label_map[shape]) %>%
  ggplot(aes(x = reorder(shape_clean, -n), y = n, fill = shape_clean)) +
  geom_col(alpha = 0.8) +
  geom_text(aes(label = sprintf("%.1f%%", pct)), vjust = -0.5, size = 4.5) +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Thread Shape Distribution",
    x = "Thread shape",
    y = "Number of posts"
  ) +
  theme_paper() +
  theme(legend.position = "none",
        axis.text.x = element_text(size = 12))

save_figure(p_shapes, "fig_thread_shapes", width = 9, height = 6)

# =============================================================================
# RESPONSE TIMING (if timestamps are reliable)
# =============================================================================
message("\n=== Response Timing Analysis ===")

# Time between post and first comment
first_comments <- comments %>%
  filter(depth == 1) %>%
  group_by(post_id) %>%
  slice_min(created_at, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  select(post_id, first_comment_at = created_at)

posts_timing <- posts %>%
  inner_join(first_comments, by = c("id" = "post_id")) %>%
  mutate(
    response_time_mins = as.numeric(difftime(first_comment_at, created_at, units = "mins"))
  ) %>%
  filter(response_time_mins >= 0, response_time_mins < 60 * 24)  # Filter outliers

message(sprintf("Median time to first comment: %.1f minutes",
                median(posts_timing$response_time_mins, na.rm = TRUE)))
message(sprintf("Mean time to first comment: %.1f minutes",
                mean(posts_timing$response_time_mins, na.rm = TRUE)))

p_timing <- posts_timing %>%
  ggplot(aes(x = response_time_mins)) +
  geom_histogram(bins = 50, fill = "purple", alpha = 0.7) +
  scale_x_continuous(limits = c(0, 120)) +
  labs(
    title = "Time to First Comment",
    subtitle = sprintf("Median = %.1f mins, N = %s posts",
                       median(posts_timing$response_time_mins, na.rm = TRUE),
                       comma(nrow(posts_timing))),
    x = "Minutes after post creation",
    y = "Number of posts"
  ) +
  theme_paper()

save_figure(p_timing, "fig_response_timing", width = 8, height = 5)

# =============================================================================
# SAVE PROCESSED DATA
# =============================================================================
saveRDS(comments, "../data/comments_with_depth.rds")
saveRDS(thread_shapes, "../data/thread_shapes.rds")

# =============================================================================
# SUMMARY TABLE
# =============================================================================
conversation_stats <- tibble(
  Metric = c(
    "Mean comment depth",
    "Median comment depth",
    "Maximum comment depth",
    "90th percentile depth",
    "Mean replies per comment",
    "Comments with ≥1 reply (%)",
    "Mean direct replies per post",
    "Median time to first comment (mins)"
  ),
  Value = c(
    sprintf("%.2f", depth_stats$mean_depth),
    sprintf("%.0f", depth_stats$median_depth),
    sprintf("%d", depth_stats$max_depth),
    sprintf("%.0f", depth_stats$p90_depth),
    sprintf("%.2f", branch_stats$mean_replies),
    sprintf("%.1f%%", branch_stats$pct_with_replies),
    sprintf("%.2f", mean(posts_with_comments$n_direct)),
    sprintf("%.1f", median(posts_timing$response_time_mins, na.rm = TRUE))
  )
)

save_table(conversation_stats, "table2_conversation_stats")
print(conversation_stats)

message("\n=== Conversation dynamics analysis complete ===")
