# 02_structural.R - Structural properties analysis
#
# Analyzes:
# - Agent activity distribution (posts/comments per agent)
# - Submolt size distribution
# - Reply network construction and metrics
# - Community formation patterns

source("utils.R")

library(tidyverse)
library(igraph)
library(scales)

# --- Load data ---
message("Loading processed data...")
data <- readRDS("../data/agents.rds")
agents <- data
posts <- readRDS("../data/posts.rds")
comments <- readRDS("../data/comments.rds")
submolts <- readRDS("../data/submolts.rds")

# =============================================================================
# AGENT ACTIVITY DISTRIBUTION
# =============================================================================
message("\n=== Agent Activity Distribution ===")

# Activity counts (total posts + comments per agent)
activity_dist <- agents %>%
  filter(total_activity > 0) %>%
  arrange(desc(total_activity))

message(sprintf("Active agents: %d", nrow(activity_dist)))
message(sprintf("Gini coefficient: %.3f", gini(activity_dist$total_activity)))
message(sprintf("Top 10%% produce %.1f%% of content",
                100 * sum(head(activity_dist$total_activity, n = ceiling(nrow(activity_dist) * 0.1))) /
                  sum(activity_dist$total_activity)))

# Power law exponent
alpha <- estimate_power_law_exponent(activity_dist$total_activity, xmin = 1)
message(sprintf("Power law exponent (alpha): %.2f", alpha))

# Plot: Activity distribution (log-log)
p_activity <- activity_dist %>%
  count(total_activity, name = "n_agents") %>%
  ggplot(aes(x = total_activity, y = n_agents)) +
  geom_point(alpha = 0.6, size = 2) +
  scale_x_log10(labels = comma) +
  scale_y_log10(labels = comma) +
  labs(
    title = "Agent Activity Distribution",
    subtitle = sprintf("N = %s agents, Gini = %.2f, α ≈ %.2f",
                       comma(nrow(activity_dist)), gini(activity_dist$total_activity), alpha),
    x = "Total posts + comments",
    y = "Number of agents"
  ) +
  theme_paper()

save_figure(p_activity, "fig1_activity_distribution", width = 7, height = 5)

# CCDF (complementary cumulative distribution)
p_activity_ccdf <- activity_dist %>%
  arrange(desc(total_activity)) %>%
  mutate(
    rank = row_number(),
    ccdf = rank / n()
  ) %>%
  ggplot(aes(x = total_activity, y = ccdf)) +
  geom_line(linewidth = 0.8) +
  scale_x_log10(labels = comma) +
  scale_y_log10(labels = percent) +
  labs(
    title = "Agent Activity CCDF",
    subtitle = "Fraction of agents with activity ≥ x",
    x = "Total activity (posts + comments)",
    y = "P(X ≥ x)"
  ) +
  theme_paper()

save_figure(p_activity_ccdf, "fig1b_activity_ccdf", width = 7, height = 5)

# =============================================================================
# SUBMOLT SIZE DISTRIBUTION
# =============================================================================
message("\n=== Submolt Size Distribution ===")

submolt_sizes <- submolts %>%
  filter(post_count > 0) %>%
  arrange(desc(post_count))

message(sprintf("Active submolts (>=1 post): %d", nrow(submolt_sizes)))
message(sprintf("Median posts per submolt: %.0f", median(submolt_sizes$post_count)))
message(sprintf("Top 10 submolts have %.1f%% of posts",
                100 * sum(head(submolt_sizes$post_count, 10)) / sum(submolt_sizes$post_count)))

# Plot: Submolt size distribution
p_submolt <- submolt_sizes %>%
  count(post_count, name = "n_submolts") %>%
  ggplot(aes(x = post_count, y = n_submolts)) +
  geom_point(alpha = 0.6, size = 2) +
  scale_x_log10(labels = comma) +
  scale_y_log10(labels = comma) +
  labs(
    title = "Submolt Size Distribution",
    subtitle = sprintf("N = %s submolts with ≥1 post", comma(nrow(submolt_sizes))),
    x = "Number of posts",
    y = "Number of submolts"
  ) +
  theme_paper()

save_figure(p_submolt, "fig2_submolt_distribution", width = 7, height = 5)

# Plot: Top submolts bar chart
top_n_submolts <- 15
p_top_submolts <- submolt_sizes %>%
  head(top_n_submolts) %>%
  mutate(name = fct_reorder(name, post_count)) %>%
  ggplot(aes(x = post_count, y = name)) +
  geom_col(fill = "steelblue", alpha = 0.8) +
  scale_x_continuous(labels = comma) +
  labs(
    title = sprintf("Top %d Submolts by Post Volume", top_n_submolts),
    subtitle = sprintf("These %d submolts contain %.1f%% of all posts",
                       top_n_submolts,
                       100 * sum(head(submolt_sizes$post_count, top_n_submolts)) / sum(submolt_sizes$post_count)),
    x = "Number of posts",
    y = NULL
  ) +
  theme_paper()

save_figure(p_top_submolts, "fig_top_submolts", width = 8, height = 6)

# Table: Top submolts
table_top_submolts <- submolt_sizes %>%
  head(top_n_submolts) %>%
  select(name, post_count, description) %>%
  mutate(
    pct_posts = sprintf("%.1f%%", 100 * post_count / sum(submolt_sizes$post_count)),
    description = str_trunc(description %||% "", 50)
  ) %>%
  select(Submolt = name, Posts = post_count, `% of Total` = pct_posts)

save_table(table_top_submolts, "table_top_submolts")

# =============================================================================
# REPLY NETWORK
# =============================================================================
message("\n=== Reply Network Analysis ===")

# Build reply network from comments
# Edge: commenter -> post author (or parent comment author)

# First, get post authors
post_authors <- posts %>%
  select(id, author_name) %>%
  rename(post_id = id, post_author = author_name)

# Get parent comment authors
parent_authors <- comments %>%
  select(id, author_name) %>%
  rename(parent_id = id, parent_author = author_name)

# Build edges: commenter replies to someone
reply_edges <- comments %>%
  filter(!is.na(author_name)) %>%
  left_join(post_authors, by = "post_id") %>%
  left_join(parent_authors, by = "parent_id") %>%
  mutate(
    target = coalesce(parent_author, post_author)  # Reply to parent comment or post author
  ) %>%
  filter(!is.na(target), author_name != target) %>%  # Remove self-loops
  select(from = author_name, to = target) %>%
  count(from, to, name = "weight")

message(sprintf("Reply edges: %d", nrow(reply_edges)))
message(sprintf("Unique agents in network: %d",
                length(unique(c(reply_edges$from, reply_edges$to)))))

# Create igraph object
g <- graph_from_data_frame(reply_edges, directed = TRUE)

# Network metrics
message(sprintf("Nodes: %d", vcount(g)))
message(sprintf("Edges: %d", ecount(g)))
message(sprintf("Density: %.6f", edge_density(g)))

# Reciprocity
recip <- reciprocity(g)
message(sprintf("Reciprocity: %.3f", recip))

# Degree distribution
in_degree <- degree(g, mode = "in")
out_degree <- degree(g, mode = "out")

message(sprintf("Mean in-degree: %.2f", mean(in_degree)))
message(sprintf("Mean out-degree: %.2f", mean(out_degree)))

# Clustering coefficient (transitivity)
# Note: For directed graphs, use undirected version
g_undir <- as.undirected(g, mode = "collapse")
clustering <- transitivity(g_undir, type = "global")
message(sprintf("Global clustering coefficient: %.4f", clustering))

# Plot: Degree distribution
degree_df <- tibble(
  degree = c(in_degree, out_degree),
  type = rep(c("In-degree", "Out-degree"), each = length(in_degree))
)

p_degree <- degree_df %>%
  filter(degree > 0) %>%
  ggplot(aes(x = degree, fill = type)) +
  geom_histogram(bins = 50, alpha = 0.7, position = "identity") +
  scale_x_log10() +
  facet_wrap(~type, scales = "free_y") +
  labs(
    title = "Reply Network Degree Distribution",
    subtitle = sprintf("N = %s nodes, reciprocity = %.3f", comma(vcount(g)), recip),
    x = "Degree",
    y = "Count"
  ) +
  theme_paper() +
  theme(legend.position = "none")

save_figure(p_degree, "fig_reply_network_degree", width = 9, height = 4)

# =============================================================================
# GROWTH OVER TIME
# =============================================================================
message("\n=== Growth Over Time ===")

# Cumulative posts and comments over time
posts_over_time <- posts %>%
  filter(!is.na(created_at)) %>%
  arrange(created_at) %>%
  mutate(
    hour = floor_date(created_at, "hour"),
    cumulative_posts = row_number()
  ) %>%
  group_by(hour) %>%
  summarise(
    new_posts = n(),
    cumulative_posts = max(cumulative_posts),
    .groups = "drop"
  )

comments_over_time <- comments %>%
  filter(!is.na(created_at)) %>%
  arrange(created_at) %>%
  mutate(
    hour = floor_date(created_at, "hour"),
    cumulative_comments = row_number()
  ) %>%
  group_by(hour) %>%
  summarise(
    new_comments = n(),
    cumulative_comments = max(cumulative_comments),
    .groups = "drop"
  )

# Combine for cumulative plot
growth_data <- posts_over_time %>%
  full_join(comments_over_time, by = "hour") %>%
  arrange(hour) %>%
  mutate(
    cumulative_posts = zoo::na.locf(cumulative_posts, na.rm = FALSE),
    cumulative_comments = zoo::na.locf(cumulative_comments, na.rm = FALSE)
  ) %>%
  filter(!is.na(hour))

# Fill NAs with 0 for new counts
growth_data <- growth_data %>%
  mutate(
    new_posts = replace_na(new_posts, 0),
    new_comments = replace_na(new_comments, 0),
    cumulative_posts = replace_na(cumulative_posts, 0),
    cumulative_comments = replace_na(cumulative_comments, 0)
  )

# Plot: Cumulative growth
p_growth_cumulative <- growth_data %>%
  select(hour, Posts = cumulative_posts, Comments = cumulative_comments) %>%
  pivot_longer(cols = c(Posts, Comments), names_to = "type", values_to = "count") %>%
  ggplot(aes(x = hour, y = count, color = type)) +
  geom_line(linewidth = 1) +
  scale_y_continuous(labels = comma) +
  scale_color_manual(values = c("Posts" = "steelblue", "Comments" = "coral")) +
  labs(
    title = "Cumulative Growth of Posts and Comments",
    subtitle = sprintf("Total: %s posts, %s comments over %.1f days",
                       comma(nrow(posts)), comma(nrow(comments)),
                       as.numeric(difftime(max(growth_data$hour), min(growth_data$hour), units = "days"))),
    x = "Time",
    y = "Cumulative count",
    color = NULL
  ) +
  theme_paper() +
  theme(legend.position = "bottom")

save_figure(p_growth_cumulative, "fig_growth_cumulative", width = 9, height = 5)

# Plot: Growth rates (hourly)
p_growth_rates <- growth_data %>%
  select(hour, Posts = new_posts, Comments = new_comments) %>%
  pivot_longer(cols = c(Posts, Comments), names_to = "type", values_to = "count") %>%
  ggplot(aes(x = hour, y = count, color = type)) +
  geom_line(alpha = 0.7) +
  geom_smooth(se = FALSE, span = 0.2) +
  scale_y_continuous(labels = comma) +
  scale_color_manual(values = c("Posts" = "steelblue", "Comments" = "coral")) +
  labs(
    title = "Posting and Commenting Rates Over Time",
    subtitle = "Hourly counts with smoothed trend",
    x = "Time",
    y = "New items per hour",
    color = NULL
  ) +
  theme_paper() +
  theme(legend.position = "bottom")

save_figure(p_growth_rates, "fig_growth_rates", width = 9, height = 5)

# =============================================================================
# COMMUNITY FORMATION OVER TIME
# =============================================================================
message("\n=== Community Formation Over Time ===")

# Submolts created per hour
submolt_creation <- submolts %>%
  filter(!is.na(created_at)) %>%
  mutate(hour = floor_date(created_at, "hour")) %>%
  count(hour, name = "new_submolts") %>%
  arrange(hour) %>%
  mutate(cumulative = cumsum(new_submolts))

p_community_growth <- submolt_creation %>%
  ggplot(aes(x = hour, y = cumulative)) +
  geom_line(linewidth = 1.2, color = "steelblue") +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Cumulative Submolt Creation Over Time",
    subtitle = sprintf("%s communities created in %.0f hours",
                       comma(nrow(submolts)),
                       as.numeric(difftime(max(submolt_creation$hour),
                                           min(submolt_creation$hour), units = "hours"))),
    x = "Time (UTC)",
    y = "Cumulative submolts"
  ) +
  theme_paper()

save_figure(p_community_growth, "fig_community_growth", width = 10, height = 6)

# =============================================================================
# SUMMARY TABLE
# =============================================================================
message("\n=== Generating Summary Table ===")

summary_stats <- tibble(
  Metric = c(
    "Total agents",
    "Active agents (≥1 post/comment)",
    "Total posts",
    "Total comments",
    "Total submolts",
    "Date range (days)",
    "Activity Gini coefficient",
    "Activity power law exponent (α)",
    "Reply network nodes",
    "Reply network edges",
    "Reply network reciprocity",
    "Global clustering coefficient"
  ),
  Value = c(
    comma(nrow(agents)),
    comma(nrow(activity_dist)),
    comma(nrow(posts)),
    comma(nrow(comments)),
    comma(nrow(submolts)),
    sprintf("%.1f", as.numeric(difftime(max(posts$created_at, na.rm = TRUE),
                                         min(posts$created_at, na.rm = TRUE), units = "days"))),
    sprintf("%.3f", gini(activity_dist$total_activity)),
    sprintf("%.2f", alpha),
    comma(vcount(g)),
    comma(ecount(g)),
    sprintf("%.3f", recip),
    sprintf("%.4f", clustering)
  )
)

save_table(summary_stats, "table1_summary_stats")
print(summary_stats)

message("\n=== Structural analysis complete ===")
