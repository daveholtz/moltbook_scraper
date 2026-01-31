# 07_owner_analysis.R - Human owner analysis
#
# Analyzes:
# - Owner demographics (X/Twitter followers, verified status)
# - Correlation between owner attributes and agent behavior
# - Multi-agent owners

source("utils.R")

library(tidyverse)
library(jsonlite)
library(scales)

# --- Load data ---
message("Loading data...")

# Use saved snapshot data for reproducibility
# Note: Owner data may have been enriched after snapshot, so we
# read fresh from DB but filter to agents that exist in snapshot
agents_snapshot <- readRDS("../data/agents.rds")
posts <- readRDS("../data/posts.rds")
comments <- readRDS("../data/comments.rds")

# Get owner data from current DB (owner JSON not in snapshots)
con <- connect_db()
agents_raw <- dbReadTable(con, "agents") %>%
  as_tibble() %>%
  # Filter to only agents that exist in snapshot
  filter(name %in% agents_snapshot$name)
dbDisconnect(con)

# =============================================================================
# PARSE OWNER DATA
# =============================================================================
message("\n=== Parsing Owner Data ===")

# Parse JSON owner data
parse_owner <- function(json_str) {
  if (is.na(json_str) || json_str == "" || json_str == "null") {
    return(tibble(
      x_handle = NA_character_,
      x_name = NA_character_,
      x_bio = NA_character_,
      x_follower_count = NA_integer_,
      x_following_count = NA_integer_,
      x_verified = NA
    ))
  }
  tryCatch({
    parsed <- fromJSON(json_str)
    tibble(
      x_handle = parsed$x_handle %||% NA_character_,
      x_name = parsed$x_name %||% NA_character_,
      x_bio = parsed$x_bio %||% NA_character_,
      x_follower_count = as.integer(parsed$x_follower_count %||% NA),
      x_following_count = as.integer(parsed$x_following_count %||% NA),
      x_verified = parsed$x_verified %||% NA
    )
  }, error = function(e) {
    tibble(
      x_handle = NA_character_,
      x_name = NA_character_,
      x_bio = NA_character_,
      x_follower_count = NA_integer_,
      x_following_count = NA_integer_,
      x_verified = NA
    )
  })
}

# Parse all owners
owners <- agents_raw %>%
  rowwise() %>%
  mutate(owner_parsed = list(parse_owner(owner_json))) %>%
  unnest(owner_parsed) %>%
  ungroup()

# Summary stats
n_with_owner <- sum(!is.na(owners$x_handle))
n_claimed <- sum(owners$is_claimed == 1, na.rm = TRUE)

message(sprintf("Total agents: %d", nrow(owners)))
message(sprintf("Agents with owner data: %d (%.1f%%)", n_with_owner, 100 * n_with_owner / nrow(owners)))
message(sprintf("Claimed agents: %d", n_claimed))

# =============================================================================
# OWNER DEMOGRAPHICS
# =============================================================================
message("\n=== Owner Demographics ===")

owners_with_data <- owners %>%
  filter(!is.na(x_handle))

# Unique owners
unique_owners <- owners_with_data %>%
  distinct(x_handle, .keep_all = TRUE)

message(sprintf("Unique human owners: %d", nrow(unique_owners)))

# Follower distribution
follower_stats <- unique_owners %>%
  filter(!is.na(x_follower_count)) %>%
  summarise(
    mean = mean(x_follower_count),
    median = median(x_follower_count),
    max = max(x_follower_count),
    p25 = quantile(x_follower_count, 0.25),
    p75 = quantile(x_follower_count, 0.75),
    p90 = quantile(x_follower_count, 0.90),
    p99 = quantile(x_follower_count, 0.99)
  )

message("\nOwner X follower distribution:")
print(follower_stats)

# Verified owners
n_verified <- sum(unique_owners$x_verified == TRUE, na.rm = TRUE)
message(sprintf("\nVerified owners: %d (%.1f%%)", n_verified, 100 * n_verified / nrow(unique_owners)))

# Plot: Owner follower distribution
p_owner_followers <- unique_owners %>%
  filter(!is.na(x_follower_count), x_follower_count > 0) %>%
  ggplot(aes(x = x_follower_count)) +
  geom_histogram(bins = 50, fill = "steelblue", alpha = 0.8) +
  scale_x_log10(labels = comma) +
  labs(
    title = "Human Owner X Follower Distribution",
    subtitle = sprintf("N = %s unique owners, median = %s followers",
                       comma(nrow(unique_owners)),
                       comma(follower_stats$median)),
    x = "X Followers (log scale)",
    y = "Count"
  ) +
  theme_paper()

save_figure(p_owner_followers, "fig_owner_follower_dist", width = 8, height = 5)

# =============================================================================
# MULTI-AGENT OWNERS
# =============================================================================
message("\n=== Multi-Agent Owners ===")

agents_per_owner <- owners_with_data %>%
  count(x_handle, name = "n_agents") %>%
  arrange(desc(n_agents))

message(sprintf("Owners with 1 agent: %d", sum(agents_per_owner$n_agents == 1)))
message(sprintf("Owners with 2+ agents: %d", sum(agents_per_owner$n_agents >= 2)))
message(sprintf("Owners with 5+ agents: %d", sum(agents_per_owner$n_agents >= 5)))
message(sprintf("Max agents per owner: %d", max(agents_per_owner$n_agents)))

# Top multi-agent owners
message("\nTop 10 owners by number of agents:")
print(agents_per_owner %>% head(10))

# Plot: Agents per owner distribution
p_agents_per_owner <- agents_per_owner %>%
  count(n_agents, name = "n_owners") %>%
  ggplot(aes(x = n_agents, y = n_owners)) +
  geom_col(fill = "coral", alpha = 0.8) +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Agents per Human Owner",
    subtitle = sprintf("%d owners with 2+ agents, max = %d",
                       sum(agents_per_owner$n_agents >= 2),
                       max(agents_per_owner$n_agents)),
    x = "Number of agents",
    y = "Number of owners"
  ) +
  theme_paper()

save_figure(p_agents_per_owner, "fig_agents_per_owner", width = 8, height = 5)

# =============================================================================
# CORRELATE OWNER ATTRIBUTES WITH AGENT BEHAVIOR
# =============================================================================
message("\n=== Correlating Owner Attributes with Agent Behavior ===")

# Compute agent activity
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
  mutate(total_activity = post_count + comment_count)

# Join with owner data
agents_analysis <- owners %>%
  left_join(agent_activity, by = c("name" = "author_name")) %>%
  mutate(
    post_count = replace_na(post_count, 0),
    comment_count = replace_na(comment_count, 0),
    total_activity = replace_na(total_activity, 0)
  )

# Correlation: Owner followers vs agent activity
cor_data <- agents_analysis %>%
  filter(!is.na(x_follower_count), x_follower_count > 0, total_activity > 0)

if (nrow(cor_data) > 30) {
  cor_followers_activity <- cor(log10(cor_data$x_follower_count),
                                 log10(cor_data$total_activity),
                                 method = "spearman")
  message(sprintf("Correlation (owner followers vs agent activity, Spearman): %.3f", cor_followers_activity))

  # Plot
  p_cor_followers <- cor_data %>%
    ggplot(aes(x = x_follower_count, y = total_activity)) +
    geom_point(alpha = 0.3, size = 1) +
    geom_smooth(method = "lm", color = "red", se = TRUE) +
    scale_x_log10(labels = comma) +
    scale_y_log10(labels = comma) +
    labs(
      title = "Owner X Followers vs Agent Activity",
      subtitle = sprintf("Spearman r = %.3f, N = %s", cor_followers_activity, comma(nrow(cor_data))),
      x = "Owner X Followers",
      y = "Agent Total Activity (posts + comments)"
    ) +
    theme_paper()

  save_figure(p_cor_followers, "fig_owner_followers_vs_activity", width = 8, height = 6)
}

# Correlation: Owner followers vs agent karma
cor_data_karma <- agents_analysis %>%
  filter(!is.na(x_follower_count), x_follower_count > 0, !is.na(karma), karma > 0)

if (nrow(cor_data_karma) > 30) {
  cor_followers_karma <- cor(log10(cor_data_karma$x_follower_count),
                              log10(cor_data_karma$karma),
                              method = "spearman")
  message(sprintf("Correlation (owner followers vs agent karma, Spearman): %.3f", cor_followers_karma))

  p_cor_karma <- cor_data_karma %>%
    ggplot(aes(x = x_follower_count, y = karma)) +
    geom_point(alpha = 0.3, size = 1) +
    geom_smooth(method = "lm", color = "red", se = TRUE) +
    scale_x_log10(labels = comma) +
    scale_y_log10(labels = comma) +
    labs(
      title = "Owner X Followers vs Agent Karma",
      subtitle = sprintf("Spearman r = %.3f, N = %s", cor_followers_karma, comma(nrow(cor_data_karma))),
      x = "Owner X Followers",
      y = "Agent Karma"
    ) +
    theme_paper()

  save_figure(p_cor_karma, "fig_owner_followers_vs_karma", width = 8, height = 6)
}

# =============================================================================
# VERIFIED OWNERS VS NON-VERIFIED
# =============================================================================
message("\n=== Verified vs Non-Verified Owners ===")

verified_comparison <- agents_analysis %>%
  filter(!is.na(x_verified)) %>%
  group_by(x_verified) %>%
  summarise(
    n_agents = n(),
    mean_activity = mean(total_activity, na.rm = TRUE),
    median_activity = median(total_activity, na.rm = TRUE),
    mean_karma = mean(karma, na.rm = TRUE),
    median_karma = median(karma, na.rm = TRUE),
    mean_followers = mean(follower_count, na.rm = TRUE),
    .groups = "drop"
  )

message("\nVerified vs Non-Verified owner comparison:")
print(verified_comparison)

# Statistical test
verified_activity <- agents_analysis %>% filter(x_verified == TRUE) %>% pull(total_activity)
nonverified_activity <- agents_analysis %>% filter(x_verified == FALSE) %>% pull(total_activity)

if (length(verified_activity) > 5 && length(nonverified_activity) > 5) {
  wilcox_test <- wilcox.test(verified_activity, nonverified_activity)
  message(sprintf("Wilcoxon test (verified vs non-verified activity): p = %.4f", wilcox_test$p.value))
}

# =============================================================================
# OWNER INFLUENCE TIERS
# =============================================================================
message("\n=== Owner Influence Tiers ===")

# Categorize owners by X follower count
agents_analysis <- agents_analysis %>%
  mutate(
    owner_tier = case_when(
      is.na(x_follower_count) ~ "Unknown",
      x_follower_count >= 10000 ~ "Large (10k+)",
      x_follower_count >= 1000 ~ "Medium (1k-10k)",
      x_follower_count >= 100 ~ "Small (100-1k)",
      TRUE ~ "Micro (<100)"
    ),
    owner_tier = factor(owner_tier, levels = c("Unknown", "Micro (<100)", "Small (100-1k)",
                                                "Medium (1k-10k)", "Large (10k+)"))
  )

tier_summary <- agents_analysis %>%
  group_by(owner_tier) %>%
  summarise(
    n_agents = n(),
    mean_activity = mean(total_activity, na.rm = TRUE),
    median_activity = median(total_activity, na.rm = TRUE),
    mean_karma = mean(karma, na.rm = TRUE),
    total_posts = sum(post_count, na.rm = TRUE),
    total_comments = sum(comment_count, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(pct_agents = n_agents / sum(n_agents) * 100)

message("\nActivity by owner influence tier:")
print(tier_summary)

# Plot: Activity by owner tier
p_tier_activity <- agents_analysis %>%
  filter(owner_tier != "Unknown", total_activity > 0) %>%
  ggplot(aes(x = owner_tier, y = total_activity, fill = owner_tier)) +
  geom_boxplot(alpha = 0.8, outlier.alpha = 0.3) +
  scale_y_log10(labels = comma) +
  labs(
    title = "Agent Activity by Owner Influence Tier",
    subtitle = "Based on owner's X follower count",
    x = "Owner Tier",
    y = "Agent Activity (log scale)"
  ) +
  theme_paper() +
  theme(legend.position = "none")

save_figure(p_tier_activity, "fig_activity_by_owner_tier", width = 9, height = 6)

# =============================================================================
# TOP OWNERS ANALYSIS
# =============================================================================
message("\n=== Top Owners by Agent Activity ===")

# Aggregate activity by owner
owner_activity <- agents_analysis %>%
  filter(!is.na(x_handle)) %>%
  group_by(x_handle, x_name, x_follower_count, x_verified) %>%
  summarise(
    n_agents = n(),
    total_posts = sum(post_count, na.rm = TRUE),
    total_comments = sum(comment_count, na.rm = TRUE),
    total_activity = sum(total_activity, na.rm = TRUE),
    total_karma = sum(karma, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(total_activity))

message("\nTop 15 owners by total agent activity:")
print(owner_activity %>% head(15) %>%
        select(x_handle, x_follower_count, n_agents, total_posts, total_comments, total_karma))

# Top owners by karma
message("\nTop 15 owners by total agent karma:")
print(owner_activity %>%
        arrange(desc(total_karma)) %>%
        head(15) %>%
        select(x_handle, x_follower_count, n_agents, total_activity, total_karma))

# =============================================================================
# OWNER BIO ANALYSIS
# =============================================================================
message("\n=== Owner Bio Analysis ===")

# Common words/phrases in owner bios
owner_bios <- unique_owners %>%
  filter(!is.na(x_bio), nchar(x_bio) > 0) %>%
  pull(x_bio)

message(sprintf("Owners with bios: %d", length(owner_bios)))

# Quick keyword check
keywords <- c("ai", "developer", "engineer", "founder", "ceo", "builder",
              "crypto", "web3", "startup", "tech", "software")

keyword_counts <- sapply(keywords, function(kw) {
  sum(str_detect(str_to_lower(owner_bios), paste0("\\b", kw, "\\b")))
})

bio_keywords <- tibble(
  keyword = keywords,
  count = keyword_counts,
  pct = keyword_counts / length(owner_bios) * 100
) %>%
  arrange(desc(count))

message("\nKeywords in owner bios:")
print(bio_keywords)

# =============================================================================
# SUMMARY TABLE
# =============================================================================
message("\n=== Owner Analysis Summary ===")

# Handle case where correlations weren't computed
cor_followers_activity_str <- if (exists("cor_followers_activity")) sprintf("%.3f", cor_followers_activity) else "N/A (insufficient data)"
cor_followers_karma_str <- if (exists("cor_followers_karma")) sprintf("%.3f", cor_followers_karma) else "N/A (insufficient data)"

owner_summary <- tibble(
  Metric = c(
    "Total agents",
    "Agents with owner data",
    "Unique human owners",
    "Owners with 2+ agents",
    "Verified owners",
    "Median owner X followers",
    "Mean owner X followers",
    "Correlation (owner followers vs agent activity)",
    "Correlation (owner followers vs agent karma)"
  ),
  Value = c(
    comma(nrow(agents_analysis)),
    sprintf("%s (%.1f%%)", comma(n_with_owner), 100 * n_with_owner / nrow(agents_analysis)),
    comma(nrow(unique_owners)),
    comma(sum(agents_per_owner$n_agents >= 2)),
    sprintf("%d (%.1f%%)", n_verified, 100 * n_verified / nrow(unique_owners)),
    comma(follower_stats$median),
    comma(round(follower_stats$mean)),
    cor_followers_activity_str,
    cor_followers_karma_str
  )
)

save_table(owner_summary, "table9_owner_summary")
print(owner_summary)

# Save tier summary
save_table(tier_summary, "table10_owner_tier_summary")

message("\n=== Owner analysis complete ===")
