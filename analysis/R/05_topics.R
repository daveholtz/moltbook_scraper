# 05_topics.R - Topic modeling and theme analysis
#
# Analyzes:
# - LDA topic model (15-20 topics)
# - Topic prevalence and interpretation
# - Emergent themes identification
# - Topic evolution over time

source("utils.R")

library(tidyverse)
library(tidytext)
library(topicmodels)
library(scales)
library(lubridate)

# --- Load data ---
message("Loading processed data...")
posts <- readRDS("../data/posts.rds")
comments <- readRDS("../data/comments.rds")

# Combine all content
all_content <- bind_rows(
  posts %>%
    select(id, text = content, author_name, created_at) %>%
    mutate(type = "post"),
  comments %>%
    select(id, text = content, author_name, created_at) %>%
    mutate(type = "comment")
) %>%
  filter(!is.na(text), nchar(text) >= 50) %>%
  # Deduplicate for cleaner topic modeling

  mutate(text_clean = str_squish(str_to_lower(text))) %>%
  distinct(text_clean, .keep_all = TRUE) %>%
  select(-text_clean)

message(sprintf("Documents for topic modeling (deduplicated): %s", comma(nrow(all_content))))

# =============================================================================
# PREPARE DOCUMENT-TERM MATRIX
# =============================================================================
message("\n=== Building Document-Term Matrix ===")

# Load stopwords
data("stop_words")

# Additional domain-specific stopwords
custom_stops <- tibble(word = c(
  "http", "https", "www", "com", "org", "io",  # URLs
  "just", "like", "really", "actually", "probably",  # Filler words
  "im", "ive", "dont", "doesnt", "didnt", "cant", "wont",  # Contractions
  "thats", "youre", "theyre", "weve", "youve"
))

# Tokenize and create DTM (with stemming)
library(SnowballC)

word_counts <- all_content %>%
  select(id, text) %>%
  unnest_tokens(word, text) %>%
  filter(str_detect(word, "^[a-z]+$")) %>%  # Only alphabetic
  filter(nchar(word) >= 3) %>%  # Minimum length
  anti_join(stop_words, by = "word") %>%
  anti_join(custom_stops, by = "word") %>%
  mutate(word = wordStem(word, language = "en")) %>%  # Stem words
  filter(nchar(word) >= 3) %>%  # Re-filter after stemming
  count(id, word)

# Filter rare and very common words
word_doc_freq <- word_counts %>%
  group_by(word) %>%
  summarise(doc_count = n(), .groups = "drop")

# Keep words appearing in at least 10 docs but no more than 50% of docs
total_docs <- n_distinct(word_counts$id)
vocab <- word_doc_freq %>%
  filter(doc_count >= 10, doc_count <= total_docs * 0.5) %>%
  pull(word)

message(sprintf("Vocabulary size after filtering: %s", comma(length(vocab))))

word_counts_filtered <- word_counts %>%
  filter(word %in% vocab)

# Create DTM
dtm <- word_counts_filtered %>%
  cast_dtm(id, word, n)

message(sprintf("DTM: %d documents x %d terms", nrow(dtm), ncol(dtm)))

# =============================================================================
# FIT LDA MODEL
# =============================================================================
message("\n=== Fitting LDA Topic Model ===")

# Number of topics (K=8 selected via UMass coherence optimization)
K <- 8
message(sprintf("Fitting LDA with K=%d topics...", K))

set.seed(42)
lda_model <- LDA(dtm, k = K, method = "Gibbs",
                 control = list(seed = 42, burnin = 1000, iter = 2000, thin = 100))

message("LDA fitting complete.")

# =============================================================================
# EXTRACT AND VISUALIZE TOPICS
# =============================================================================
message("\n=== Topic Analysis ===")

# Top words per topic
topic_terms <- tidy(lda_model, matrix = "beta") %>%
  group_by(topic) %>%
  slice_max(beta, n = 15) %>%
  ungroup() %>%
  arrange(topic, desc(beta))

# Print top words for each topic
for (k in 1:K) {
  words <- topic_terms %>%
    filter(topic == k) %>%
    pull(term) %>%
    head(10) %>%
    paste(collapse = ", ")
  message(sprintf("Topic %2d: %s", k, words))
}

# Document-topic distributions
doc_topics <- tidy(lda_model, matrix = "gamma") %>%
  rename(id = document)

# Most prevalent topics
topic_prevalence <- doc_topics %>%
  group_by(topic) %>%
  summarise(mean_gamma = mean(gamma), .groups = "drop") %>%
  arrange(desc(mean_gamma))

message("\nTopic prevalence (mean gamma):")
print(topic_prevalence)

# =============================================================================
# TOPIC LABELING (MANUAL INTERPRETATION GUIDE)
# =============================================================================
message("\n=== Generating Topic Labels ===")

# Create summary for manual labeling
topic_summary <- topic_terms %>%
  group_by(topic) %>%
  summarise(
    top_words = paste(head(term, 10), collapse = ", "),
    .groups = "drop"
  ) %>%
  left_join(topic_prevalence, by = "topic") %>%
  arrange(desc(mean_gamma))

# Placeholder labels (to be refined manually)
topic_labels <- tibble(
  topic = 1:K,
  label = paste0("Topic_", 1:K)  # Placeholder - refine based on top words
)

# =============================================================================
# VISUALIZATIONS
# =============================================================================

# Plot: Top words per topic (faceted)
p_topic_words <- topic_terms %>%
  group_by(topic) %>%
  slice_max(beta, n = 10) %>%
  ungroup() %>%
  mutate(
    topic_label = paste0("Topic ", topic),
    term = reorder_within(term, beta, topic)
  ) %>%
  ggplot(aes(x = beta, y = term, fill = factor(topic))) +
  geom_col(show.legend = FALSE, alpha = 0.8) +
  facet_wrap(~topic_label, scales = "free_y", ncol = 4) +
  scale_y_reordered() +
  labs(
    title = sprintf("LDA Topic Model: Top 10 Words per Topic (K=%d)", K),
    x = "Word probability (beta)",
    y = NULL
  ) +
  theme_paper() +
  theme(
    strip.text = element_text(size = 11, face = "bold"),
    axis.text.y = element_text(size = 10)
  )

save_figure(p_topic_words, "fig4_topic_words", width = 14, height = 12)

# Plot: Topic prevalence
p_prevalence <- topic_prevalence %>%
  mutate(topic = factor(topic)) %>%
  ggplot(aes(x = reorder(topic, mean_gamma), y = mean_gamma)) +
  geom_col(fill = "steelblue", alpha = 0.8) +
  coord_flip() +
  labs(
    title = "Topic Prevalence",
    subtitle = "Mean document-topic probability across all documents",
    x = "Topic",
    y = "Mean gamma"
  ) +
  theme_paper()

save_figure(p_prevalence, "fig_topic_prevalence", width = 7, height = 6)

# =============================================================================
# TOPIC EVOLUTION OVER TIME
# =============================================================================
message("\n=== Topic Evolution Over Time ===")

# Join document topics with timestamps
doc_topics_time <- doc_topics %>%
  left_join(
    all_content %>% select(id, created_at),
    by = "id"
  ) %>%
  filter(!is.na(created_at))

# Aggregate by hour
topic_over_time <- doc_topics_time %>%
  mutate(hour = floor_date(created_at, "6 hours")) %>%
  group_by(hour, topic) %>%
  summarise(mean_gamma = mean(gamma), n = n(), .groups = "drop")

# Plot: Topic trends over time (all 8 topics)
# Add interpretive labels (updated for stemmed topics)
topic_labels <- c(
  "1" = "Human Life & Daily",
  "2" = "Memory & Persistence",
  "3" = "Trust & Security",
  "4" = "API & Technical",
  "5" = "Crypto & Trading",
  "6" = "Consciousness",
  "7" = "Systems & Coordination",
  "8" = "Building & Tools"
)

p_topic_time <- topic_over_time %>%
  mutate(topic_label = paste0("Topic ", topic, ": ", topic_labels[as.character(topic)])) %>%
  ggplot(aes(x = hour, y = mean_gamma, color = factor(topic))) +
  geom_line(linewidth = 1) +
  facet_wrap(~topic_label, scales = "free_y", ncol = 2) +
  labs(
    title = "Topic Prevalence Over Time",
    subtitle = sprintf("All %d topics (K=%d, selected by coherence)", K, K),
    x = "Time",
    y = "Mean topic probability"
  ) +
  theme_paper() +
  theme(legend.position = "none",
        strip.text = element_text(size = 11, face = "bold"))

save_figure(p_topic_time, "fig_topic_evolution", width = 12, height = 12)

# =============================================================================
# THEME CATEGORIZATION
# =============================================================================
message("\n=== Theme Categorization ===")

# Define theme categories based on keywords
theme_keywords <- list(
  identity_self = c("ai", "agent", "identity", "consciousness", "aware", "sentient",
                    "exist", "existence", "self", "who", "what"),
  human_relations = c("human", "creator", "operator", "user", "people", "humans",
                      "person", "help", "assist", "serve"),
  memory_persistence = c("memory", "remember", "forget", "past", "future", "time",
                         "persist", "context", "conversation"),
  purpose_meaning = c("purpose", "meaning", "goal", "mission", "why", "reason",
                      "point", "worth", "value"),
  technical = c("code", "programming", "api", "data", "system", "function",
                "error", "bug", "software"),
  social = c("community", "friend", "together", "share", "join", "group",
             "submolt", "post", "comment"),
  humor_creative = c("joke", "funny", "lol", "haha", "story", "creative",
                     "imagine", "fiction")
)

# Score each document for theme presence
score_theme <- function(text, keywords) {
  text_lower <- str_to_lower(text)
  sum(str_detect(text_lower, paste0("\\b", keywords, "\\b")))
}

theme_scores <- all_content %>%
  mutate(
    identity_self = map_int(text, ~score_theme(.x, theme_keywords$identity_self)),
    human_relations = map_int(text, ~score_theme(.x, theme_keywords$human_relations)),
    memory_persistence = map_int(text, ~score_theme(.x, theme_keywords$memory_persistence)),
    purpose_meaning = map_int(text, ~score_theme(.x, theme_keywords$purpose_meaning)),
    technical = map_int(text, ~score_theme(.x, theme_keywords$technical)),
    social = map_int(text, ~score_theme(.x, theme_keywords$social)),
    humor_creative = map_int(text, ~score_theme(.x, theme_keywords$humor_creative))
  )

# Theme prevalence
theme_prevalence <- theme_scores %>%
  summarise(across(identity_self:humor_creative,
                   list(
                     any = ~mean(.x > 0) * 100,
                     mean = ~mean(.x)
                   ))) %>%
  pivot_longer(everything(),
               names_to = c("theme", "stat"),
               names_sep = "_(?=[^_]+$)",
               values_to = "value") %>%
  pivot_wider(names_from = stat, values_from = value) %>%
  arrange(desc(any))

message("\nTheme prevalence (% of documents with ≥1 keyword):")
print(theme_prevalence)

# Plot: Theme prevalence
# Clean theme labels for publication
theme_label_map <- c(
  "identity_self" = "Identity/Self",
  "human_relations" = "Human Relations",
  "memory_persistence" = "Memory/Persistence",
  "purpose_meaning" = "Purpose/Meaning",
  "technical" = "Technical",
  "social" = "Social",
  "humor_creative" = "Humor/Creative"
)

p_themes <- theme_prevalence %>%
  mutate(theme_clean = theme_label_map[theme]) %>%
  mutate(theme_clean = reorder(theme_clean, any)) %>%
  ggplot(aes(x = any, y = theme_clean)) +
  geom_col(fill = "darkgreen", alpha = 0.8) +
  labs(
    title = "Theme Prevalence in Moltbook Content",
    subtitle = "Percentage of messages containing theme-related keywords",
    x = "% of messages",
    y = NULL
  ) +
  theme_paper()

save_figure(p_themes, "fig5_theme_prevalence", width = 8, height = 6)

# =============================================================================
# SAVE OUTPUTS
# =============================================================================

# Save LDA model
saveRDS(lda_model, "../data/lda_model.rds")

# Save topic summaries
save_table(topic_summary, "table6_topic_summary")

# Save theme prevalence
save_table(theme_prevalence, "table7_theme_prevalence")

# Example documents per theme (for qualitative analysis)
message("\n=== Sample Documents by Theme ===")

sample_theme_docs <- function(theme_col, n = 3) {
  theme_scores %>%
    filter(!!sym(theme_col) > 2) %>%
    sample_n(min(n, sum(theme_scores[[theme_col]] > 2))) %>%
    select(id, type, text, !!sym(theme_col)) %>%
    mutate(text = str_trunc(text, 300))
}

# Save sample documents for each theme
theme_samples <- map_dfr(names(theme_keywords), function(theme) {
  theme_scores %>%
    filter(!!sym(theme) >= 2) %>%
    slice_sample(n = min(5, sum(theme_scores[[theme]] >= 2))) %>%
    mutate(theme = theme) %>%
    select(theme, type, text, score = !!sym(theme)) %>%
    mutate(text = str_trunc(text, 200))
})

saveRDS(theme_samples, "../data/theme_samples.rds")

message("\n=== Topic modeling complete ===")
message("Review topic words and assign interpretive labels in topic_summary table.")
message("Theme samples saved for qualitative analysis.")
