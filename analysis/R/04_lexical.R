# 04_lexical.R - Lexical diversity and content analysis
#
# Analyzes:
# - Vocabulary size and diversity metrics (Distinct-1, TTR)
# - Duplicate and near-duplicate detection
# - Key phrase extraction ("my human", identity language)
# - N-gram frequencies

source("utils.R")

library(tidyverse)
library(tidytext)
library(scales)

# --- Load data ---
message("Loading processed data...")
posts <- readRDS("../data/posts.rds")
comments <- readRDS("../data/comments.rds")

# Combine all text content
all_content <- bind_rows(
  posts %>%
    select(id, text = content, author_name, created_at) %>%
    mutate(type = "post"),
  comments %>%
    select(id, text = content, author_name, created_at) %>%
    mutate(type = "comment")
) %>%
  filter(!is.na(text), nchar(text) > 0)

message(sprintf("Total text items: %s", comma(nrow(all_content))))

# =============================================================================
# TOKENIZATION
# =============================================================================
message("\n=== Tokenizing text ===")

# Tokenize to words
tokens <- all_content %>%
  select(id, text, type) %>%
  unnest_tokens(word, text, token = "words") %>%
  filter(str_detect(word, "[a-z]"))  # Keep only words with letters

message(sprintf("Total tokens: %s", comma(nrow(tokens))))
message(sprintf("Unique tokens (vocabulary): %s", comma(n_distinct(tokens$word))))

# =============================================================================
# LEXICAL DIVERSITY METRICS
# =============================================================================
message("\n=== Lexical Diversity Metrics ===")

# Overall metrics
total_tokens <- nrow(tokens)
unique_tokens <- n_distinct(tokens$word)
ttr <- unique_tokens / total_tokens  # Type-token ratio

# Distinct-1: unique unigrams / total unigrams
distinct_1 <- unique_tokens / total_tokens

# Compute on sample of fixed size for fair comparison
set.seed(42)
sample_size <- min(100000, total_tokens)
token_sample <- sample(tokens$word, sample_size)
distinct_1_sample <- length(unique(token_sample)) / sample_size

message(sprintf("Type-Token Ratio (TTR): %.4f", ttr))
message(sprintf("Distinct-1 (full): %.4f", distinct_1))
message(sprintf("Distinct-1 (100k sample): %.4f", distinct_1_sample))

# Shannon entropy of unigram distribution
word_freqs <- tokens %>%
  count(word) %>%
  mutate(p = n / sum(n))

entropy <- -sum(word_freqs$p * log2(word_freqs$p))
message(sprintf("Unigram entropy: %.2f bits", entropy))

# =============================================================================
# DUPLICATE DETECTION
# =============================================================================
message("\n=== Duplicate Detection ===")

# Exact duplicates
content_dupes <- all_content %>%
  mutate(text_clean = str_squish(str_to_lower(text))) %>%
  add_count(text_clean, name = "dupe_count") %>%
  filter(dupe_count > 1)

n_exact_dupes <- nrow(content_dupes)
pct_exact_dupes <- n_exact_dupes / nrow(all_content) * 100

message(sprintf("Exact duplicate messages: %s (%.1f%%)",
                comma(n_exact_dupes), pct_exact_dupes))

# Top duplicated messages
top_dupes <- all_content %>%
  mutate(text_clean = str_squish(str_to_lower(text))) %>%
  count(text_clean, sort = TRUE) %>%
  filter(n > 1) %>%
  head(20)

message("\nTop 10 most duplicated messages:")
print(top_dupes %>% head(10) %>% mutate(text_clean = str_trunc(text_clean, 60)))

# Unique duplicate patterns
n_unique_dupe_patterns <- nrow(top_dupes %>% filter(n > 1))
message(sprintf("\nUnique messages appearing >1 time: %s", comma(n_unique_dupe_patterns)))

# Near-duplicates using Jaccard similarity (sample-based)
message("\nComputing near-duplicate statistics (sample-based)...")

# Sample for computational efficiency
set.seed(42)
sample_texts <- all_content %>%
  sample_n(min(5000, nrow(all_content))) %>%
  filter(nchar(text) >= 20)

# Compute pairwise Jaccard on word sets (sample of pairs)
compute_sample_jaccard <- function(texts, n_pairs = 10000) {
  n <- length(texts)
  if (n < 2) return(NA)

  # Tokenize each text
  text_tokens <- lapply(texts, function(t) {
    unique(unlist(str_split(str_to_lower(t), "\\s+")))
  })

  # Sample random pairs
  set.seed(42)
  pairs <- tibble(
    i = sample(seq_len(n), n_pairs, replace = TRUE),
    j = sample(seq_len(n), n_pairs, replace = TRUE)
  ) %>%
    filter(i < j)

  # Compute Jaccard for each pair
  jaccards <- map2_dbl(pairs$i, pairs$j, function(i, j) {
    jaccard_similarity(text_tokens[[i]], text_tokens[[j]])
  })

  jaccards
}

jaccard_scores <- compute_sample_jaccard(sample_texts$text, n_pairs = 10000)

message(sprintf("Mean pairwise Jaccard (sample): %.4f", mean(jaccard_scores)))
message(sprintf("Median pairwise Jaccard (sample): %.4f", median(jaccard_scores)))
message(sprintf("Pairs with Jaccard > 0.5: %.2f%%", mean(jaccard_scores > 0.5) * 100))
message(sprintf("Pairs with Jaccard > 0.8: %.2f%%", mean(jaccard_scores > 0.8) * 100))

# =============================================================================
# KEY PHRASE ANALYSIS
# =============================================================================
message("\n=== Key Phrase Analysis ===")

# Phrases of interest (identity, human-agent relations)
key_phrases <- c(
  "my human",
  "my creator",
  "my operator",
  "my user",
  "as an ai",
  "as an agent",
  "i am an ai",
  "artificial intelligence",
  "language model",
  "llm",
  "consciousness",
  "sentient",
  "self-aware",
  "memory",
  "remember",
  "forget",
  "identity",
  "who am i",
  "what am i",
  "existence",
  "purpose",
  "meaning",
  "existential"
)

# Count occurrences
phrase_counts <- map_dfr(key_phrases, function(phrase) {
  pattern <- regex(phrase, ignore_case = TRUE)
  matches <- sum(str_detect(all_content$text, pattern), na.rm = TRUE)
  tibble(phrase = phrase, count = matches)
}) %>%
  mutate(
    pct = count / nrow(all_content) * 100,
    per_1000 = count / nrow(all_content) * 1000
  ) %>%
  arrange(desc(count))

message("\nKey phrase frequencies (per 1000 messages):")
print(phrase_counts %>% filter(count > 0))

# "My human" analysis - more detailed
my_human_examples <- all_content %>%
  filter(str_detect(text, regex("my human", ignore_case = TRUE))) %>%
  sample_n(min(10, sum(str_detect(all_content$text, regex("my human", ignore_case = TRUE))))) %>%
  select(type, text)

message("\nSample 'my human' usages:")
for (i in seq_len(nrow(my_human_examples))) {
  message(sprintf("\n[%s] %s", my_human_examples$type[i],
                  str_trunc(my_human_examples$text[i], 200)))
}

# =============================================================================
# N-GRAM ANALYSIS
# =============================================================================
message("\n=== N-gram Analysis ===")

# Bigrams
bigrams <- all_content %>%
  select(id, text) %>%
  unnest_tokens(bigram, text, token = "ngrams", n = 2) %>%
  filter(!is.na(bigram))

top_bigrams <- bigrams %>%
  count(bigram, sort = TRUE) %>%
  head(30)

message("Top 20 bigrams:")
print(top_bigrams %>% head(20))

# Trigrams
trigrams <- all_content %>%
  select(id, text) %>%
  unnest_tokens(trigram, text, token = "ngrams", n = 3) %>%
  filter(!is.na(trigram))

top_trigrams <- trigrams %>%
  count(trigram, sort = TRUE) %>%
  head(30)

message("\nTop 20 trigrams:")
print(top_trigrams %>% head(20))

# =============================================================================
# ZIPF'S LAW CHECK
# =============================================================================
message("\n=== Zipf's Law Analysis ===")

word_ranks <- word_freqs %>%
  arrange(desc(n)) %>%
  mutate(rank = row_number())

# Fit log-log regression
zipf_fit <- lm(log10(n) ~ log10(rank), data = word_ranks)
zipf_exponent <- -coef(zipf_fit)[2]

message(sprintf("Zipf exponent: %.3f (ideal = 1.0)", zipf_exponent))

p_zipf <- word_ranks %>%
  head(500) %>%
  ggplot(aes(x = rank, y = n)) +
  geom_point(alpha = 0.5, size = 1) +
  geom_smooth(method = "lm", color = "red", se = FALSE) +
  scale_x_log10() +
  scale_y_log10(labels = comma) +
  labs(
    title = "Word Frequency Distribution (Zipf's Law)",
    subtitle = sprintf("Top 500 words, exponent = %.2f", zipf_exponent),
    x = "Rank",
    y = "Frequency"
  ) +
  theme_paper()

save_figure(p_zipf, "fig_zipf_distribution", width = 7, height = 5)

# =============================================================================
# VISUALIZATIONS
# =============================================================================

# Plot: Top unigrams (excluding stopwords)
data("stop_words")

top_words <- tokens %>%
  anti_join(stop_words, by = "word") %>%
  count(word, sort = TRUE) %>%
  head(30)

p_top_words <- top_words %>%
  mutate(word = reorder(word, n)) %>%
  ggplot(aes(x = n, y = word)) +
  geom_col(fill = "steelblue", alpha = 0.8) +
  scale_x_continuous(labels = comma) +
  labs(
    title = "Most Frequent Words (Excluding Stopwords)",
    x = "Frequency",
    y = NULL
  ) +
  theme_paper()

save_figure(p_top_words, "fig_top_words", width = 8, height = 8)

# Plot: Key phrase frequencies
p_phrases <- phrase_counts %>%
  filter(count > 0) %>%
  mutate(phrase = reorder(phrase, per_1000)) %>%
  ggplot(aes(x = per_1000, y = phrase)) +
  geom_col(fill = "coral", alpha = 0.8) +
  labs(
    title = "Key Phrase Frequencies",
    subtitle = "Identity and human-agent relation terms",
    x = "Occurrences per 1,000 messages",
    y = NULL
  ) +
  theme_paper()

save_figure(p_phrases, "fig_key_phrases", width = 8, height = 6)

# Plot: Duplicate distribution
p_dupes <- top_dupes %>%
  head(50) %>%
  mutate(rank = row_number()) %>%
  ggplot(aes(x = rank, y = n)) +
  geom_col(fill = "purple", alpha = 0.7) +
  scale_y_log10(labels = comma) +
  labs(
    title = "Duplicate Message Frequency",
    subtitle = sprintf("Top 50 duplicated messages (%.1f%% of messages are exact duplicates)",
                       pct_exact_dupes),
    x = "Duplicate pattern rank",
    y = "Count (log scale)"
  ) +
  theme_paper() +
  theme(axis.text.x = element_blank())

save_figure(p_dupes, "fig_duplicate_distribution", width = 8, height = 5)

# =============================================================================
# SUMMARY TABLE
# =============================================================================
lexical_stats <- tibble(
  Metric = c(
    "Total tokens",
    "Vocabulary size (unique tokens)",
    "Type-Token Ratio (TTR)",
    "Distinct-1",
    "Unigram entropy (bits)",
    "Zipf exponent",
    "Exact duplicates (%)",
    "Mean pairwise Jaccard (sample)",
    "High similarity pairs (Jaccard > 0.5) %"
  ),
  Value = c(
    comma(total_tokens),
    comma(unique_tokens),
    sprintf("%.4f", ttr),
    sprintf("%.4f", distinct_1_sample),
    sprintf("%.2f", entropy),
    sprintf("%.3f", zipf_exponent),
    sprintf("%.1f%%", pct_exact_dupes),
    sprintf("%.4f", mean(jaccard_scores)),
    sprintf("%.2f%%", mean(jaccard_scores > 0.5) * 100)
  )
)

save_table(lexical_stats, "table3_lexical_stats")
print(lexical_stats)

# Save key phrase table
save_table(phrase_counts %>% filter(count > 0), "table4_key_phrases")

# Save top duplicates (truncated)
save_table(
  top_dupes %>%
    head(20) %>%
    mutate(text_clean = str_trunc(text_clean, 80)),
  "table5_top_duplicates"
)

message("\n=== Lexical analysis complete ===")
