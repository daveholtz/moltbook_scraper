# 06_network_deep.R - Deep network structure analysis
#
# Analyzes:
# - Degree distributions (in/out/total)
# - Clustering coefficients (global and local distribution)
# - Average path length and diameter
# - Network visualization
# - Directed vs undirected comparison
# - Community detection

source("utils.R")

library(tidyverse)
library(igraph)
library(scales)

# --- Load data ---
message("Loading processed data...")
posts <- readRDS("../data/posts.rds")
comments <- readRDS("../data/comments.rds")

# =============================================================================
# BUILD NETWORKS
# =============================================================================
message("\n=== Building Networks ===")

# Get post authors
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
    target = coalesce(parent_author, post_author)
  ) %>%
  filter(!is.na(target), author_name != target) %>%
  select(from = author_name, to = target) %>%
  count(from, to, name = "weight")

message(sprintf("Total edges (weighted): %d", nrow(reply_edges)))
message(sprintf("Total interactions: %d", sum(reply_edges$weight)))

# --- Directed graph ---
g_dir <- graph_from_data_frame(reply_edges, directed = TRUE)
message(sprintf("\nDirected graph: %d nodes, %d edges", vcount(g_dir), ecount(g_dir)))

# --- Undirected graph (collapse edges) ---
g_undir <- as_undirected(g_dir, mode = "collapse", edge.attr.comb = list(weight = "sum"))
message(sprintf("Undirected graph: %d nodes, %d edges", vcount(g_undir), ecount(g_undir)))

# =============================================================================
# DEGREE DISTRIBUTIONS
# =============================================================================
message("\n=== Degree Distribution Analysis ===")

# Directed degrees
in_deg <- degree(g_dir, mode = "in")
out_deg <- degree(g_dir, mode = "out")
total_deg_dir <- degree(g_dir, mode = "all")

# Undirected degree
deg_undir <- degree(g_undir)

degree_stats <- tibble(
  Metric = c(
    "Mean in-degree", "Median in-degree", "Max in-degree",
    "Mean out-degree", "Median out-degree", "Max out-degree",
    "Mean degree (undirected)", "Median degree (undirected)", "Max degree (undirected)"
  ),
  Value = c(
    sprintf("%.2f", mean(in_deg)), sprintf("%.0f", median(in_deg)), sprintf("%d", max(in_deg)),
    sprintf("%.2f", mean(out_deg)), sprintf("%.0f", median(out_deg)), sprintf("%d", max(out_deg)),
    sprintf("%.2f", mean(deg_undir)), sprintf("%.0f", median(deg_undir)), sprintf("%d", max(deg_undir))
  )
)
print(degree_stats)

# Plot: Degree distributions (log-log)
degree_df <- tibble(
  degree = c(in_deg, out_deg, deg_undir),
  type = c(rep("In-degree", length(in_deg)),
           rep("Out-degree", length(out_deg)),
           rep("Undirected", length(deg_undir)))
)

# Frequency tables for log-log plot
degree_freq <- degree_df %>%
  filter(degree > 0) %>%
  group_by(type, degree) %>%
  summarise(count = n(), .groups = "drop")

p_degree_loglog <- degree_freq %>%
  ggplot(aes(x = degree, y = count, color = type)) +
  geom_point(alpha = 0.6, size = 2) +
  scale_x_log10() +
  scale_y_log10() +
  facet_wrap(~type) +
  labs(
    title = "Degree Distribution (Log-Log)",
    x = "Degree",
    y = "Count"
  ) +
  theme_paper() +
  theme(legend.position = "none")

save_figure(p_degree_loglog, "fig_network_degree_loglog", width = 10, height = 4)

# CCDF plot
degree_ccdf <- degree_df %>%
  filter(degree > 0) %>%
  group_by(type) %>%
  arrange(desc(degree)) %>%
  mutate(
    rank = row_number(),
    ccdf = rank / n()
  ) %>%
  ungroup()

p_degree_ccdf <- degree_ccdf %>%
  ggplot(aes(x = degree, y = ccdf, color = type)) +
  geom_line(linewidth = 1) +
  scale_x_log10() +
  scale_y_log10(labels = percent) +
  labs(
    title = "Degree Distribution CCDF",
    subtitle = "Complementary cumulative distribution",
    x = "Degree",
    y = "P(X >= x)"
  ) +
  theme_paper()

save_figure(p_degree_ccdf, "fig_network_degree_ccdf", width = 8, height = 5)

# Power law exponent estimates
alpha_in <- estimate_power_law_exponent(in_deg[in_deg > 0])
alpha_out <- estimate_power_law_exponent(out_deg[out_deg > 0])
alpha_undir <- estimate_power_law_exponent(deg_undir[deg_undir > 0])

message(sprintf("\nPower law exponents:"))
message(sprintf("  In-degree alpha: %.2f", alpha_in))
message(sprintf("  Out-degree alpha: %.2f", alpha_out))
message(sprintf("  Undirected alpha: %.2f", alpha_undir))

# =============================================================================
# CLUSTERING COEFFICIENTS
# =============================================================================
message("\n=== Clustering Coefficient Analysis ===")

# Global clustering (transitivity)
global_clust_dir <- transitivity(g_dir, type = "global")
global_clust_undir <- transitivity(g_undir, type = "global")

message(sprintf("Global clustering (directed): %.4f", global_clust_dir))
message(sprintf("Global clustering (undirected): %.4f", global_clust_undir))

# Local clustering coefficients (undirected)
local_clust <- transitivity(g_undir, type = "local")
local_clust <- local_clust[!is.nan(local_clust)]

message(sprintf("Mean local clustering: %.4f", mean(local_clust, na.rm = TRUE)))
message(sprintf("Median local clustering: %.4f", median(local_clust, na.rm = TRUE)))

# Plot: Local clustering distribution
p_clustering <- tibble(clustering = local_clust) %>%
  filter(!is.na(clustering)) %>%
  ggplot(aes(x = clustering)) +
  geom_histogram(bins = 50, fill = "steelblue", alpha = 0.8) +
  labs(
    title = "Local Clustering Coefficient Distribution",
    subtitle = sprintf("Mean = %.3f, Median = %.3f (undirected graph)",
                       mean(local_clust, na.rm = TRUE), median(local_clust, na.rm = TRUE)),
    x = "Local clustering coefficient",
    y = "Count"
  ) +
  theme_paper()

save_figure(p_clustering, "fig_network_clustering_dist", width = 8, height = 5)

# =============================================================================
# PATH LENGTH AND DIAMETER
# =============================================================================
message("\n=== Path Length Analysis ===")

# For large graphs, computing all-pairs shortest paths is expensive
# Sample approach: compute on largest connected component, or sample

# Get largest connected component (undirected)
components <- components(g_undir)
largest_cc_idx <- which.max(components$csize)
lcc_nodes <- which(components$membership == largest_cc_idx)
g_lcc <- induced_subgraph(g_undir, lcc_nodes)

message(sprintf("Largest connected component: %d nodes (%.1f%% of network)",
                vcount(g_lcc), 100 * vcount(g_lcc) / vcount(g_undir)))

# If LCC is small enough, compute exact metrics
if (vcount(g_lcc) <= 5000) {
  message("Computing exact path lengths on LCC...")

  avg_path <- mean_distance(g_lcc, directed = FALSE)
  diameter_val <- diameter(g_lcc, directed = FALSE)

  message(sprintf("Average path length: %.2f", avg_path))
  message(sprintf("Diameter: %d", diameter_val))
} else {
  message("LCC too large for exact computation. Sampling...")

  # Sample-based estimation
  set.seed(42)
  sample_nodes <- sample(V(g_lcc), min(1000, vcount(g_lcc)))

  # Compute distances from sample nodes
  sample_distances <- distances(g_lcc, v = sample_nodes, to = V(g_lcc))
  sample_distances <- sample_distances[is.finite(sample_distances) & sample_distances > 0]

  avg_path <- mean(sample_distances)
  diameter_val <- max(sample_distances)

  message(sprintf("Estimated average path length (sampled): %.2f", avg_path))
  message(sprintf("Estimated diameter (sampled): %d", diameter_val))
}

# =============================================================================
# RECIPROCITY ANALYSIS
# =============================================================================
message("\n=== Reciprocity Analysis ===")

recip <- reciprocity(g_dir)
message(sprintf("Reciprocity: %.3f", recip))

# Dyad census
dyads <- dyad_census(g_dir)
message(sprintf("Mutual dyads: %d", dyads$mut))
message(sprintf("Asymmetric dyads: %d", dyads$asym))
message(sprintf("Null dyads: %d", dyads$null))

# =============================================================================
# COMMUNITY DETECTION
# =============================================================================
message("\n=== Community Detection ===")

# Use Louvain algorithm on undirected graph
set.seed(42)
communities <- cluster_louvain(g_undir)

n_communities <- length(unique(membership(communities)))
modularity_val <- modularity(communities)

message(sprintf("Number of communities (Louvain): %d", n_communities))
message(sprintf("Modularity: %.3f", modularity_val))

# Community size distribution
comm_sizes <- sizes(communities)
message(sprintf("Largest community: %d nodes", max(comm_sizes)))
message(sprintf("Median community size: %.0f", median(comm_sizes)))

# Plot: Community size distribution
p_community_sizes <- tibble(size = as.numeric(comm_sizes)) %>%
  count(size, name = "n_communities") %>%
  ggplot(aes(x = size, y = n_communities)) +
  geom_point(alpha = 0.6, size = 2) +
  scale_x_log10() +
  scale_y_log10() +
  labs(
    title = "Community Size Distribution",
    subtitle = sprintf("%d communities detected (Louvain), modularity = %.3f",
                       n_communities, modularity_val),
    x = "Community size (nodes)",
    y = "Number of communities"
  ) +
  theme_paper()

save_figure(p_community_sizes, "fig_network_community_sizes", width = 8, height = 5)

# =============================================================================
# NETWORK VISUALIZATION
# =============================================================================
message("\n=== Network Visualization ===")

# For visualization, we need to sample or use only high-degree nodes
# Option 1: Top nodes by degree
top_n <- 500
top_nodes <- names(sort(deg_undir, decreasing = TRUE)[1:top_n])
g_viz <- induced_subgraph(g_undir, top_nodes)

message(sprintf("Visualization subgraph: %d nodes, %d edges", vcount(g_viz), ecount(g_viz)))

# Add community membership to viz graph
viz_membership <- membership(communities)[V(g_viz)$name]
V(g_viz)$community <- viz_membership

# Add degree for sizing
V(g_viz)$degree <- degree(g_viz)

# Layout
message("Computing layout...")
set.seed(42)
layout_fr <- layout_with_fr(g_viz, niter = 500)

# Plot using base igraph (ggraph can be slow for large graphs)
pdf("../output/figures/fig_network_visualization.pdf", width = 12, height = 12)

# Color by community (limit to top 10 communities by size in this subgraph)
comm_in_viz <- table(viz_membership)
top_comms <- names(sort(comm_in_viz, decreasing = TRUE)[1:10])
node_colors <- ifelse(as.character(viz_membership) %in% top_comms,
                      as.character(viz_membership), "other")
color_palette <- c(rainbow(10), "gray80")
names(color_palette) <- c(top_comms, "other")

plot(g_viz,
     layout = layout_fr,
     vertex.size = sqrt(V(g_viz)$degree) * 0.8,
     vertex.label = NA,
     vertex.color = color_palette[node_colors],
     edge.width = 0.3,
     edge.color = rgb(0.5, 0.5, 0.5, 0.2),
     main = sprintf("Reply Network (Top %d nodes by degree)", top_n))

dev.off()
message("Saved: ../output/figures/fig_network_visualization.pdf")

# Also save as PNG
png("../output/figures/fig_network_visualization.png", width = 1200, height = 1200, res = 100)
plot(g_viz,
     layout = layout_fr,
     vertex.size = sqrt(V(g_viz)$degree) * 0.8,
     vertex.label = NA,
     vertex.color = color_palette[node_colors],
     edge.width = 0.3,
     edge.color = rgb(0.5, 0.5, 0.5, 0.2),
     main = sprintf("Reply Network (Top %d nodes by degree)", top_n))
dev.off()
message("Saved: ../output/figures/fig_network_visualization.png")

# =============================================================================
# ASSORTATIVITY
# =============================================================================
message("\n=== Assortativity ===")

# Degree assortativity (do high-degree nodes connect to high-degree nodes?)
assort_degree <- assortativity_degree(g_undir)
message(sprintf("Degree assortativity: %.3f", assort_degree))

# Interpretation
if (assort_degree > 0) {
  message("  -> Assortative: high-degree nodes tend to connect to high-degree nodes")
} else {
  message("  -> Disassortative: high-degree nodes tend to connect to low-degree nodes")
}

# =============================================================================
# HUB AND AUTHORITY SCORES (DIRECTED)
# =============================================================================
message("\n=== Hub and Authority Analysis (Directed) ===")

hub_scores <- hub_score(g_dir)$vector
auth_scores <- authority_score(g_dir)$vector

# Top hubs (agents who reply to many others)
top_hubs <- sort(hub_scores, decreasing = TRUE)[1:10]
message("\nTop 10 Hubs (reply to many):")
print(tibble(agent = names(top_hubs), hub_score = round(top_hubs, 4)))

# Top authorities (agents who receive many replies)
top_auths <- sort(auth_scores, decreasing = TRUE)[1:10]
message("\nTop 10 Authorities (receive many replies):")
print(tibble(agent = names(top_auths), authority_score = round(top_auths, 4)))

# =============================================================================
# SUMMARY TABLE
# =============================================================================
message("\n=== Network Summary ===")

network_summary <- tibble(
  Metric = c(
    "Nodes",
    "Edges (directed)",
    "Edges (undirected)",
    "Density",
    "Reciprocity",
    "Global clustering (directed)",
    "Global clustering (undirected)",
    "Mean local clustering",
    "Average path length (LCC)",
    "Diameter (LCC)",
    "LCC size (% of network)",
    "Number of communities",
    "Modularity",
    "Degree assortativity",
    "Power law exponent (in-degree)",
    "Power law exponent (out-degree)",
    "Power law exponent (undirected)"
  ),
  Value = c(
    comma(vcount(g_dir)),
    comma(ecount(g_dir)),
    comma(ecount(g_undir)),
    sprintf("%.6f", edge_density(g_dir)),
    sprintf("%.3f", recip),
    sprintf("%.4f", global_clust_dir),
    sprintf("%.4f", global_clust_undir),
    sprintf("%.4f", mean(local_clust, na.rm = TRUE)),
    sprintf("%.2f", avg_path),
    sprintf("%d", diameter_val),
    sprintf("%.1f%%", 100 * vcount(g_lcc) / vcount(g_undir)),
    comma(n_communities),
    sprintf("%.3f", modularity_val),
    sprintf("%.3f", assort_degree),
    sprintf("%.2f", alpha_in),
    sprintf("%.2f", alpha_out),
    sprintf("%.2f", alpha_undir)
  )
)

save_table(network_summary, "table8_network_summary")
print(network_summary)

# Save graph objects for later use
saveRDS(g_dir, "../data/network_directed.rds")
saveRDS(g_undir, "../data/network_undirected.rds")
saveRDS(communities, "../data/network_communities.rds")

message("\n=== Deep network analysis complete ===")
