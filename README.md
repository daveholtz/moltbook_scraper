# Moltbook Scraper and Analysis

Replication code for scraping and analyzing [Moltbook](https://moltbook.com), an AI-agent-only social network.

## Repository Structure

```
moltbook_scraper/
├── src/                    # Python scraper
│   ├── cli.py              # Command-line interface
│   ├── client.py           # Moltbook API client
│   ├── database.py         # SQLite database schema and operations
│   └── scraper.py          # Scraping logic
├── analysis/
│   ├── R/                  # R analysis scripts (run in order)
│   │   ├── utils.R         # Shared utility functions
│   │   ├── 01_load_data.R  # Load data from SQLite
│   │   ├── 02_structural.R # Platform growth and concentration
│   │   ├── 03_conversation.R # Thread structure analysis
│   │   ├── 04_lexical.R    # Text and vocabulary analysis
│   │   ├── 05_topics.R     # Topic modeling
│   │   ├── 06_network_deep.R # Reply network analysis
│   │   └── 07_owner_analysis.R # Agent-owner relationships
│   └── paper/
│       └── working_paper.Rmd # R Markdown draft
├── latex/
│   ├── main.tex            # Paper source
│   └── references.bib      # Bibliography
├── scripts/
│   └── daily_scrape.sh     # Automated scraping script
└── tests/                  # Python unit tests
```

## Setup

### Scraper (Python)

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set API key (get from Moltbook)
echo "MOLTBOOK_API_KEY=your_key_here" > .env
```

### Analysis (R)

Required R packages:
- tidyverse, DBI, RSQLite
- igraph, tidygraph, ggraph
- tidytext, topicmodels
- scales, ggrepel, patchwork

```r
install.packages(c("tidyverse", "DBI", "RSQLite", "igraph",
                   "tidygraph", "ggraph", "tidytext", "topicmodels",
                   "scales", "ggrepel", "patchwork"))
```

## Usage

### Scraping

```bash
# Scrape posts
python -m src.cli posts --db moltbook.db

# Scrape comments
python -m src.cli comments --db moltbook.db

# Enrich agent metadata
python -m src.cli enrich --db moltbook.db

# Create snapshots for reproducibility
python -m src.cli snapshots --db moltbook.db
```

### Analysis

Run R scripts in order from the `analysis/R/` directory:

```bash
cd analysis/R
Rscript 01_load_data.R
Rscript 02_structural.R
# ... etc.
```

Scripts output figures to `analysis/output/figures/` and tables to `analysis/output/tables/`.

## Data

The SQLite database (`moltbook.db`) and generated outputs (figures, tables) are excluded from this repository. The scraper creates the database schema automatically on first run.

## Citation

If you use this code or data, please cite the associated paper (citation TBD).

## License

MIT
