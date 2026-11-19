# Elasticsearch Search Engine and Retrieval Evaluation

A command-line search engine over 8,000 news and blog articles from the Signal Media One-Million News Articles dataset. It is built on Elasticsearch and reports precision and recall for every query.

University of Essex, CE706 Information Retrieval, Assignment 2 (supervisor Dr Alba García Seco de Herrera), April 2020. Joint work with Aritra Ganguly.

## What it does

1. **Indexing:** reads `signalmedia-1m.jsonl` line by line and indexes the first 7,999 articles (title, content, source, media type, publication date) into the index `news_article_dataset`.
2. **Searching:** a menu of ten query types written in the Elasticsearch query DSL:

   | # | Query | DSL used |
   |---|---|---|
   | 1 | Title words within a publication-date range | `bool.should` of `match` + `range` |
   | 2 | Title and content phrase | `bool.should` of two `match` |
   | 3 | Media type, content and date range | `bool.should` of `match` × 2 + `range` |
   | 4 | Content starting with a phrase | `match_phrase_prefix` |
   | 5 | Exact title | `match_phrase` |
   | 6 | Keyword with content weighted 2× over title | `multi_match` with `content^2` |
   | 7 | Content starting with a letter | `wildcard` |
   | 8 | Source and content | `bool.should` of two `match` |
   | 9 | All keywords present | `match` with `operator: and` |
   | 10 | Any keyword present | `match` with `operator: or` |

3. **Evaluation:** for each query, prints every hit, then a table of precision and recall at each document position, plus mean average precision and mean average recall.

`output.txt` is a recorded session showing indexing status, an exact-title query (option 5) and its evaluation output.

## Repository contents

| File | Description |
|---|---|
| `searchengine.py` | Original script (Elasticsearch client < 8, `json_lines`) |
| `searchengine_es8.py` | Same program, updated for the Elasticsearch 8 Python client (see below) |
| `tests/test_searchengine_es8.py` | Tests for the updated script. They need no server: the client is mocked |
| `output.txt` | Example session from the original script |
| `Information Retrieval Report.pdf` | Report: setup (Elasticsearch, Kibana), indexing, query design, test collection, evaluation, crowdsourcing |
| `requirements.txt` | Python dependencies for the updated script and tests |

## Running it

1. Start Elasticsearch 8 locally. With Docker:

   ```bash
   docker run -d -p 9200:9200 -e discovery.type=single-node -e xpack.security.enabled=false \
     docker.elastic.co/elasticsearch/elasticsearch:8.15.0
   ```

2. Download `signalmedia-1m.jsonl` (Signal Media One-Million News Articles dataset).
3. Run:

   ```bash
   pip install -r requirements.txt
   python searchengine_es8.py --data signalmedia-1m.jsonl --host http://localhost:9200
   ```

   Answer `no` to the first prompt to build the index, then choose queries from the menu.

4. To run the tests, which need neither a server nor the data: `python -m pytest tests/`

## About the updated script

`searchengine.py` no longer runs against Elasticsearch 8, because the `doc_type` parameter was removed. `searchengine_es8.py` keeps the same menu, the same ten queries and the same evaluation arithmetic. Its other changes are:

- host, index name and data path are command-line options;
- indexing uses the bulk API and stops reading after the first 7,999 lines, where the original read through the whole 1M-line file;
- an invalid menu choice re-prompts, where the original crashed;
- the unused `search_by_index` helper, which referred to an undefined variable, is replaced by a working `get_by_id`.

One test loads the original `mapr` function directly from `searchengine.py` and checks that the updated evaluation prints exactly the same table for the same input.

**On the evaluation:** a document counts as relevant here if the query returns it, and hits are ordered by document id rather than by score. The figures therefore describe how the result set is distributed across the 8,000-document collection, not retrieval quality against human relevance judgements. The report discusses building a judged test collection through crowdsourcing as the next step.
