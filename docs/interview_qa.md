# Interview Q&A — Yelp Data Engineering Project

These questions will be asked if you present this project in an interview.
Every answer is tied directly to decisions made in this codebase.

---

## Architecture & Design
**Q1. Why did you choose the Yelp dataset for this project?**

> The Yelp dataset is six separate JSON files that simulate six different
> operational source systems — a business service, a reviews API, a user
> service, a checkin event tracker, a tips service, and a media service.
> Each file has a different schema, a different grain, and different quality
> problems. That forces me to build a real multi-source pipeline rather than
> a single-file ETL script. It also gives me genuine SCD Type 2 candidates
> in both the business and user dimensions, which is what I wanted to showcase.

---

**Q2. Walk me through your Bronze → Silver → Gold flow.**

> Bronze is the raw JSON files as-is in ADLS Gen2. Nothing is transformed there —
> it's the immutable replay source. Silver is where six separate PySpark notebooks
> each handle one source file: flattening nested structs, exploding arrays, FK
> validation against other Silver tables, quarantining bad rows, and computing
> derived columns. Business and users use Delta MERGE for SCD Type 2. The other
> three write as partitioned Delta with overwrite or MERGE on natural key. Gold
> is dbt on top of Silver — staging views for aliasing, dimension tables with
> SCD Type 2 point-in-time join logic, fact tables, and three pre-aggregated
> tables for dashboards.

---

## PySpark & Transformations

**Q3. The `attributes` field in business.json is a nested struct with 50+ keys
and inconsistent value types. How did you handle it?**

> The attributes values are a mess — some are plain booleans like "True",
> some are Python-style strings like "u'free'", and some are nested JSON
> strings like "{'romantic': True, 'casual': False}". I extracted the
> high-value scalar attributes by name using column references on the struct
> — e.g. `col("attributes.WiFi")` — and applied regex cleaning to strip
> the Python string formatting artifacts. For nested attributes like Ambience
> and GoodForMeal, I left them as strings in Bronze and don't surface them in
> Silver until there's a specific analytical need. This is the right call — you
> don't flatten everything speculatively. You flatten what analysts actually query.

---

**Q4. The `date` field in checkin.json is a comma-separated string of timestamps,
not a proper array. Why is that the hardest field in the dataset?**

> Because it's two problems disguised as one. First, it's the wrong type —
> a string that should be an array. Second, it means the Bronze grain is wrong
> — one row per business, but the analytical grain is one row per checkin event.
> My approach was: split on comma to get an array, explode that array into individual
> rows, then parse each element as a proper timestamp. I also added a pre-explode
> count and post-explode count assertion — if the numbers don't match, the explode
> dropped or duplicated rows. That kind of count assertion is what separates
> production-quality pipelines from scripts.

---

**Q5. How did you validate FK integrity between Silver tables?**

> Reviews and tips both reference `business_id` and `user_id`. Before writing
> to Silver, I loaded the current-version business and user IDs from their
> respective Silver Delta tables, then left-joined the incoming review/tip data
> against those sets. Rows where the join produced null on the FK side got a
> typed reject reason — "orphan_business_id" or "orphan_user_id" — and went to
> the quarantine Delta table. This is important because the Yelp dataset does
> contain orphan records — businesses that appear in reviews but not in
> business.json. Silently dropping them would make your review counts wrong.

---

**Q6. You mentioned the `friends` field is an array of user_ids that can be
thousands of entries per user. How did you handle it without blowing up Silver?**

> I stored the count only on the main Silver users table. The full friend array
> is dropped after the count is computed. If you tried to store millions of
> user_id arrays in a columnar Delta table, the storage and query performance
> would be terrible. The analytical value of the full array is minimal anyway
> — most questions are "does this user have many friends?" not "list all
> friend IDs". If someone needed a social graph for network analysis, the right
> approach is a dedicated graph database or a separate bridge table with one
> row per (user_id, friend_id) — that's a different pipeline, not something
> that belongs in the dimension table.

---

## SCD Type 2

**Q7. Explain your SCD Type 2 implementation in detail. Why two MERGE statements?**

> The MERGE pattern requires two passes. The first MERGE finds rows where
> `business_id` matches an existing current row but the `change_hash` is
> different — meaning the business has changed. It updates those rows by
> setting `valid_to = now` and `is_current = false`. The second MERGE
> inserts all incoming rows that don't already have a matching current row
> with the same hash — these are either brand new businesses or the updated
> versions of changed businesses. You need two passes because Delta MERGE
> can't both expire an old row and insert a replacement in the same statement
> without a self-join. Two clean MERGE operations are safer and easier to debug
> than one complex MERGE with multiple WHEN clauses.

---

**Q8. How does the SCD Type 2 point-in-time join work in `fact_reviews`?**

> When a review is written in 2018, it should be associated with the 2018
> version of that business — not the 2022 version where the business may have
> changed rating, closed, or moved. The join condition in `fact_reviews` is:
> `r.business_id = b.business_id AND r.review_date >= b.valid_from AND r.review_date < b.valid_to`.
> That picks exactly the version of dim_business that was active on the review
> date. For businesses with only one version — which is most of them — this
> degenerates to `is_current = true`. But for businesses that changed, the
> historical reviews get the historical version. This is the only correct way
> to build a Type 2 fact table.

---

**Q9. How did you detect when a business actually changed between pipeline runs?**

> I compute a `change_hash` — a SHA-256 hash of all the SCD-tracked columns
> concatenated with a pipe separator. The tracked columns for business are
> `stars, review_count, is_open, city, state, categories, price_range`. If
> the hash on the incoming row differs from the hash on the current Silver row
> for the same `business_id`, that triggers an expiry + insert. If hashes match,
> the MERGE does nothing — no redundant writes. The pipe separator is important:
> without it, `("AB", "C")` and `("A", "BC")` would produce the same hash.

---

## Delta Lake

**Q10. You used OPTIMIZE + ZORDER on every Silver table. What does ZORDER actually do?**

> ZORDER is a data skipping optimization specific to Delta Lake. It co-locates
> related data in the same Parquet files by sorting within each file on the
> specified columns. Delta then records the min/max of those columns per file
> in the Delta log. When a query filters on a ZORDERed column — say
> `WHERE city = 'Las Vegas'` — Delta reads the file-level statistics and
> skips any file where the max city value is lexicographically less than
> "Las Vegas". For the business Silver table, I ZORDER on `city` and `stars`
> because those are the two most common dashboard filter columns. Without
> ZORDER, Spark reads every Parquet file even for a narrow city filter.

---

**Q11. Why did you drop the raw review text in the Silver layer?**

> Raw text accounts for roughly 60% of the review.json file size. At 7 million
> reviews, keeping text in Silver would roughly double storage costs and slow
> every Silver query. The text is already preserved in Bronze — the immutable
> replay source — so it can always be reprocessed if needed. What I kept in
> Silver are derived text features: `text_char_len`, `text_word_count`, and
> `sentiment_proxy`. Those answer 95% of analytical questions about review
> quality without the storage cost. This is a real production tradeoff —
> engineers who keep raw text in Silver are usually thinking about it as a
> backup, not as an architecture decision.

---

## dbt

**Q12. What's the difference between a surrogate key and a natural key in your dim tables?**

> The natural key is `business_id` — it comes from Yelp and uniquely identifies
> a business in the real world. But in a SCD Type 2 dimension, one business can
> have multiple rows — one for each version of that business. The `business_id`
> alone isn't unique across all rows. The surrogate key — generated with
> `dbt_utils.generate_surrogate_key(['business_id', 'valid_from'])` — is unique
> per version. Fact tables like `fact_reviews` store the surrogate key as the FK,
> not the natural key. That's what makes the point-in-time join possible and
> unambiguous: a review's FK points to exactly one row in dim_business.

---

**Q13. Why three separate aggregate models instead of one big one?**

> Each aggregate answers a different set of business questions and has a
> different grain. `agg_business_monthly` is business × month — for trend
> analysis per location. `agg_city_category` is city × category × year — for
> market analysis. `agg_user_cohort` is cohort_year × review_year — for
> retention analysis. If I put them all in one model, I'd need a massive
> GROUP BY that can't serve all three use cases well, and I'd have to scan
> the entire fact table for every query. Pre-aggregated models let dashboards
> hit small, fast tables instead of 7 million fact rows on every load.

---

## Data Quality

**Q14. You have three singular dbt tests. What does each catch?**

> `assert_no_orphan_reviews` checks that every review in `fact_reviews` has
> a matching row in `dim_business` using the point-in-time join condition.
> This catches cases where the Silver FK validation missed something or the
> SCD MERGE has a gap in valid_from/valid_to coverage.
>
> `assert_scd_no_overlap` checks that no `business_id` has more than one row
> with `is_current = true`. If the MERGE logic has a bug — say it expired a
> row but then the dedup didn't work — you'd get two current rows for the same
> business, which would cause fan-out in fact joins. This test catches that.
>
> `assert_checkin_count_preserved` verifies that the explode operation
> preserved every individual checkin event. After exploding the comma-separated
> timestamp string, I stored `total_checkins_for_business` on every row. This
> test groups by `business_id` and checks that the count of rows equals
> `max(total_checkins_for_business)`. A mismatch means the explode dropped
> or duplicated rows — a silent data loss that would be invisible otherwise.

---

## Orchestration

**Q15. In your Airflow DAG, which tasks run in parallel and why?**

> Two groups run in parallel. First, `silver_business` and `silver_users` run
> simultaneously — they read independent Bronze files and write to independent
> Silver paths. Neither depends on the other. Second, `silver_reviews`,
> `silver_checkins`, and `silver_tips` all run simultaneously after ADF
> completes — again, independent sources and destinations. The dbt chain
> then runs sequentially: staging → dimensions → facts → aggregates → tests.
> Dimensions must complete before facts because facts join to them. Aggregates
> must wait for facts. The sequential dbt chain is a deliberate dependency
> constraint, not a performance limitation.

---

**Q16. Why is `max_active_runs=1` critical for this pipeline?**

> The SCD Type 2 MERGE on business and users is not idempotent under concurrent
> runs. If two DAG runs simultaneously both try to expire the same row and insert
> a replacement, you could get two "current" rows for the same business — which
> breaks the SCD contract and causes fan-out in every fact join. Delta Lake's
> optimistic concurrency control would catch and fail one of the writes, but
> that would cause a task failure rather than silent corruption. `max_active_runs=1`
> prevents the situation entirely. For pipelines with SCD MERGE, this is
> non-negotiable.

---

**Q17. How would you extend this pipeline to handle near-real-time Yelp updates?**

> The Medallion Architecture supports streaming natively. If Yelp exposed a
> Change Data Capture (CDC) feed or Kafka stream, I'd replace the ADF batch
> copy with an Event Hub consumer. The Databricks notebooks would become
> Structured Streaming jobs using `spark.readStream` with a Delta sink. The
> SCD MERGE logic stays identical — Delta's transaction log makes streaming
> MERGEs ACID-safe. The dbt Gold layer would run on a shorter schedule —
> hourly for the aggregates. The main adjustment would be in the business
> and user notebooks: instead of processing the full file each run, they'd
> process only the CDC events since the last watermark.

---

**Q18. What would you do differently if this were a real production system?**

> Three things. First, I'd add data contracts between Bronze and Silver — a
> schema registry that validates each JSON file matches the expected schema
> before the PySpark job starts. Schema drift from the source is the #1 silent
> killer in production pipelines. Second, I'd add row-level lineage — a
> `source_record_hash` on every Silver row that maps back to the exact Bronze
> file and byte offset, so I can trace any Gold number back to its raw source.
> Third, I'd instrument every notebook with structured logging to a pipeline
> metrics Delta table — row counts, quarantine rates, MERGE stats, and
> OPTIMIZE duration — so I can alert when the quarantine rate spikes above 5%,
> which is usually the first signal that something changed in the source system.
