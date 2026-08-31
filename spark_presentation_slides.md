#  Spark & Delta Lake Optimization



---

##  `repartition()` vs `coalesce()`

![PySpark Repartition vs Coalesce](images-ss/Screenshot%202026-08-27%20at%201.34.51 PM.png)

###  Key  Points:
- **`repartition(n)`**: Triggers a **full network data shuffle**. Balances data uniformly across worker nodes. Use to fix data skew or increase parallelism.
- **`coalesce(n)`**: **No network shuffle**. Merges adjacent local partitions on the same node. Use to decrease partition count before writing to disk.
- **Pro-Tip**: Use `coalesce(1)` instead of `repartition(1)` right before saving small output files to avoid unnecessary network I/O.

---

## Broadcast Joins (Map-Side Joins)

![Spark Broadcast Join Architecture](images-ss/Screenshot%202026-08-27%20at%201.42.01 PM.png)

###  Key  Points:
- **How it works**: The Spark Driver broadcasts the small table (< 10 MB default) to the RAM of **all executor nodes**.
- **Performance Win**: Converts a heavy network shuffle join into a fast **local map-side join**.
- **PySpark Syntax**: `df_large.join(broadcast(df_small), "key")`
- **Warning**: Do not broadcast tables > 1 GB to avoid Driver / Executor Out-Of-Memory (`OOM`) crashes.

---

## Delta Lake `OPTIMIZE` & Z-Ordering

![Delta Lake OPTIMIZE and Z-Order](images-ss/Screenshot%202026-08-27%20at%201.43.34 PM.png)

![Delta Lake OPTIMIZE and Z-Order](images-ss/Screenshot%202026-08-27%20at%201.46.37 PM.png)


###  Key  Points:
- **`OPTIMIZE` (Compaction)**: Solves the **Small File Problem** by bin-packing thousands of tiny Parquet files into uniform **1 GB files**.
- **`ZORDER BY` (Multidimensional Clustering)**: Clusters data *inside* Parquet files along specified high-cardinality columns (e.g. `customer_id`).
- **Data Skipping**: Leverages min/max statistics in `_delta_log` to bypass 90%+ of unneeded files during read queries, reducing query times from minutes to seconds.

---

## Data Skew & The Salting Technique

![PySpark Data Skew vs Salting Technique](images-ss/Screenshot%202026-08-27%20at%201.49.04 PM.png)

###  Key  Points:
- **The Skew Problem**: Uneven key distribution leaves 1 worker core overloaded (stuck at 99% for an hour) while other cores sit idle.
- **AQE Solution**: Set `spark.sql.adaptive.skewJoin.enabled = true` for automatic dynamic splitting.
- **Salting Technique**: Appends a random integer (`0..N-1`) to the skewed join key in the main dataset, and explodes lookup table keys by `0..N-1` so hot keys are distributed evenly across worker cores.
