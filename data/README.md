# Input data

The pipelines read these files from `hdfs://localhost:9000/user/hadoop/project/`. Upload them with:

```bash
bash scripts/load_data_to_hdfs.sh data
```

| File | Format | Size | Rows | Used by | Contents |
| --- | --- | --- | --- | --- | --- |
| `order_items.csv` | CSV | 37.5 MB | 397,569 | `ecommerce_csv_analytics.py` | One row per order line: order and product IDs, quantity, unit price, discount, gross and net sales, tax, shipping, cost and profit |
| `customer_master.csv` | CSV | 2.2 MB | 25,000 | `ecommerce_csv_analytics.py` | Customer profile: name, age, gender, segment, city, state, country, region, postal code and acquisition cost |
| `product_catalog.csv` | CSV | 155 KB | 1,175 | `ecommerce_csv_analytics.py` | Product name, category, subcategory, brand, supplier, unit price, cost and rating |
| `userdata.parquet` | Parquet | 114 KB | 1,000 | `userdata_parquet_etl.py` | 13 columns: `registration_dttm`, `id`, `first_name`, `last_name`, `email`, `gender`, `ip_address`, `cc`, `country`, `birthdate`, `salary`, `title`, `comments` |
| `airports.json` | Multi-line JSON | 5 KB | 30 airports | `airports_json_analysis.py` | An `airports` array; each airport has `code`, `name`, `city`, `country`, `lat` and `lon` |
| `dataset_statistics.csv` | CSV | < 1 KB | 1 | Reference only | Summary of the e-commerce source data: transactions, customers, products, date range, revenue, profit, average order value, rating, return and cancellation rates |

## Not included yet

`ecommerce_sales_customer_analytics_150k.csv` (138,116 orders) is also read by `ecommerce_csv_analytics.py`. It holds one row per order with date, status, channel, payment, shipping, delivery, returns, reviews, marketing and customer attributes. Add it to this folder before running the e-commerce pipeline.

## Shape of `airports.json`

```json
{
  "airports": [
    {
      "code": "ATL",
      "name": "Hartsfield-Jackson Atlanta International",
      "city": "Atlanta",
      "country": "US",
      "lat": 33.6407,
      "lon": -84.4277
    }
  ]
}
```
