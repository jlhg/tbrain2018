# Google Cloud Dataflow scripts for Trend AI content

## Usage

```
python customer_sum.py --project $project --runner DataflowRunner \
  --staging_location gs://${bucket}/staging --temp_location gs://${bucket}/temp \
  --input gs://${bucket}/query_log.csv --output gs://${bucket}/output/customer_sum/customer_sum
```
