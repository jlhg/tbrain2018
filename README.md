# Google Cloud Dataflow scripts for 2018 Trend AI contest

## Usage

```
python customer_sum.py --project $project --runner DataflowRunner \
  --staging_location gs://${bucket}/staging --temp_location gs://${bucket}/temp \
  --region asia-east1 --zone asia-east1-a --input gs://${bucket}/query_log.csv \
  --output gs://${bucket}/output/customer_sum/customer_sum
```
