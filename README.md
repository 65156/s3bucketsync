# S3 Bucket Sync

A simple Python utility for bulk copying files from source S3 buckets to destination buckets based on YAML configuration.

## Features

- **Bulk Operations**: Copy all files from multiple source buckets to multiple destination buckets
- **YAML Configuration**: Easy-to-manage bucket mapping via `buckets.yaml`
- **Progress Tracking**: Real-time progress bars with `tqdm`
- **Comprehensive Logging**: Detailed logs to both file and console
- **Validation**: Pre-flight checks for bucket access and permissions
- **Error Handling**: Graceful handling of AWS credential and access issues

## Important Limitations

⚠️ **This tool performs simple one-way copying and does NOT support:**
- Timestamp-based sync (incremental sync)
- "Sync from destination if newer" functionality
- File comparison based on modification dates
- Bidirectional synchronization

**All files are copied and overwritten regardless of their timestamps.**

## Prerequisites

- Python 3.7+
- AWS credentials configured via:
  - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
  - AWS credentials file (`~/.aws/credentials`)
  - IAM roles (if running on EC2)
- Required permissions: `s3:ListBucket`, `s3:GetObject`, `s3:PutObject` on source and destination buckets

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your bucket mappings in `buckets.yaml`

## Configuration

Edit `buckets.yaml` to define your source and destination bucket mappings:

```yaml
buckets:
  - source_bucket: my-source-bucket-1
    destination_buckets:
      - my-dest-bucket-1
      - my-dest-bucket-2
  - source_bucket: my-source-bucket-2
    destination_buckets:
      - my-dest-bucket-3
```

## Usage

Run the sync operation:

```bash
python s3_sync.py
```

The script will:
1. Validate AWS credentials and bucket access
2. List all objects in each source bucket
3. Copy all objects to the specified destination buckets
4. Display progress and generate detailed logs

## Logs

- Console output: Real-time progress and status
- Log file: `s3_sync.log` with detailed operation history

## Exit Codes

- `0`: Success - all files copied successfully
- `1`: Failure - one or more files failed to copy or other errors occurred

## Example Output

```
S3 Bucket Sync Utility
==============================
2024-10-30 10:15:23 - INFO - Successfully initialized S3 client
2024-10-30 10:15:23 - INFO - Loaded configuration for 5 source buckets
2024-10-30 10:15:24 - INFO - Found 150 objects in bucket 'source-bucket-1'
Copying from source-bucket-1: 100%|████████| 300/300 [01:23<00:00,  3.62it/s]
==============================
SYNC SUMMARY
==============================
Total files copied successfully: 300
Total files failed: 0
Total time: 83.45 seconds
==============================
```

## Dependencies

- `boto3` - AWS SDK for Python
- `PyYAML` - YAML configuration parsing
- `tqdm` - Progress bar functionality
