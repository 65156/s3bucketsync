#!/usr/bin/env python3
"""
S3 Bucket Sync Script

This script copies all files from source buckets to destination buckets
based on the configuration defined in buckets.yaml.

Requirements:
- AWS credentials via environment variables
- boto3, PyYAML, tqdm packages
"""

import os
import sys
import logging
import yaml
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('s3_sync.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class S3BucketSync:
    def __init__(self, config_file='buckets.yaml'):
        """Initialize the S3 sync utility."""
        self.config_file = config_file
        self.s3_client = None
        self.bucket_configs = []
        self.total_files_copied = 0
        self.total_files_failed = 0
        
    def _initialize_s3_client(self):
        """Initialize boto3 S3 client using environment variables."""
        try:
            # Check if AWS credentials are available
            session = boto3.Session()
            credentials = session.get_credentials()
            
            if not credentials:
                raise NoCredentialsError()
                
            self.s3_client = boto3.client('s3')
            logger.info("Successfully initialized S3 client")
            
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please set the following environment variables:")
            logger.error("- AWS_ACCESS_KEY_ID")
            logger.error("- AWS_SECRET_ACCESS_KEY")
            logger.error("- AWS_SESSION_TOKEN (if using temporary credentials)")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            sys.exit(1)
    
    def _load_config(self):
        """Load bucket configuration from YAML file."""
        try:
            if not os.path.exists(self.config_file):
                logger.error(f"Configuration file {self.config_file} not found")
                sys.exit(1)
                
            with open(self.config_file, 'r') as file:
                config = yaml.safe_load(file)
                
            if 'buckets' not in config:
                logger.error("Invalid configuration: 'buckets' key not found")
                sys.exit(1)
                
            self.bucket_configs = config['buckets']
            logger.info(f"Loaded configuration for {len(self.bucket_configs)} source buckets")
            
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {str(e)}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            sys.exit(1)
    
    def _validate_bucket_exists(self, bucket_name):
        """Check if a bucket exists and is accessible."""
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"Bucket '{bucket_name}' does not exist")
            elif error_code == '403':
                logger.error(f"Access denied to bucket '{bucket_name}'")
            else:
                logger.error(f"Error accessing bucket '{bucket_name}': {str(e)}")
            return False
    
    def _list_objects(self, bucket_name):
        """List all objects in a bucket recursively."""
        objects = []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name)
            
            for page in page_iterator:
                if 'Contents' in page:
                    objects.extend(page['Contents'])
                    
            logger.info(f"Found {len(objects)} objects in bucket '{bucket_name}'")
            return objects
            
        except ClientError as e:
            logger.error(f"Error listing objects in bucket '{bucket_name}': {str(e)}")
            return []
    
    def _copy_object(self, source_bucket, dest_bucket, obj_key):
        """Copy a single object from source to destination bucket."""
        try:
            copy_source = {
                'Bucket': source_bucket,
                'Key': obj_key
            }
            
            # Use copy_object for regular files, copy for large files
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=obj_key
            )
            
            return True
            
        except ClientError as e:
            logger.error(f"Failed to copy {obj_key} from {source_bucket} to {dest_bucket}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error copying {obj_key}: {str(e)}")
            return False
    
    def _copy_objects_to_destinations(self, source_bucket, objects, dest_buckets):
        """Copy all objects from source bucket to multiple destination buckets."""
        total_operations = len(objects) * len(dest_buckets)
        
        if total_operations == 0:
            logger.info(f"No objects to copy from {source_bucket}")
            return
        
        logger.info(f"Starting copy of {len(objects)} objects to {len(dest_buckets)} destination buckets")
        
        with tqdm(total=total_operations, desc=f"Copying from {source_bucket}") as pbar:
            for obj in objects:
                obj_key = obj['Key']
                
                for dest_bucket in dest_buckets:
                    if self._copy_object(source_bucket, dest_bucket, obj_key):
                        self.total_files_copied += 1
                        logger.debug(f"✓ Copied {obj_key} to {dest_bucket}")
                    else:
                        self.total_files_failed += 1
                        logger.warning(f"✗ Failed to copy {obj_key} to {dest_bucket}")
                    
                    pbar.update(1)
                    
                    # Small delay to avoid overwhelming the API
                    time.sleep(0.01)
    
    def _process_bucket_config(self, config):
        """Process a single bucket configuration."""
        source_bucket = config['source_bucket']
        dest_buckets = config['destination_buckets']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing source bucket: {source_bucket}")
        logger.info(f"Destination buckets: {', '.join(dest_buckets)}")
        logger.info(f"{'='*60}")
        
        # Validate source bucket
        if not self._validate_bucket_exists(source_bucket):
            logger.error(f"Skipping {source_bucket} due to validation failure")
            return
        
        # Validate destination buckets
        valid_dest_buckets = []
        for dest_bucket in dest_buckets:
            if self._validate_bucket_exists(dest_bucket):
                valid_dest_buckets.append(dest_bucket)
            else:
                logger.warning(f"Skipping destination bucket {dest_bucket}")
        
        if not valid_dest_buckets:
            logger.error(f"No valid destination buckets for {source_bucket}")
            return
        
        # List objects in source bucket
        objects = self._list_objects(source_bucket)
        
        if not objects:
            logger.info(f"No objects found in source bucket {source_bucket}")
            return
        
        # Copy objects to destination buckets
        self._copy_objects_to_destinations(source_bucket, objects, valid_dest_buckets)
    
    def sync_buckets(self):
        """Main method to sync all buckets according to configuration."""
        logger.info("Starting S3 bucket sync process")
        
        # Initialize S3 client
        self._initialize_s3_client()
        
        # Load configuration
        self._load_config()
        
        # Process each bucket configuration
        start_time = time.time()
        
        for config in self.bucket_configs:
            try:
                self._process_bucket_config(config)
            except Exception as e:
                logger.error(f"Error processing bucket config: {str(e)}")
                continue
        
        # Print summary
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"\n{'='*60}")
        logger.info("SYNC SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total files copied successfully: {self.total_files_copied}")
        logger.info(f"Total files failed: {self.total_files_failed}")
        logger.info(f"Total time: {duration:.2f} seconds")
        logger.info(f"{'='*60}")
        
        if self.total_files_failed > 0:
            logger.warning(f"Sync completed with {self.total_files_failed} failures. Check logs for details.")
            sys.exit(1)
        else:
            logger.info("All files synced successfully!")


def main():
    """Main entry point."""
    print("S3 Bucket Sync Utility")
    print("=" * 30)
    
    # Create sync instance and run
    sync = S3BucketSync()
    try:
        sync.sync_buckets()
    except KeyboardInterrupt:
        logger.info("\nSync interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
