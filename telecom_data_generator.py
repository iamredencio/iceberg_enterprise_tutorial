#!/usr/bin/env python3
"""
Telecom Data Generator for Apache Iceberg Tutorials

This script generates synthetic telecom time-series data for testing and
demonstration purposes. It creates realistic network performance metrics
across multiple sites, regions, and technologies.

Usage:
    python telecom_data_generator.py --sites 100 --chunks 50 --output data.csv
    
    # Save to S3
    python telecom_data_generator.py --sites 100 --chunks 50 --s3-bucket my-bucket --s3-key data/telecom_data.parquet
    
Or import as a module:
    from telecom_data_generator import generate_telecom_data
    data = generate_telecom_data(num_sites=100, num_time_chunks=50)
"""

import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import sys
import os
from typing import Tuple, List, Dict, Optional

# S3 support (optional import)
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False


class TelecomDataGenerator:
    """Generator for synthetic telecom time-series data."""
    
    def __init__(self, seed: Optional[int] = None):
        """Initialize the generator with optional random seed."""
        if seed is not None:
            np.random.seed(seed)
            random.seed(seed)
        
        # Store Python's built-in functions before any potential conflicts
        self.python_min = min
        self.python_max = max
        
        # Telecom infrastructure configuration
        self.regions = ['North', 'South', 'East', 'West', 'Central']
        self.technologies = ['4G', '5G', '6G', '7G', '8G']
        self.vendors = ['Ericsson', 'Nokia', 'Huawei', 'Samsung']
        
        # Performance baselines by technology
        self.tech_baselines = {
            '4G': {'rssi': -85, 'latency': 45, 'data_volume': 500},
            '5G': {'rssi': -80, 'latency': 25, 'data_volume': 1200},
            '6G': {'rssi': -75, 'latency': 15, 'data_volume': 2500},
            '7G': {'rssi': -70, 'latency': 8, 'data_volume': 5000},
            '8G': {'rssi': -65, 'latency': 5, 'data_volume': 8000}
        }
        
        # Initialize S3 client if available
        self.s3_client = None
        if S3_AVAILABLE:
            try:
                self.s3_client = boto3.client('s3')
            except Exception:
                pass  # S3 client initialization will be handled later if needed
    
    def generate_site_metadata(self, num_sites: int) -> pd.DataFrame:
        """Generate metadata for telecom sites."""
        sites = []
        
        for site_id in range(1, num_sites + 1):
            site = {
                'site_id': f'SITE_{site_id:04d}',
                'region': random.choice(self.regions),
                'technology': random.choice(self.technologies),
                'vendor': random.choice(self.vendors),
                'latitude': round(np.random.uniform(25.0, 49.0), 6),
                'longitude': round(np.random.uniform(-125.0, -66.0), 6),
                'installation_date': (
                    datetime.now() - timedelta(days=random.randint(30, 1825))
                ).strftime('%Y-%m-%d')
            }
            sites.append(site)
        
        return pd.DataFrame(sites)
    
    def generate_time_series_data(self, sites_df: pd.DataFrame, 
                                num_time_chunks: int) -> pd.DataFrame:
        """Generate time-series performance data for all sites."""
        data = []
        base_time = datetime.now() - timedelta(hours=num_time_chunks)
        
        print(f"Generating time-series data for {len(sites_df)} sites...")
        
        for idx, site in sites_df.iterrows():
            if idx % 50 == 0:
                print(f"Processing site {idx + 1}/{len(sites_df)}")
            
            # Get technology baseline
            tech = site['technology']
            baseline = self.tech_baselines[tech]
            
            # Generate vendor performance modifier
            vendor_modifier = {
                'Ericsson': 1.05,
                'Nokia': 1.02,
                'Huawei': 0.98,
                'Samsung': 1.01
            }[site['vendor']]
            
            for hour in range(num_time_chunks):
                timestamp = base_time + timedelta(hours=hour)
                
                # Add daily and weekly patterns
                hour_of_day = timestamp.hour
                day_of_week = timestamp.weekday()
                
                # Traffic patterns (higher during business hours and weekdays)
                traffic_multiplier = 1.0
                if 8 <= hour_of_day <= 18:  # Business hours
                    traffic_multiplier *= 1.5
                if day_of_week < 5:  # Weekdays
                    traffic_multiplier *= 1.3
                
                # Generate metrics with realistic variations
                rssi = baseline['rssi'] * vendor_modifier + np.random.normal(0, 5)
                rssi = self.python_max(-120, self.python_min(-50, rssi))
                
                latency = baseline['latency'] / vendor_modifier + np.random.normal(0, 3)
                latency = self.python_max(1, latency)
                
                data_volume = (baseline['data_volume'] * traffic_multiplier * 
                             vendor_modifier + np.random.normal(0, 100))
                data_volume = self.python_max(0, data_volume)
                
                drop_rate = np.random.exponential(0.5) * (1.1 - vendor_modifier)
                drop_rate = self.python_min(20, drop_rate)
                
                cpu_usage = (50 + 30 * traffic_multiplier + 
                           np.random.normal(0, 10)) / vendor_modifier
                cpu_usage = self.python_max(0, self.python_min(100, cpu_usage))
                
                record = {
                    'timestamp': timestamp,
                    'site_id': site['site_id'],
                    'region': site['region'],
                    'technology': site['technology'],
                    'vendor': site['vendor'],
                    'rssi_dbm': round(rssi, 2),
                    'latency_ms': round(latency, 2),
                    'data_volume_mb': round(data_volume, 2),
                    'drop_rate_percent': round(drop_rate, 3),
                    'cpu_usage_percent': round(cpu_usage, 1),
                    'latitude': site['latitude'],
                    'longitude': site['longitude']
                }
                data.append(record)
        
        return pd.DataFrame(data)
    
    def generate_telecom_data(self, num_sites: int = 100, 
                            num_time_chunks: int = 50) -> pd.DataFrame:
        """Generate complete telecom dataset."""
        print(f"Generating telecom data: {num_sites} sites, {num_time_chunks} time chunks")
        
        # Generate site metadata
        sites_df = self.generate_site_metadata(num_sites)
        print(f"Generated metadata for {len(sites_df)} sites")
        
        # Generate time-series data
        data_df = self.generate_time_series_data(sites_df, num_time_chunks)
        print(f"Generated {len(data_df):,} records")
        
        return data_df
    
    def _setup_s3_client(self, aws_access_key_id: Optional[str] = None,
                        aws_secret_access_key: Optional[str] = None,
                        aws_region: str = 'us-east-1') -> bool:
        """Setup S3 client with credentials."""
        if not S3_AVAILABLE:
            print("ERROR: boto3 is not installed. Install with: pip install boto3")
            return False
        
        try:
            # Use provided credentials or environment variables
            if aws_access_key_id and aws_secret_access_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=aws_region
                )
            else:
                # Use default credential chain (environment variables, IAM roles, etc.)
                self.s3_client = boto3.client('s3', region_name=aws_region)
            
            # Test credentials by listing buckets (or at least attempting to)
            self.s3_client.list_buckets()
            return True
            
        except NoCredentialsError:
            print("ERROR: AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables or use --aws-access-key and --aws-secret-key options.")
            return False
        except ClientError as e:
            print(f"ERROR: AWS authentication failed: {e}")
            return False
        except Exception as e:
            print(f"ERROR: Failed to setup S3 client: {e}")
            return False
    
    def save_to_s3(self, data_df: pd.DataFrame, bucket: str, key: str,
                  aws_access_key_id: Optional[str] = None,
                  aws_secret_access_key: Optional[str] = None,
                  aws_region: str = 'us-east-1') -> bool:
        """Save DataFrame to S3."""
        if not self._setup_s3_client(aws_access_key_id, aws_secret_access_key, aws_region):
            return False
        
        try:
            # Determine file format from key extension
            if key.endswith('.parquet'):
                # Save as parquet (recommended for Iceberg)
                import io
                buffer = io.BytesIO()
                data_df.to_parquet(buffer, index=False)
                buffer.seek(0)
                
                self.s3_client.upload_fileobj(buffer, bucket, key)
                print(f"Data uploaded to s3://{bucket}/{key} (Parquet format)")
                
            elif key.endswith('.csv'):
                # Save as CSV
                import io
                buffer = io.StringIO()
                data_df.to_csv(buffer, index=False)
                
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=buffer.getvalue()
                )
                print(f"Data uploaded to s3://{bucket}/{key} (CSV format)")
                
            else:
                # Default to parquet
                import io
                buffer = io.BytesIO()
                data_df.to_parquet(buffer, index=False)
                buffer.seek(0)
                
                parquet_key = key + '.parquet' if not key.endswith('.parquet') else key
                self.s3_client.upload_fileobj(buffer, bucket, parquet_key)
                print(f"Data uploaded to s3://{bucket}/{parquet_key} (Parquet format)")
            
            print(f"Dataset info: {len(data_df):,} rows, {len(data_df.columns)} columns")
            return True
            
        except ClientError as e:
            print(f"ERROR: Failed to upload to S3: {e}")
            return False
        except Exception as e:
            print(f"ERROR: Unexpected error during S3 upload: {e}")
            return False
    
    def save_data(self, data_df: pd.DataFrame, output_file: str = None,
                 s3_bucket: str = None, s3_key: str = None,
                 aws_access_key_id: str = None, aws_secret_access_key: str = None,
                 aws_region: str = 'us-east-1') -> None:
        """Save generated data to file or S3."""
        
        if s3_bucket and s3_key:
            # Save to S3
            success = self.save_to_s3(
                data_df, s3_bucket, s3_key,
                aws_access_key_id, aws_secret_access_key, aws_region
            )
            if not success:
                print("S3 upload failed. Falling back to local file save.")
                if output_file:
                    self._save_local_file(data_df, output_file)
        elif output_file:
            # Save to local file
            self._save_local_file(data_df, output_file)
        else:
            print("ERROR: No output destination specified (local file or S3)")
    
    def _save_local_file(self, data_df: pd.DataFrame, output_file: str) -> None:
        """Save DataFrame to local file."""
        if output_file.endswith('.csv'):
            data_df.to_csv(output_file, index=False)
        elif output_file.endswith('.parquet'):
            data_df.to_parquet(output_file, index=False)
        else:
            # Default to CSV
            data_df.to_csv(output_file + '.csv', index=False)
        
        print(f"Data saved to {output_file}")
        print(f"Dataset info: {len(data_df):,} rows, {len(data_df.columns)} columns")


def generate_telecom_data(num_sites: int = 100, num_time_chunks: int = 50, 
                         seed: Optional[int] = None) -> pd.DataFrame:
    """
    Convenience function to generate telecom data.
    
    Args:
        num_sites: Number of telecom sites to generate
        num_time_chunks: Number of time periods for each site
        seed: Random seed for reproducibility
    
    Returns:
        DataFrame with generated telecom time-series data
    """
    generator = TelecomDataGenerator(seed=seed)
    return generator.generate_telecom_data(num_sites, num_time_chunks)


def main():
    """Command-line interface for the data generator."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic telecom time-series data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--sites', type=int, default=100,
        help='Number of telecom sites to generate'
    )
    
    parser.add_argument(
        '--chunks', type=int, default=50,
        help='Number of time chunks (hours) per site'
    )
    
    parser.add_argument(
        '--output', type=str, default='telecom_data.csv',
        help='Output file path (supports .csv and .parquet)'
    )
    
    parser.add_argument(
        '--seed', type=int, default=None,
        help='Random seed for reproducible data generation'
    )
    
    parser.add_argument(
        '--preview', action='store_true',
        help='Show data preview instead of saving to file'
    )
    
    # S3 options
    parser.add_argument(
        '--s3-bucket', type=str, default=None,
        help='S3 bucket name for data upload'
    )
    
    parser.add_argument(
        '--s3-key', type=str, default=None,
        help='S3 key (path) for data upload'
    )
    
    parser.add_argument(
        '--aws-access-key', type=str, default=None,
        help='AWS access key ID (or use AWS_ACCESS_KEY_ID env var)'
    )
    
    parser.add_argument(
        '--aws-secret-key', type=str, default=None,
        help='AWS secret access key (or use AWS_SECRET_ACCESS_KEY env var)'
    )
    
    parser.add_argument(
        '--aws-region', type=str, default='us-east-1',
        help='AWS region for S3 operations'
    )
    
    args = parser.parse_args()
    
    # Check S3 availability if S3 options are provided
    if (args.s3_bucket or args.s3_key) and not S3_AVAILABLE:
        print("ERROR: S3 functionality requires boto3. Install with: pip install boto3")
        sys.exit(1)
    
    # Validate S3 options
    if args.s3_bucket and not args.s3_key:
        print("ERROR: --s3-key is required when --s3-bucket is specified")
        sys.exit(1)
    
    if args.s3_key and not args.s3_bucket:
        print("ERROR: --s3-bucket is required when --s3-key is specified")
        sys.exit(1)
    
    try:
        # Generate data
        generator = TelecomDataGenerator(seed=args.seed)
        data_df = generator.generate_telecom_data(args.sites, args.chunks)
        
        if args.preview:
            print("\nData Preview:")
            print("=" * 80)
            print(data_df.head(10))
            print("\nData Summary:")
            print(data_df.describe())
        else:
            # Save data
            generator.save_data(
                data_df, 
                output_file=args.output if not (args.s3_bucket and args.s3_key) else None,
                s3_bucket=args.s3_bucket,
                s3_key=args.s3_key,
                aws_access_key_id=args.aws_access_key,
                aws_secret_access_key=args.aws_secret_key,
                aws_region=args.aws_region
            )
            
    except KeyboardInterrupt:
        print("\nData generation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error generating data: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 