# utils/backup.py
import os
import subprocess
import logging
from datetime import datetime
import boto3
import gzip
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def backup_database(db_name="airflow", db_user="airflow", db_password="airflow", db_host="postgres"):
    """Create a backup of the PostgreSQL database"""
    try:
        # Create backup directory if it doesn't exist
        backup_dir = "/opt/airflow/backups"
        os.makedirs(backup_dir, exist_ok=True)
        
        # Backup filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"{backup_dir}/db_backup_{timestamp}.sql"
        compressed_file = f"{backup_file}.gz"
        
        # Set PostgreSQL password environment variable
        env = os.environ.copy()
        env["PGPASSWORD"] = db_password
        
        # Execute pg_dump command
        cmd = [
            "pg_dump",
            "-h", db_host,
            "-U", db_user,
            "-d", db_name,
            "-f", backup_file,
            "-F", "p"  # Plain-text SQL format
        ]
        
        logger.info(f"Starting database backup to {backup_file}")
        process = subprocess.run(cmd, env=env, check=True, capture_output=True)
        
        # Compress the backup file
        with open(backup_file, 'rb') as f_in:
            with gzip.open(compressed_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Remove the uncompressed file
        os.remove(backup_file)
        
        logger.info(f"Database backup completed: {compressed_file}")
        
        return compressed_file
    except subprocess.CalledProcessError as e:
        logger.error(f"Database backup failed: {e.stderr.decode()}")
        return None
    except Exception as e:
        logger.error(f"Error during database backup: {str(e)}")
        return None

def upload_backup_to_s3(backup_file, bucket_name="trading-system-backups", region="ap-southeast-1"):
    """Upload backup file to S3"""
    try:
        if not os.path.exists(backup_file):
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        # Get file name from path
        file_name = os.path.basename(backup_file)
        
        # Create S3 client
        s3_client = boto3.client('s3', region_name=region)
        
        # Upload file
        logger.info(f"Uploading {file_name} to S3 bucket {bucket_name}")
        s3_client.upload_file(backup_file, bucket_name, file_name)
        
        logger.info(f"Successfully uploaded {file_name} to S3")
        return True
    except Exception as e:
        logger.error(f"Error uploading backup to S3: {str(e)}")
        return False

def restore_database_from_backup(backup_file, db_name="airflow", db_user="airflow", db_password="airflow", db_host="postgres"):
    """Restore database from backup file"""
    try:
        # Check if file exists
        if not os.path.exists(backup_file):
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        # If file is compressed, uncompress it
        if backup_file.endswith('.gz'):
            uncompressed_file = backup_file[:-3]  # Remove .gz extension
            with gzip.open(backup_file, 'rb') as f_in:
                with open(uncompressed_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            backup_file = uncompressed_file
        
        # Set PostgreSQL password environment variable
        env = os.environ.copy()
        env["PGPASSWORD"] = db_password
        
        # Execute psql command to restore database
        cmd = [
            "psql",
            "-h", db_host,
            "-U", db_user,
            "-d", db_name,
            "-f", backup_file
        ]
        
        logger.info(f"Starting database restore from {backup_file}")
        process = subprocess.run(cmd, env=env, check=True, capture_output=True)
        
        logger.info("Database restore completed successfully")
        
        # Remove the uncompressed file if we created it
        if backup_file.endswith('.sql') and os.path.exists(backup_file + '.gz'):
            os.remove(backup_file)
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Database restore failed: {e.stderr.decode()}")
        return False
    except Exception as e:
        logger.error(f"Error during database restore: {str(e)}")
        return False

def download_backup_from_s3(file_name, bucket_name="trading-system-backups", region="ap-southeast-1"):
    """Download backup file from S3"""
    try:
        # Create backup directory if it doesn't exist
        backup_dir = "/opt/airflow/backups"
        os.makedirs(backup_dir, exist_ok=True)
        
        # Local file path
        local_file = f"{backup_dir}/{file_name}"
        
        # Create S3 client
        s3_client = boto3.client('s3', region_name=region)
        
        # Download file
        logger.info(f"Downloading {file_name} from S3 bucket {bucket_name}")
        s3_client.download_file(bucket_name, file_name, local_file)
        
        logger.info(f"Successfully downloaded {file_name} from S3")
        return local_file
    except Exception as e:
        logger.error(f"Error downloading backup from S3: {str(e)}")
        return None