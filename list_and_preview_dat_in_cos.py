import boto3
import io
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

# IBM COS Configuration
##bucket_name = os.getenv("BUCKET_NAME",'raw-data-bucket')
##endpoint = os.getenv("COS_ENDPOINT",'https://s3.jp-tok.cloud-object-storage.appdomain.cloud')
##access_key = os.getenv("COS_ACCESS_KEY",'9d05516331d44f38ba2c230ff036e8a0')
##secret_key = os.getenv("COS_SECRET_KEY",'423c929fb85ff48809011f3264e10e7a87240150a5896b01')

bucket_name = os.getenv("BUCKET_NAME")
endpoint = os.getenv("COS_ENDPOINT")
access_key = os.getenv("COS_ACCESS_KEY")
secret_key = os.getenv("COS_SECRET_KEY")


# Create S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url=endpoint  # Use IBM COS endpoint here
)

def human_readable_size(size_in_bytes):
    """
    Convert size in bytes to a human-readable format (e.g., KB, MB, GB).
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024
    return f"{size_in_bytes:.2f} TB"

def list_files_in_bucket(bucket_name):
    """List all files in the given bucket."""
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        files = response.get('Contents', [])
        return files
        # return [file['Key'] for file in files]
    except Exception as e:
        print(f"Error listing files: {e}")
        return []

# def read_and_inspect_dat_file(bucket_name, file_key):
def read_top_rows_from_dat_file(bucket_name, file_key, delimiter='|', nrows=10):
    """Read and inspect a .dat file from the bucket."""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = response['Body']
        df = pd.read_csv(io.BytesIO(body.read(1024 * 1024)),  # Read a chunk of bytes (~1MB)
                         delimiter=delimiter, header=None, nrows=nrows)
        print(f"\n📂 File: {file_key}")
        print("🔍 Top Rows:")
        print(df.to_string(index=False, header=False, max_rows=nrows))
        return df
        # print(file_content[:500].decode('utf-8', errors='ignore'))  # Print the first 500 bytes
    except Exception as e:
        print(f"Error reading file {file_key}: {e}")
        return None

def main():
    """Main function to list and inspect .dat files."""
    print(f"📦 Listing files in bucket: '{bucket_name}'")
    files = list_files_in_bucket(bucket_name)

    if not files:
        print("No files found in the bucket.")
        return

    dat_files = [file for file in files if file['Key'].endswith('.dat')]

    if not dat_files:
        print("\nNo .dat files found in the bucket.")
        return

    total_size = sum(file['Size'] for file in dat_files)

    print(f"\n✅ Found {len(dat_files)} .dat file(s):")

    for file in dat_files:
        print(f"   - {file['Key']} (Size: {human_readable_size(file['Size'])})")

    print(f"\n📊 Total size of .dat files: {human_readable_size(total_size)}")


    print("\n📄 Inspecting .dat files:\n")
    for file in dat_files:
        read_top_rows_from_dat_file(bucket_name, file['Key'], nrows=5)

if __name__ == "__main__":
    main()
