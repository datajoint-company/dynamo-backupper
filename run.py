import sys
import io
import argparse
import pandas as pd
from datetime import date
from dynamo_backupper.DynamoDBAdapter import DynamoDBAdapter
from dynamo_backupper.GoogleAPIAdapter import GoogleAPIAdapter
from dynamo_backupper.S3Adapter import S3Adapter

def main(args):
    # Create the adapter instance
    db_adapter= DynamoDBAdapter(args.region)

    # Try to fetch data, if it blows up let it so the stack get logged into logs
    data = db_adapter.scan(args.table) 

    if args.save_to == "google":
        google_api_adapter = GoogleAPIAdapter()
        pd.DataFrame(data['Items']).to_csv('temp_dump.csv')
        folder_id = google_api_adapter.find_folder_id(args.gdfolder)
        google_api_adapter.upload_csv_under_folder(str(date.today()), 'temp_dump.csv', folder_id)
    elif args.save_to == "s3":
        s3_adapter = S3Adapter(args.region, args.bucket_name)
        csv_file_stream = io.StringIO()
        pd.DataFrame(data['Items']).to_csv(csv_file_stream)
        print(csv_file_stream.getvalue())
        s3_adapter.upload(csv_file_stream, key_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save_to", "-to", required=True, help="[s3, google]")
    parser.add_argument("--region", "-reg", required=True, help="AWS region")
    parser.add_argument("--table", "-t", required=True, help="Dynamo table name")
    # For Google Drive
    parser.add_argument("--gdfolder", "-gdf", help="Google driver folder")
    # For S3
    parser.add_argument("--bucket_name", "-b", help="S3 bucket name")
    parser.add_argument("--key_name", "-k", help="S3 key name")
    args = parser.parse_args()
    main(args)