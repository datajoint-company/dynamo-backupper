import sys
import io
import os
import pandas as pd
from datetime import date
from dynamo_backupper.DynamoDBAdapter import DynamoDBAdapter
from dynamo_backupper.GoogleAPIAdapter import GoogleAPIAdapter

# Args are aws-region, dynamodb table name, and google drive folder to upload to
def main(argv):
    # Create the adapter instance
    db_adapter= DynamoDBAdapter(os.environ.get('REGION'))

    # Try to fetch data, if it blows up let it so the stack get logged into logs
    data = db_adapter.scan(os.environ.get('TABLE_NAME'))

    # Create Stream 
    file_stream = io.StringIO()
    pd.DataFrame(data['Items']).to_csv(file_stream)

    google_api_adapter = GoogleAPIAdapter()

    folder_id = google_api_adapter.find_folder_id(os.environ.get('FOLDER_NAME'))
    google_api_adapter.upload_csv_under_folder(str(date.today()), file_stream, folder_id)
    file_stream.close()

if __name__ == "__main__":
   main(sys.argv[1:])