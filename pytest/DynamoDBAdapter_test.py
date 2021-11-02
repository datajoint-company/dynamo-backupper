import os
from dynamo_backupper.DynamoDBAdapter import DynamoDBAdapter

def test_scan():
    # Create the adapter instance
    db_adapter= DynamoDBAdapter(os.environ.get('REGION'))

    # Try to fetch data, if it blows up let it so the stack get logged into logs
    data = db_adapter.scan(os.environ.get('TABLE_NAME'))

    assert(data)