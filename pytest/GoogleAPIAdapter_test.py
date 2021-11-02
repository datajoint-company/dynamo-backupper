import io
import os
from dynamo_backupper.GoogleAPIAdapter import GoogleAPIAdapter


def test_upload_test_csv_file_under_folder():
    google_api_adapter = GoogleAPIAdapter()

    folder_id = google_api_adapter.find_folder_id(os.environ.get('FOLDER_NAME'))
    assert(folder_id)

    file_stream = io.StringIO()

    file_stream.write('data,test,')

    google_api_adapter.upload_csv_under_folder('py_test', file_stream, folder_id)

    file_stream.close()