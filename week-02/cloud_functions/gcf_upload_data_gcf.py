"""
requirements.txt
google-auth
google-cloud-storage==1.42.3
"""
from google.cloud import storage

def load_csv(self):
    """Loads CSV file from public Github repository in the Google Cloud Storage bucket
    Args:
    Returns:
        Google Storage bucket name
    """
    client = storage.Client()

    bucket = client.get_bucket('wizeline-bootcamp-330020')
    blob = bucket.blob('remote_file.txt')
    blob.upload_from_string('this is test content!')
        
    return 'Successful upload!'