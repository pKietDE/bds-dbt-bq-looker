import datetime

class GCStorage:
    def __init__(self,storage_client):
        self.client = storage_client

    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")

    def upload_files_to_gcs(self, file_name):
        try:
            # Tạo tên blob (tên file trên GCS) với ngày tháng năm
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
            destination_blob_name = f"data_bds_{current_date}.json"

            # Upload file lên GCS
            self.upload_blob("bkt-batdongsan", file_name, destination_blob_name)

        except Exception as e:
            print(f"Error uploading file to GCS: {e}")

    
