class UploadToGCSFailedError(Exception):
    def __init__(self, message="Error occurred while uploading to GCS"):
        self.message = message
        super().__init__(self.message)

class UploadToBigQueryFailedError(Exception):
    def __init__(self, message="Error occurred while uploading to cleaned data to big query"):
        self.message = message
        super().__init__(self.message)