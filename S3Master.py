import boto3
from LineNotification import LineNotification

class S3Master:
    @classmethod
    def initialize(cls):
        cls.__bucket_name = 'bot-trade-log'

    @classmethod
    def get_file_list(cls):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(cls.__bucket_name)
        for object in bucket.objects.all():
            print(object)

    @classmethod
    def save_file(cls, file_name):
        try:
            s3 = boto3.resource('s3')
            s3.Object(cls.__bucket_name, file_name).upload_file(file_name)
            return 0
        except Exception as e:
            print('s3 bot trade log save file error!={}'.format(e))
            LineNotification.send_error('s3 bot trade log save file error!={}'.format(e))
            return -1

    @classmethod
    def remove_file(cls, file_name):
        files = cls.get_file_list()
        s3_client = boto3.client('s3')
        if files is not None:
            for f in files:
                if file_name in str(f.key):
                    s3_client.delete_object(Bucket=cls.__bucket_name, Key=f.key)

if __name__ == '__main__':
    pass
