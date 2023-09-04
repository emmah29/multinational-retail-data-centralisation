import boto3
# s3_client = boto3.client('s3')
# # response = s3_client.upload_file(file_name, bucket, object_name)
# response = s3_client.upload_file('Screenshot1.png', 'testbucketemmahumphreys', 'screenshot1.png')

#Now, we attempt to view the content(s) of the bucket:

s3 = boto3.resource('s3')
my_bucket = s3.Bucket('data-handling-public')
for file in my_bucket.objects.all():
    print(file.key)

# Once you have viewed the contents, you can download the files:
# s3.download_file('BUCKET_NAME', 'OBJECT_NAME', 'FILE_NAME')
#s3 = boto3.client('s3')
#s3.download_file('testbucketemmahumphreys', 'screenshot1.png', 'screenshot1fromAWS.png')

# s3://data-handling-public/products.csv
