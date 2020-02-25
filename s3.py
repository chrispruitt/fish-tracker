import boto3

bucket_name = 'fish-tracker'
detection_data_path = '/detection_data'

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
for obj in bucket.objects.all():
    print(obj.key, obj.last_modified)


client = boto3.client('s3')
response = client.list_objects(Bucket=bucket_name)
for content in response['Contents']:
    obj_dict = client.get_object(Bucket=bucket_name, Key=content['Key'])
    print(content['Key'], obj_dict['LastModified'])

# def myFunc():
#     print('hi')
#     return None
#
# x = myFunc()
#
# print(x)