import boto3
from os.path import join

s3 = boto3.resource('s3')

bucket_name = "beofen-genesis"


for filename in ["wikiplaces.tsv"]:
    filepath = join("/tmp", filename)
    with open(filepath, "rb") as f:
        print("uploading " + filepath + " to " + bucket_name + " .  .  .", end="")
        s3.Bucket(bucket_name).put_object(Key=filename, Body=f)
        print("done")
