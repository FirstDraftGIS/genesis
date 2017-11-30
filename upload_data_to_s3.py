import boto3
from os.path import join

s3 = boto3.client('s3')

bucket_name = "beofen-genesis"


for filename in ["wikiplaces.tsv"]:
    filepath = join("/tmp", filename)
    print("uploading " + filepath + " to " + bucket_name + " .  .  .", end="")
    s3.upload_file(filepath, bucket_name, filename)
    print("done")
