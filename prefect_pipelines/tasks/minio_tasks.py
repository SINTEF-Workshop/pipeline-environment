from prefect import task

@task(name='dump_file')
async def dump_file(min, bucket, s3_file, filename):
    print("Adding file")
    min.add_file(bucket, s3_file, filename)
