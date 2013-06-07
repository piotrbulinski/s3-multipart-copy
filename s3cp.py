import os
import sys
import argparse
import boto

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Multi-part copy for S3')
    parser.add_argument('src', help='source file, eg.: bucket1/filename')
    parser.add_argument('dst', help='destination, eg.: bucket2/filename')
    parser.add_argument('--aws-access-key', help='AWS access key')
    parser.add_argument('--aws-secret-key', help='AWS secret key')
    args = parser.parse_args()

    aws_access_key = args.aws_access_key or os.getenv('AWS_ACCESS_KEY')
    aws_secret_key = args.aws_secret_key or os.getenv('AWS_SECRET_KEY')

    if aws_access_key is None:
        sys.exit('You need to provide AWS access key')

    if aws_secret_key is None:
        sys.exit('You need to provide AWS secret key')

    if '/' not in args.src or '/' not in args.dst:
        sys.exit('wrong location format')

    src_bucket_name, src_filename = args.src.split('/', 1)
    dst_bucket_name, dst_filename = args.dst.split('/', 1)

    if not dst_filename:
        dst_filename = src_filename

    s3 = boto.connect_s3(aws_access_key, aws_secret_key)

    src_bucket = s3.get_bucket(src_bucket_name)
    key = src_bucket.get_key(src_filename)

    if key is None:
        sys.exit(src_filename + ' not found in ' + src_bucket_name)

    print 'Found file ' + src_filename + ' in ' + src_bucket_name
    print 'File size: ' + str(key.size)

    part_size = 512 * 1024 * 1024

    if key.size < 5 * 1024 * 1024:
        sys.exit('Cannot use multi-part copy for files smaller than 5MB')

    if key.size > part_size:
        full_parts = int(key.size / part_size)
    else:
        full_parts = 1
        part_size = key.size

    total_parts = full_parts

    if full_parts * part_size < key.size:
        total_parts += 1

    dst_bucket = s3.get_bucket(dst_bucket_name)
    mp = dst_bucket.initiate_multipart_upload(dst_filename)

    for i in xrange(full_parts):
        part_id = i + 1
        part_range_from = i * part_size
        part_range_to = i * part_size + part_size - 1
        print "uploading part {part_id}/{parts} (bytes {part_range_from}-{part_range_to})".format(
            part_id=part_id, parts=total_parts, part_range_from=part_range_from, part_range_to=part_range_to)
        mp.copy_part_from_key(src_bucket_name, dst_filename, part_id, part_range_from, part_range_to)

    if total_parts > full_parts:
        part_id = total_parts
        part_range_from = full_parts * part_size
        part_range_to = key.size - 1
        print "uploading part {part_id}/{parts} (bytes {part_range_from}-{part_range_to})".format(
            part_id=part_id, parts=total_parts, part_range_from=part_range_from, part_range_to=part_range_to)
        mp.copy_part_from_key(src_bucket_name, dst_filename, part_id, part_range_from, part_range_to)

    print 'completeing download...'
    mp.complete_upload()
    print 'done'
