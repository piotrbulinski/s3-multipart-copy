s3-multipart-copy
=================

S3 Multipart Copy allows to copy files bigger than 5GB between buckets (using boto).

Installation:

    git clone git@github.com:piotrbulinski/s3-multipart-copy.git
    cd s3-multipart-copy
    virtualenv venv
    . venv/bin/activate
    pip install -r requirements.txt

Usage:

    . venv/bin/activate; python s3cp.py src_bucket/path/filename dst_bucket/[path/filename]
