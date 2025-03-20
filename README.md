# S3 Hash test

Test hash checksum verification of S3 multipart upload.

## Description

The program will attempt 3 multipart uploads (a small 6 byte file) to the specified S3 service.

 1. The first upload has correct part checksum and correct composite object checksum and should thus succeed
 2. The second upload has **incorrect part checksum** and the upload should fail
 3. The third upload has correct part checksum but **incorrect composite object** checksum and the upload should fail

## Build and run

Build the docker image:

```
docker build -t s3-hash-test .
```

Test some remote S3 service:

```
docker run -e AWS_ACCESS_KEY_ID=mykeyid -e AWS_SECRET_ACCESS_KEY=mysecret s3-hash-test https://s3.example.com mybucket /data
```

Test a locally running S3 service, on port 9000:

```
docker run -e AWS_ACCESS_KEY_ID=mykeyid -e AWS_SECRET_ACCESS_KEY=mysecret --add-host host.docker.internal:host-gateway s3-hash-test http://host.docker.internal:9000 mybucket /data
```
