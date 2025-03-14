package nl.ru.s3test;


import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointParams;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class Main {

  public static void main(String[] args) {

    String endpoint = args[0];
    String bucket = args[1];
    System.out.println("Connecting to " + endpoint + " using " + bucket);
    S3Client s3 = S3Client.builder()
        .region(Region.AF_SOUTH_1)
        .endpointProvider(new S3EndpointProvider() {
          @Override
          public CompletableFuture<Endpoint> resolveEndpoint(S3EndpointParams s3EndpointParams) {
            return CompletableFuture.completedFuture(Endpoint.builder().url(URI.create(endpoint)).build());
          }

          @Override
          public CompletableFuture<Endpoint> resolveEndpoint(
              Consumer<S3EndpointParams.Builder> endpointParamsConsumer) {
            return S3EndpointProvider.super.resolveEndpoint(endpointParamsConsumer);
          }
        })
        .build();
    System.out.println("Buckets");
    s3.listBuckets().buckets().forEach(System.out::println);
    uploadTestFile(s3, bucket, "w6uP8Tcg6K2QR905Rms8iXTlksL6OD1KOWBxTK7wxPI=",
        "Pyx8yumK+B5EwOxBlln1DYt9SMaB5dV/x0fQRh5C3aE=-1", "foobar");
    boolean failed = false;
    try {
      uploadTestFile(s3, bucket, "aauP8Tcg6K2QR905Rms8iXTlksL6OD1KOWBxTK7wxPI=",
          "Pyx8yumK+B5EwOxBlln1DYt9SMaB5dV/x0fQRh5C3aE=-1", "foobar");
      System.out.println("ERROR: Incorrect part hash was excepted.");
      failed = true;
    } catch (S3Exception e) {
      if ("The provided 'x-amz-checksum' header does not match what was computed.".equals(
          e.awsErrorDetails().errorMessage())) {
        System.out.println("Incorrect part checksum failed as expected.");
      } else {
        System.out.println("ERROR: " + e.getMessage());
        failed = true;
      }
    }
    try {
      uploadTestFile(s3, bucket, "w6uP8Tcg6K2QR905Rms8iXTlksL6OD1KOWBxTK7wxPI=",
          "aax8yumK+B5EwOxBlln1DYt9SMaB5dV/x0fQRh5C3aE=-1", "foobar");
      System.out.println("ERROR: Incorrect object hash was excepted.");
      failed = true;
    } catch (S3Exception e) {
      if ("The provided 'x-amz-checksum' header does not match what was computed.".equals(
          e.awsErrorDetails().errorMessage())) {
        System.out.println("Incorrect object checksum failed as expected.");
      } else {
        System.out.println("ERROR: " + e.getMessage());
        failed = true;
      }
    }
    if (failed) {
      System.exit(-2);
    }
  }

  private static void uploadTestFile(S3Client s3, String bucket, String partHash, String objectHash, String data) {
    String key = bucket + "/testpupload" + new Random().nextInt();
    System.out.println("  ===  ");
    System.out.println("Uploading to " + bucket + "/" + key);
    CreateMultipartUploadResponse result = s3.createMultipartUpload(b -> b
        .bucket(bucket)
        .key(key)
        .checksumAlgorithm(ChecksumAlgorithm.SHA256)
    );
    System.out.println("Upload id:" + result.uploadId());

    s3.uploadPart(b -> b
            .partNumber(1)
            .bucket(bucket)
            .uploadId(result.uploadId())
            .key(key)
            .checksumAlgorithm(ChecksumAlgorithm.SHA256)
            .checksumSHA256(partHash)
        , RequestBody.fromString(data));

    ListPartsResponse parts = s3.listParts(b -> b
        .bucket(bucket)
        .uploadId(result.uploadId())
        .key(key)
    );
    parts.parts().forEach(System.out::println);

    CompleteMultipartUploadResponse uploadResponse = s3.completeMultipartUpload(b -> b
        .bucket(bucket)
        .uploadId(result.uploadId()).key(key)
        .multipartUpload(c -> c
            .parts(parts.parts().stream().map(p -> CompletedPart.builder()
                    .partNumber(p.partNumber())
                    .eTag(p.eTag())
                    .checksumSHA256(p.checksumSHA256())
                    .build())
                .toList()
            )
        )
        .checksumSHA256(objectHash)
    );
    System.out.println("Response was ok/not ok: " + uploadResponse.sdkHttpResponse().statusCode()
        + uploadResponse.sdkHttpResponse().statusText());
    System.out.println("Response checksum: " + uploadResponse.checksumSHA256());
  }
}