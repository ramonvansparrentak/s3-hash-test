package nl.ru.s3test;


import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointParams;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class Main {

  static List<String> partHashes = List.of(
      "sMr7YveEjyARSsdNUjA5ClGv8/B7EyNLf7fvgOha2AA=",
      "kvSJiBkFRHxltpMXtNJrHSwwZGdShD5oQaO2laYv8nY=",
      "y44bgegqZU2JpeFSuQHbLiHZZgKK6kakqiFY3Y09Mtk=",
      "ph/2HqhwijI12Gh/6b7ReIvf67cUeZyLwiDCsxzBXzI=",
      "BxzmIZGn83fWg/ClGH2iJDPlzfxz4UichPmkK2KpfVA=",
      "G+uCR/4NBYzxGKVmdtMMNkRsd183L+kWoBz9tQC5ZeI=",
      "marl9uL3CA6LhHm0gx21NuwJEjUcVhzL2PUsTtMs9lk=",
      "xdP1/dv00AdFztBQCo5O005nakcxIfniaKLEixQILdg=",
      "upuMrJszzrTqtw3z0mgmJXok1HbckM2gNRCtqPJ6e/g=",
      "LXAvGdYE+16mz9/318MSsrjQRvIdGbJXniB+J7kxufA="
  );

  public static void main(String[] args) {

    String endpoint = args[0];
    String bucket = args[1];
    System.out.println("Connecting to " + endpoint + " using " + bucket);
    S3Client s3 = S3Client.builder()
        .region(Region.AF_SOUTH_1)
        .serviceConfiguration(S3Configuration.builder()
            .pathStyleAccessEnabled(true)
            .build())
        .endpointOverride(URI.create(args[0]))
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

    uploadParts(s3, bucket, args[2], false);
    uploadParts(s3, bucket, args[2], true);
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

  private static void uploadParts(S3Client s3, String bucket, String dir, boolean skipHashchecksum) {
    String key = "100m" + new Date().toString().replace(' ', '-') + " " + new Random().nextInt(100);
    System.out.println("  ===  ");

    String compositeChecksum = compositeChecksum();

    long start = System.currentTimeMillis();
    CreateMultipartUploadResponse result = s3.createMultipartUpload(b ->
        {
          b = b.bucket(bucket).key(key);
          if (!skipHashchecksum) {
            b.checksumAlgorithm(ChecksumAlgorithm.SHA256);
          }
        }
    );
    System.out.println("Upload id:" + result.uploadId());

    for (int i = 0; i < partHashes.size(); i++ ) {
      long partStart = System.currentTimeMillis();
      uploadPart(s3, bucket, key, result.uploadId(), i, dir, skipHashchecksum);
      System.out.println("Part " + i + " uploaded in " + (System.currentTimeMillis() - partStart) + "ms");
    }

    ListPartsResponse parts = s3.listParts(b -> b
        .bucket(bucket)
        .uploadId(result.uploadId())
        .key(key)
    );
    parts.parts().forEach(System.out::println);

    CompleteMultipartUploadResponse uploadResponse = s3.completeMultipartUpload(b -> {
          b = b.bucket(bucket)
              .uploadId(result.uploadId()).key(key)
              .multipartUpload(c -> c
                  .parts(parts.parts().stream().map(p -> CompletedPart.builder()
                          .partNumber(p.partNumber())
                          .eTag(p.eTag())
                          .checksumSHA256(p.checksumSHA256())
                          .build())
                      .toList()
                  )
              );
          if (!skipHashchecksum) {
              b.checksumSHA256(compositeChecksum);
          }
        }
    );
    System.out.println(System.currentTimeMillis() - start);
    if (skipHashchecksum) {
      System.out.println("Response was ok/not ok: " + uploadResponse.sdkHttpResponse().statusCode() + " for " + key + " without checksum");
    } else {
      System.out.println("Response was ok/not ok: " + uploadResponse.sdkHttpResponse().statusCode() + " for " + key + " "
          + compositeChecksum);
    }
  }

  private static String compositeChecksum() {
    try {
      MessageDigest sha256ChecksumOfChecksums = MessageDigest.getInstance("SHA-256");
      partHashes.forEach(part -> sha256ChecksumOfChecksums.update(Base64.getDecoder().decode(part)));
      return Base64.getEncoder().encodeToString(sha256ChecksumOfChecksums.digest()) + "-" + partHashes.size();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static void uploadPart(S3Client s3, String bucket, String key, String uploadId, int i, String dir,
      boolean skipHashchecksum) {
    s3.uploadPart(b -> {
          b = b.partNumber(i + 1)
              .bucket(bucket)
              .uploadId(uploadId)
              .key(key);
          if (!skipHashchecksum) {
              b.checksumAlgorithm(ChecksumAlgorithm.SHA256)
                .checksumSHA256(partHashes.get(i));
          }
        }
        , RequestBody.fromFile(new File(dir, "part" + i)));
  }
}