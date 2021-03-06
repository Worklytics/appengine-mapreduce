package com.google.appengine.tools.mapreduce;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.SneakyThrows;
import lombok.ToString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Optional;

public interface GcpCredentialOptions {

  String getServiceAccountKey();

  /**
   * @return Credentials to use when accessing GCS bucket
   * @throws IOException if can't parse ServiceAccountCredentials from serviceAccountKey
   */
  @JsonIgnore
  @SneakyThrows //having this explicitly throw checked IOException is annoying, not especially useful
  default Optional<ServiceAccountCredentials> getServiceAccountCredentials() {
    if (getServiceAccountKey() == null) {
      return Optional.empty();
    } else {
      String jsonKey = new String(Base64.getDecoder().decode(getServiceAccountKey().trim().getBytes()));
      return Optional.of(ServiceAccountCredentials.fromStream(new ByteArrayInputStream(jsonKey.getBytes())));
    }
  }

  //helper util; consider moving to GCPUtils class, or something ...
  static Storage getStorageClient(GcpCredentialOptions gcpCredentialOptions) {
    Credentials credentials =
      gcpCredentialOptions.getServiceAccountCredentials().map(c -> (Credentials) c)
        .orElseGet(() -> StorageOptions.getDefaultInstance().getCredentials());

    return StorageOptions.newBuilder()
      .setCredentials(credentials)
      .build().getService();

  }
}
