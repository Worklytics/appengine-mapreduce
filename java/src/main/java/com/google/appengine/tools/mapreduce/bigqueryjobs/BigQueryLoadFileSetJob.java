package com.google.appengine.tools.mapreduce.bigqueryjobs;

import com.google.api.client.extensions.appengine.http.UrlFetchTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.appengine.tools.mapreduce.GcpCredentialOptions;
import com.google.appengine.tools.mapreduce.GcsFilename;
import com.google.appengine.tools.mapreduce.impl.BigQueryConstants;
import com.google.appengine.tools.mapreduce.impl.util.SerializableValue;
import com.google.appengine.tools.pipeline.ImmediateValue;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.Value;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * A pipeline job that manages the lifecycle of a bigquery load {@link Job}. It triggers, polls for
 * status and retries or cleans up the files based on the status of the load job.
 */
@RequiredArgsConstructor
final class BigQueryLoadFileSetJob extends Job1<BigQueryLoadJobReference, Integer> {
  private static final long serialVersionUID = 2L;
  private static final Logger log = Logger.getLogger(BigQueryLoadFileSetJob.class.getName());

  //the name of the BigQuery dataset.
  private final String dataset;
  //name of the BigQuery table to load data.
  private final String tableName;
  //BigQuery project Id.
  private final String projectId;
  //list of the GCS files to load.
  private final List<GcsFilename> fileSet;
  //wrapper around a non-serializable {@link TableSchema} object.
  private final SerializableValue<TableSchema> schema;
  private final GcpCredentialOptions gcpCredentialOptions;

  /**
   * Triggers a bigquery load {@link Job} request and returns the job Id for the same.
   */
  private BigQueryLoadJobReference triggerBigQueryLoadJob() {
    Job job = createJob();
    // Set up Bigquery Insert
    try {

      Insert insert =
        GcpCredentialOptions.getBigqueryClient(gcpCredentialOptions)
          .jobs().insert(projectId, job);
      Job executedJob = insert.execute();
      log.info("Triggered the bigQuery load job for files " + fileSet + " . Job Id = "
          + executedJob.getId());
      return new BigQueryLoadJobReference(projectId, executedJob.getJobReference());
    } catch (IOException e) {
      throw new RuntimeException("Error in triggering BigQuery load job for files " + fileSet, e);
    }
  }

  /**
   * Create a {@link Job} instance for the specified files and bigquery {@link TableSchema} with
   * default settings.
   */
  private Job createJob() {
    Job job = new Job();
    JobConfiguration jobConfig = new JobConfiguration();
    JobConfigurationLoad loadConfig = new JobConfigurationLoad();
    jobConfig.setLoad(loadConfig);
    job.setConfiguration(jobConfig);

    loadConfig.setAllowQuotedNewlines(false);
    loadConfig.setSourceFormat("NEWLINE_DELIMITED_JSON");

    List<String> sources = new ArrayList<String>();
    for (GcsFilename file : fileSet) {
      sources.add(getFileUri(file));
    }

    loadConfig.setSourceUris(sources);

    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(dataset);
    tableRef.setTableId(tableName);
    tableRef.setProjectId(projectId);
    loadConfig.setDestinationTable(tableRef);
    loadConfig.setSchema(schema.getValue());
    return job;
  }

  // TODO : Make it a part of a util class ?
  private String getFileUri(GcsFilename fileName) {
    return "gs://" + fileName.getBucketName() + "/" + fileName.getObjectName();
  }

  @Override
  public Value<BigQueryLoadJobReference> run(Integer retryCount) throws Exception {
    if (retryCount >= BigQueryConstants.MAX_RETRIES) {
      throw new RuntimeException("Unable to load the files into BigQuery = " + fileSet
          + " after max number of retries. Check log for more details.");
    }
    ImmediateValue<BigQueryLoadJobReference> jobTrigger = immediate(triggerBigQueryLoadJob());

    PromisedValue<String> jobStatus = newPromise();
    futureCall(new BigQueryLoadPollJob(jobStatus.getHandle(), gcpCredentialOptions), jobTrigger);

    return futureCall(new RetryLoadOrCleanupJob(dataset, tableName, projectId, fileSet, schema, gcpCredentialOptions),
        jobTrigger, immediate(retryCount), waitFor(jobStatus));
  }
}