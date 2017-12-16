package xyz.lannt.client;

import java.io.IOException;
import java.util.List;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableRow;

import xyz.lannt.domain.model.BigQueryObject;
import xyz.lannt.exception.BigQueryClientException;
import xyz.lannt.property.GoogleProperty;

public class BigQueryClient {

  private GoogleProperty property;
  private Bigquery bigquery;

  public BigQueryClient(GoogleProperty property) {
    this.property = property;
    this.bigquery = createAuthorizedClient();
  }

  private Bigquery createAuthorizedClient() {
    // Create the credential
    HttpTransport transport = new NetHttpTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential = null;

    try {
      credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
    } catch (IOException ex) {
      throw new BigQueryClientException(ex.getMessage(), ex);
    }

    if (credential.createScopedRequired()) {
      credential = credential.createScoped(BigqueryScopes.all());
    }

    return new Bigquery.Builder(transport, jsonFactory, credential).setApplicationName("Bigquery Samples").build();
  }

  public List<TableRow> select(String querySql) {
    return this.executeQuery(querySql).getRows();
  }

  public void insert(String datasetName, String tableName, BigQueryObject data) {
    try {
      executeQuery(data.buildInsertQuery(datasetName, tableName));
    } catch (IllegalAccessException e) {
      throw new BigQueryClientException(e);
    }
  }

  private GetQueryResultsResponse executeQuery(String sqlQuery) {
    QueryResponse query;
    GetQueryResultsResponse queryResult;
    try {
      query = bigquery.jobs().query(this.property.getProjectId(), new QueryRequest().setQuery(sqlQuery)).execute();
      queryResult = bigquery.jobs()
          .getQueryResults(query.getJobReference().getProjectId(), query.getJobReference().getJobId()).execute();
    } catch (IOException e) {
      throw new BigQueryClientException(e.getMessage(), e);
    }

    return queryResult;
  }
}
