package xyz.lannt.domain.model;

import java.util.stream.Stream;

import xyz.lannt.exception.BigQueryClientException;

public interface BigQueryObjects<E extends BigQueryObject> {

  public Stream<E> stream();

  default String buildInsertQuery(String datasetName, String tableName) {
    StringBuilder query = new StringBuilder();
    query.append("INSERT ");
    query.append(datasetName + ".");
    query.append(tableName + " ");
    query.append("(");
    query.append(this.stream().findFirst().orElseThrow(() -> new BigQueryClientException("List is empty!")).getFieldNames());
    query.append(") ");
    query.append("VALUES ");
    query.append(getAllFieldValues());

    return query.toString();
  }

  default String getAllFieldValues() {
    return this.stream()
        .map(e -> {
          return "(" + e.getFieldValues() + ")";
        })
        .reduce((v1, v2) -> String.join(", ", v1, v2))
        .orElseThrow(() -> new BigQueryClientException("List is empty!"));
  }
}
