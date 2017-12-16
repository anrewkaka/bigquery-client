package xyz.lannt.domain.model;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import xyz.lannt.annotation.BigQueryDatetime;
import xyz.lannt.annotation.BigQueryFieldName;
import xyz.lannt.annotation.BigQueryString;
import xyz.lannt.exception.BigQueryClientException;

public interface BigQueryObject {

  default String buildInsertQuery(String datasetName, String tableName) throws IllegalAccessException {
    StringBuilder query = new StringBuilder();
    query.append("INSERT ");
    query.append(datasetName + ".");
    query.append(tableName + " ");
    query.append("(");
    query.append(
        getFields(getClass()).stream()
            .map(e -> getFieldName(e))
            .reduce((v1, v2) -> String.join(", ", v1, v2))
            .orElseThrow(() -> new BigQueryClientException("BigQuery field not found")));
    query.append(")\r");
    query.append("VALUES (");
    query.append(
        getFields(getClass()).stream()
            .map(e -> getFieldValue(e))
            .reduce((v1, v2) -> String.join(", ", v1, v2))
            .orElseThrow(() -> new BigQueryClientException("BigQuery field not found")));
    query.append(")");

    return query.toString();
  }

  default String getFieldName(Field field) {
    if (field == null) {
      return "";
    }

    BigQueryFieldName annotation = field.getAnnotation(BigQueryFieldName.class);
    if (annotation == null) {
      return "";
    }

    if (annotation.value() == null) {
      return "";
    }

    return annotation.value();
  }

  default List<Field> getFields(Class<?> clazz) {
    Class<?> superClass = clazz.getSuperclass();
    if (superClass == null) {
      return Collections.emptyList();
    }

    List<Field> fields = new ArrayList<Field>();
    for (Field field : clazz.getDeclaredFields()) {
      fields.add(field);
    }

    fields.addAll(getFields(superClass));

    return fields;
  }

  default String getFieldValue(Field field) {

    field.setAccessible(true);
    Class<?> targetType = field.getType();
    Object fieldData;
    try {
      fieldData = field.get(this);
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new BigQueryClientException(e);
    }
    if (fieldData == null) {
      return null;
    }

    if (targetType.isPrimitive()) {
      return String.valueOf(fieldData);
    }

    String result = fieldData.toString();
    if (field.getAnnotation(BigQueryString.class) != null
        || field.getAnnotation(BigQueryDatetime.class) != null) {
      result = "'" + result + "'";
    }

    return result;
  }
}
