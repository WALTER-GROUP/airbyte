/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.ibmi;

import static io.airbyte.cdk.db.jdbc.DateTimeConverter.putJavaSQLTime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.cdk.db.jdbc.DateTimeConverter;
import io.airbyte.cdk.db.jdbc.JdbcSourceOperations;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.JsonSchemaType;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Db2IBMiSourceOperations extends JdbcSourceOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(Db2IBMiSourceOperations.class);
  private static final List<String> DB2_UNIQUE_NUMBER_TYPES = List.of("DECFLOAT");

  @Override
  public JsonNode rowToJson(final ResultSet queryContext) throws SQLException {
    final int columnCount = queryContext.getMetaData().getColumnCount();
    final ObjectNode jsonNode = (ObjectNode) Jsons.jsonNode(Collections.emptyMap());

    for (int i = 1; i <= columnCount; i++) {
      setFields(queryContext, i, jsonNode);
    }

    return jsonNode;
  }

  /* Helpers */

  private void setFields(final ResultSet queryContext, final int index, final ObjectNode jsonNode) throws SQLException {
    try {
      copyToJsonField(queryContext, index, jsonNode);
    } catch (final SQLException e) {
      if (DB2_UNIQUE_NUMBER_TYPES.contains(queryContext.getMetaData().getColumnTypeName(index))) {
        db2UniqueTypes(queryContext, index, jsonNode);
      } else {
        throw new SQLException("Failed to process column at index " + index, e);
      }
    } catch (NullPointerException e) {
      // AS400 driver may throw NullPointerException when accessing NULL DATE columns
      if (!queryContext.getMetaData().getColumnTypeName(index).equals("DATE") && queryContext.getDate(index) != null){
        throw e;
      }
    }
  }

  private void db2UniqueTypes(final ResultSet resultSet, final int index, final ObjectNode jsonNode) throws SQLException {
    final String columnType = resultSet.getMetaData().getColumnTypeName(index);
    final String columnName = resultSet.getMetaData().getColumnName(index);
    if (DB2_UNIQUE_NUMBER_TYPES.contains(columnType)) {
      putDecfloat(jsonNode, columnName, resultSet, index);
    }
  }

  private void putDecfloat(final ObjectNode node,
                           final String columnName,
                           final ResultSet resultSet,
                           final int index) {
    try {
      final double value = resultSet.getDouble(index);
      if (resultSet.wasNull()) {
        node.put(columnName, (Double) null);
      } else {
        node.put(columnName, value);
      }
    } catch (final SQLException e) {
      node.put(columnName, (Double) null);
    }
  }

  @Override
  public void copyToJsonField(final ResultSet resultSet, final int colIndex, final ObjectNode json) throws SQLException {
    final int columnTypeInt = resultSet.getMetaData().getColumnType(colIndex);
    final String columnName = resultSet.getMetaData().getColumnName(colIndex);
    final JDBCType columnType = JDBCType.valueOf(columnTypeInt);

    // Handle numeric types with explicit wasNull() checks to fix AS400 JDBC driver quirk
    // where primitive getters return 0 instead of throwing for NULL values
    switch (columnType) {
      case TINYINT, SMALLINT -> putNumericWithNullCheck(json, columnName, resultSet.getShort(colIndex), resultSet.wasNull());
      case INTEGER -> putNumericWithNullCheck(json, columnName, resultSet.getInt(colIndex), resultSet.wasNull());
      case BIGINT -> putNumericWithNullCheck(json, columnName, resultSet.getLong(colIndex), resultSet.wasNull());
      case FLOAT, DOUBLE -> putNumericWithNullCheck(json, columnName, resultSet.getDouble(colIndex), resultSet.wasNull());
      case REAL -> putNumericWithNullCheck(json, columnName, resultSet.getFloat(colIndex), resultSet.wasNull());
      case NUMERIC, DECIMAL -> putNumericWithNullCheck(json, columnName, resultSet.getBigDecimal(colIndex), resultSet.wasNull());
      default -> super.copyToJsonField(resultSet, colIndex, json);
    }
  }

  private void putNumericWithNullCheck(final ObjectNode json, final String columnName, final Number value, final boolean wasNull) {
    if (wasNull) {
      json.putNull(columnName);
    } else {
        switch (value) {
            case Short i -> json.put(columnName, i);
            case Integer i -> json.put(columnName, i);
            case Long l -> json.put(columnName, l);
            case Float v -> json.put(columnName, v);
            case Double v -> json.put(columnName, v);
            case BigDecimal bigDecimal -> json.put(columnName, bigDecimal);
            default -> json.put(columnName, value.toString());
        }
    }
  }

  @Override
  protected void putTime(final ObjectNode node,
                         final String columnName,
                         final ResultSet resultSet,
                         final int index)
      throws SQLException {
    putJavaSQLTime(node, columnName, resultSet, index);
  }

  @Override
  protected void putTimestamp(final ObjectNode node, final String columnName, final ResultSet resultSet, final int index) throws SQLException {
    final Timestamp timestamp = resultSet.getTimestamp(index);
    node.put(columnName, timestamp != null ? DateTimeConverter.convertToTimestamp(timestamp) : null);
  }

  @Override
  protected void setTimestamp(final PreparedStatement preparedStatement, final int parameterIndex, final String value) throws SQLException {
    final LocalDateTime date = LocalDateTime.parse(value);
    preparedStatement.setTimestamp(parameterIndex, Timestamp.valueOf(date));
  }

  @Override
  protected void setDate(final PreparedStatement preparedStatement, final int parameterIndex, final String value) throws SQLException {
    final LocalDate date = LocalDate.parse(value);
  preparedStatement.setDate(parameterIndex, Date.valueOf(date));
  }

  @Override
  public JsonSchemaType getAirbyteType(final JDBCType jdbcType) {
      return switch (jdbcType) {
          case SMALLINT, INTEGER, BIGINT -> JsonSchemaType.INTEGER;
          case DOUBLE, DECIMAL, NUMERIC, REAL -> JsonSchemaType.NUMBER;
          case DATE -> JsonSchemaType.STRING_DATE;
          case BLOB, BINARY, VARBINARY -> JsonSchemaType.STRING_BASE_64;
          case TIME -> JsonSchemaType.STRING_TIME_WITHOUT_TIMEZONE;
          case TIMESTAMP -> JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE;
          default -> super.getAirbyteType(jdbcType);
      };
  }

}
