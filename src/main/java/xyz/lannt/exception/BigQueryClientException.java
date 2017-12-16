package xyz.lannt.exception;

public class BigQueryClientException extends RuntimeException {

  private static final long serialVersionUID = 4981731227163668699L;

  public BigQueryClientException(String message) {
    super(message);
  }

  public BigQueryClientException(Throwable cause) {
    super(cause);
  }

  public BigQueryClientException(String message, Throwable cause) {
    super(message, cause);
  }
}
