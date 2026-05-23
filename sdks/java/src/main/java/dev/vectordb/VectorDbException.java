package dev.vectordb;

public class VectorDbException extends RuntimeException {
  private final int statusCode;

  public VectorDbException(int statusCode, String message) {
    super("vectordb: " + statusCode + " " + message);
    this.statusCode = statusCode;
  }

  public int getStatusCode() {
    return statusCode;
  }
}
