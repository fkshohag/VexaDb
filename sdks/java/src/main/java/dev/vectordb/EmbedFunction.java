package dev.vectordb;

import java.util.List;

@FunctionalInterface
public interface EmbedFunction {
  List<Double> embed(String text) throws Exception;
}
