package org.ychernysh;

import java.io.FileNotFoundException;
import java.io.PrintStream;

public class DemoAll {
  public static void main(String[] args) {
    run("configs/consumer_configs/consumer-range.properties", "configs/subscriptions/common-subscriptions1.txt", "results/range-common.txt");
    run("configs/consumer_configs/consumer-round-robin.properties", "configs/subscriptions/common-subscriptions1.txt", "results/round-robin-common.txt");
    run("configs/consumer_configs/consumer-sticky.properties", "configs/subscriptions/common-subscriptions1.txt", "results/sticky-common.txt");
    run("configs/consumer_configs/consumer-cooperative-sticky.properties", "configs/subscriptions/common-subscriptions1.txt", "results/cooperative-sticky-common.txt");

    run("configs/consumer_configs/consumer-range.properties", "configs/subscriptions/different-subscriptions1.txt", "results/range-different.txt");
    run("configs/consumer_configs/consumer-round-robin.properties", "configs/subscriptions/different-subscriptions1.txt", "results/round-robin-different.txt");
    run("configs/consumer_configs/consumer-sticky.properties", "configs/subscriptions/different-subscriptions1.txt", "results/sticky-different.txt");
    run("configs/consumer_configs/consumer-cooperative-sticky.properties", "configs/subscriptions/different-subscriptions1.txt", "results/cooperative-sticky-different.txt");
  }
  private static void run(String consumerConfig, String subscriptions1, String outputFile) {
    try {
      new PartitionAssignorsDemo(new PrintStream(outputFile)).run(consumerConfig, subscriptions1);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
