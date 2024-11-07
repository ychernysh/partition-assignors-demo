package org.ychernysh;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.consumer.StickyAssignor;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

public class DemoAll {

  private static final Properties consumerConfig = new Properties();

  public static void main(String[] args) throws IOException {
    consumerConfig.load(new FileReader("consumer.properties"));

    run(RangeAssignor.class, "subscriptions/common1.txt", "results/range-common.txt");
    run(RoundRobinAssignor.class, "subscriptions/common1.txt", "results/round-robin-common.txt");
    run(StickyAssignor.class, "subscriptions/common1.txt", "results/sticky-common.txt");
    run(CooperativeStickyAssignor.class, "subscriptions/common1.txt", "results/cooperative-sticky-common.txt");

    run(RangeAssignor.class, "subscriptions/different1.txt", "results/range-different.txt");
    run(RoundRobinAssignor.class, "subscriptions/different1.txt", "results/round-robin-different.txt");
    run(StickyAssignor.class, "subscriptions/different1.txt", "results/sticky-different.txt");
    run(CooperativeStickyAssignor.class, "subscriptions/different1.txt", "results/cooperative-sticky-different.txt");
  }
  private static void run(Class<? extends ConsumerPartitionAssignor> assignorClass, String subscriptionsPath, String outputFile) {
    try {
      consumerConfig.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignorClass.getName());
      new PartitionAssignorsDemo(new PrintStream(outputFile)).run(consumerConfig, subscriptionsPath);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
