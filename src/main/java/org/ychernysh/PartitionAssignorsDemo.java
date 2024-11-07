package org.ychernysh;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.Utils;
import org.apache.kafka.common.TopicPartition;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class PartitionAssignorsDemo {

  private final PrintStream out;

  private Properties consumerConfig = new Properties();
  private List<Set<String>> subscriptions = new ArrayList<>();
  private ConsumerThreadGroup group;

  public PartitionAssignorsDemo(PrintStream out) {
    this.out = out;
  }

  public void run(Properties consumerConfig, String subscriptionsPath) {
    init(consumerConfig, subscriptionsPath);

    logConfiguration();

    processRebalanceEvent("Initial", () -> subscriptions.forEach(group::addMember));
    processRebalanceEvent("C1 leaves", () -> group.removeMember(1));
    Set<String> c4Subscription = subscriptions.get(0);
    processRebalanceEvent("C4 joins with subscription " + c4Subscription, () -> group.addMember(c4Subscription));

    group.close();
  }

  private void logConfiguration() {
    String assignor = consumerConfig.getProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "DEFAULT");
    out.println("========== " + assignor + " ==========");
    out.println("Partitions: " + subscriptionsPartitions());
    out.println("Subscriptions:");
    for (int i = 0; i < subscriptions.size(); i++) {
      out.printf("C%d: %s%n", i, subscriptions.get(i));
    }
    out.println();
  }

  private List<TopicPartition> subscriptionsPartitions() {
    List<TopicPartition> subscriptionsPartitions = new ArrayList<>();
    Set<String> allSubscribedTopics = new HashSet<>();
    for (Set<String> subscription: subscriptions) {
      allSubscribedTopics.addAll(subscription);
    }
    try (KafkaConsumer<String, String> tmpConsumer = new KafkaConsumer<>(consumerConfig)) {
      for (String topic: allSubscribedTopics) {
        for (int i = 0; i < tmpConsumer.partitionsFor(topic).size(); i++) {
          subscriptionsPartitions.add(new TopicPartition(topic, i));
        }
      }
    }
    subscriptionsPartitions.sort(new Utils.TopicPartitionComparator());
    return subscriptionsPartitions;
  }

  private void processRebalanceEvent(String name, Runnable event) {
    out.println("===== " + name + " =====");
    event.run();
    awaitAndPrintAssignment();
  }

  private void awaitAndPrintAssignment() {
    while (group.isRebalancing()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    group.getAssignments().forEach((k, v) -> out.printf("%s: %s%n", k, sortPartitions(v)));
  }

  private List<TopicPartition> sortPartitions(Set<TopicPartition> partitions) {
    List<TopicPartition> sorted = new ArrayList<>(partitions);
    sorted.sort(new Utils.TopicPartitionComparator());
    return sorted;
  }

  private void init(Properties consumerConfig, String subscriptionsPath) {
    this.consumerConfig = consumerConfig;
    group = new ConsumerThreadGroup(consumerConfig);
    try {
      Files.lines(Path.of(subscriptionsPath)).forEach(l -> subscriptions.add(new TreeSet<>(Set.of(l.split(",")))));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
