package org.ychernysh;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.Utils;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

public class PartitionAssignorsDemo {

  private final PrintStream out;

  private Properties consumerConfig = new Properties();
  private List<Set<String>> subscriptions = new ArrayList<>();
  private ConsumerThreadGroup group;

  public PartitionAssignorsDemo(PrintStream out) {
    this.out = out;
  }

  public void run(String consumerPropertiesPath, String subscriptionsPath) {
    init(consumerPropertiesPath, subscriptionsPath);

    logConfiguration();

    processRebalanceEvent("Initial", () -> subscriptions.forEach(group::addMember));
    processRebalanceEvent("C1 leaves", () -> group.removeMember(1));
    Set<String> c4Subscription = subscriptions.get(0);
    processRebalanceEvent("C4 joins with subscription " + c4Subscription, () -> group.addMember(c4Subscription));

    group.close();
  }

  private void logConfiguration() {
    String assignor = consumerConfig.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG).toString();
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
    Properties adminConfig = new Properties();
    adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, consumerConfig.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    try (Admin admin = Admin.create(adminConfig)) {
      Map<String, TopicDescription> topicDescriptions =
              admin.describeTopics(TopicCollection.ofTopicNames(allSubscribedTopics)).allTopicNames().get();
      for (String topic: topicDescriptions.keySet()) {
        for (int p = 0; p < topicDescriptions.get(topic).partitions().size(); p++) {
          subscriptionsPartitions.add(new TopicPartition(topic, p));
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
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

  private void init(String consumerPropertiesPath, String subscriptionsPath) {
    try {
      consumerConfig.load(new FileReader(consumerPropertiesPath));
      Files.lines(Path.of(subscriptionsPath)).forEach(l -> subscriptions.add(new TreeSet<>(Set.of(l.split(",")))));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    group = new ConsumerThreadGroup(consumerConfig);
  }

}
