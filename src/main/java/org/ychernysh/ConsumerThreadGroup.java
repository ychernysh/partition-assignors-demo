package org.ychernysh;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

public class ConsumerThreadGroup implements AutoCloseable {

  private final Properties consumerConfig;
  private final List<ConsumerThread> consumerThreads = new ArrayList<>();
  private int memberCounter = 0;
  private long lastRebalanceEventMs = -1;

  public ConsumerThreadGroup(Properties consumerConfig) {
    this.consumerConfig = consumerConfig;
    if (!consumerConfig.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
      consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    }
  }

  public void addMember(Collection<String> subscription) {
    ConsumerThread consumerThread = new ConsumerThread("C" + memberCounter++, new KafkaConsumer<>(consumerConfig), subscription);
    consumerThreads.add(consumerThread);
    consumerThread.start();
    lastRebalanceEventMs = System.currentTimeMillis();
  }

  public void removeMember(int index) {
    consumerThreads.remove(index).shutdown();
    lastRebalanceEventMs = System.currentTimeMillis();
  }

  public Map<String, Set<TopicPartition>> getAssignments() {
    return new TreeMap<>(
            consumerThreads.stream().collect(Collectors.toMap(ConsumerThread::getName, ConsumerThread::assignment)));
  }

  public boolean isRebalancing() {
    // TODO: find a better way to do it
    return System.currentTimeMillis() - lastRebalanceEventMs < 15 * 1000;
  }

  @Override
  public void close() {
    for (ConsumerThread consumerThread: consumerThreads) {
      consumerThread.shutdown();
    }
    consumerThreads.clear();
  }

}
