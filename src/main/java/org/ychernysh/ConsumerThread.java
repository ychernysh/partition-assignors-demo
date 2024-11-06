package org.ychernysh;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.Utils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class ConsumerThread extends Thread {

  private final KafkaConsumer<String, String> consumer;
  private Set<TopicPartition> assignment = new HashSet<>();
  private boolean isClosing = false;

  public ConsumerThread(String name, KafkaConsumer<String, String> consumer, Collection<String> subscription) {
    super(name);
    this.consumer = consumer;
    consumer.subscribe(subscription, new RebalanceListener());
  }

  @Override
  public void run() {
    while (!isClosing) {
      try {
        consumer.poll(Duration.ofSeconds(1));
      } catch (WakeupException e) {
        if (!isClosing) {
          throw new RuntimeException("Unexpected wake up");
        }
      }
    }
    consumer.close();
  }

  public void shutdown() {
    isClosing = true;
    consumer.wakeup();
  }

  public Set<TopicPartition> assignment() {
    return assignment;
  }

  private final class RebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
      assignment = consumer.assignment();
    }
  }

}
