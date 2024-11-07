package org.ychernysh;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class DemoMain {
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("USAGE: DemoMain <consumer_properties> <subscriptions>");
      System.exit(1);
    }
    Properties consumerConfig = new Properties();
    consumerConfig.load(new FileReader(args[0]));
    String subscriptionsFile = args[1];

    PartitionAssignorsDemo partitionAssignorsDemo = new PartitionAssignorsDemo(System.out);
    partitionAssignorsDemo.run(consumerConfig, subscriptionsFile);
  }
}
