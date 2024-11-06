package org.ychernysh;

public class DemoMain {
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("USAGE: DemoMain <consumer_properties> <subscriptions>");
      System.exit(1);
    }
    String consumerPropertiesFile = args[0];
    String subscriptionsFile = args[1];

    PartitionAssignorsDemo partitionAssignorsDemo = new PartitionAssignorsDemo(System.out);
    partitionAssignorsDemo.run(consumerPropertiesFile, subscriptionsFile);
  }
}
