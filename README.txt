0. Prepare topics
Create test topics (and stream if mapr) and produce some data to them.
1. Build 
mvn clean package dependency:copy-dependencies
2. Configure 
Choose a partition assignor in consumer.properties.
If mapr, set a default stream in consumer.properties or append the stream name to topics in subscriptions/*. Or write your own subscription file. In these files, each line is a list of subscribed topics for a consumer (N lines mean N consumers)
If apache, set bootstrap.servers in consumer.properties.
3. Run
If apache (choose preferred subscription file):
java -cp "target/partition-assignors-demo-1.0-SNAPSHOT.jar:target/dependency/*" org.ychernysh.DemoMain consumer.properties subscriptions/common1.txt
If mapr (same):
java -cp "target/partition-assignors-demo-1.0-SNAPSHOT.jar:$(mapr clientclasspath --kafka)" org.ychernysh.DemoMain consumer.properties subscriptions/common1.txt
