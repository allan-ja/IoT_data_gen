package org.sc15aj.iotplatform;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;
import java.lang.Math;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DataGenerator {

	private static final double MAX_VALUE = 100;
	private static final String SampleStatistics = "{0:-60.196392992004334,5:620.4421901009101,14:420.4220612785746,13:185.21083185702275,15:483.72692251215295,1:594.7827813502976,3:140.3239790342253,16:3.104707691856035,9:635.8535653005378,19:322.0711157700041,11:87.66295667498484,18:857.7858889856491,17:101.49594891724111,2:921.839749304954,6:697.4655671122938,7:367.3720748762538,8:855.4795500704753,10:564.4074585413068,4:913.7870598326768,12:275.71369666459043}";
	private static Random ran;
	static DateFormat dateFormat;

	public static void main(String[] args) {

		String brokers = "localhost:9092";
		if (args.length > 0) {
			brokers = args[0];
		}
		ran = new Random();
		dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

	while (true){
		String jmsg = createMessage("sen1", "humidity");
		producer.send(new ProducerRecord<String, String>("statistics", "sen1", jmsg));
		try {
			
		Thread.sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	}

	private static String createMessage(String sensorName, String sensorType) {

		Calendar cal = Calendar.getInstance();

		String date = dateFormat.format(cal.getTime());


		String msg = "";

		for (int i = 0; i < 100; i++){
			double value = ran.nextDouble() * MAX_VALUE;
			value = round(value, 3);
			msg = msg + "\n" + date + "," + sensorName + "," + value;
			//msg = date + "," + sensorName + "," + value;
		}
		return msg;
	}

	public static double round(double value, int places) {
    if (places < 0) throw new IllegalArgumentException();

    long factor = (long) Math.pow(10, places);
    value = value * factor;
    long tmp = Math.round(value);
    return (double) tmp / factor;
}

}
