import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SQLBayes {
	//http://nuncupatively.blogspot.com.ee/2011/07/naive-bayes-in-sql.html
	public static void main(String[] args){
		final long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setAppName("Naive Bayes in Spark SQL");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext hc = new HiveContext(sc.sc());
		hc.sql("SET	hive.metastore.warehouse.dir=file:///home/madis/workspace/SparkHiveSQL/tables");
		
//		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
//		loggers.add(LogManager.getRootLogger());
//		for ( Logger logger : loggers ) {
//		    logger.setLevel(Level.ERROR);
//		}
		
		JavaRDD<String> points = sc.textFile(args[0],8); 
		String schemaString = "uid feature1 feature2 class"; //change here
		List<StructField> fields = new ArrayList<StructField>();
		for(String fieldName : schemaString.split(" ")){
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = points.map(
				new Function<String, Row>(){
					private static final long serialVersionUID = 3282431521901483532L;

					public Row call(String record) throws Exception {
						String[] fields = record.split(",");
						return RowFactory.create(fields[0], fields[1], fields[2], fields[55]); //change here
					}
				});
		JavaRDD<Row>[] split = rowRDD.randomSplit(new double[] { 0.8, 0.2 });
		DataFrame data = hc.createDataFrame(split[0], schema);//change to split[0]
		data.registerTempTable("data");
		DataFrame test = hc.createDataFrame(split[1], schema);//change to split[1]
		test.registerTempTable("testdata");
		

		DataFrame a;
		a = hc.sql("drop table agg");
		a = hc.sql("drop table coefficients");
		a = hc.sql("drop table coefficients2");
		a = hc.sql("drop table testscores");
		a = hc.sql("drop table predictions");
		
		//this creates a table from input data. Most important thing here is sum(1) value that counts the occurence of feature1 and feature2
		a = hc.sql("create table agg as "
				+ "select feature1,feature2, sum(1) value, class from data group by feature1,feature2, class");
		
		//here a table is created that counts class, feature1, feature2 and feature1 given class, feature given class and total class count
		a = hc.sql("create table coefficients2 as "
				+ "select class, feature1, feature2, "
				+ "sum (value) over (partition by feature1, class) as val1, "
				+ "sum (value) over (partition by feature2, class) as val2, "
				+ "sum (value) over (partition by class) as class_count, "
				+ "b.totals as total "
				+ "from agg left join (select count(*) totals from data) b on 1=1");//better total


		/* next table adds probabilities to previous table
		 * class_count/tot is the prior
		 * val1/class_count is P(val1|class) val1 - number of times feature1 is present in class
		 * val2/class_count is P(val2|class) val2 - number of times feature2 is present in class
		 */
		a = hc.sql("create table coefficients  as "
				+ "select *, log(class_count/total)+log(val1/class_count)+log(val2/class_count) probability from coefficients2");
		//creation of testtable that takes data from testdata and compares it to adds probabilities that were created earlier
		final long modelTime = System.currentTimeMillis();
		
		a = hc.sql("create table testscores as "
				+ "select a.uid, a.class as actual, b.class as prediction, max(probability) score, a.feature1,a.feature2 from testdata a "
				+ "left join coefficients b on a.feature1 = b.feature1 and a.feature2 = b.feature2 "
				+ "group by a.uid, a.class, b.class, a.feature1, a.feature2");
		
		//now select only pest predictions
		a = hc.sql("create table predictions as "
				+ "select a.uid, actual, prediction, score,feature1, feature2 from testscores a "
				+ "inner join (select c.uid, max(score) maxscore from testscores c group by c.uid) b on a.uid = b.uid and a.score = maxscore");
		//https://books.google.ee/books?id=yqhPCwAAQBAJ&pg=PA165&lpg=PA165&dq=spark+predicted+posterior+class+probabilities+from+the+trained+model,+in+the+same+order+as+class+labels&source=bl&ots=iMTMROwPFY&sig=cUI_4H2Br-sLElhbYpvkzd99zTY&hl=et&sa=X&ved=0ahUKEwi_yor2w8PMAhVHWCwKHTlzBRAQ6AEISTAG#v=onepage&q=spark%20predicted%20posterior%20class%20probabilities%20from%20the%20trained%20model%2C%20in%20the%20same%20order%20as%20class%20labels&f=false
		//show correct predictions - for this divide #of correct predictions with # of testdata
		a = hc.sql("select b.correct/count(*) accuracy from test left join (select count(*) correct from predictions where actual = prediction) b on 1 = 1 group by b.correct");
		a.show();
		//acc on original data - 306059 / 581012 - 52%
		final long endTime = System.currentTimeMillis();
		System.out.println("Model creation time time: " + (modelTime - startTime) );
		System.out.println("Execution time: " + (endTime - startTime) );
		a.rdd().saveAsTextFile(args[0]+" "+String.valueOf(endTime) +" SQL Bayes out " + String.valueOf(rowRDD.count()));
		sc.close();
	}
}
