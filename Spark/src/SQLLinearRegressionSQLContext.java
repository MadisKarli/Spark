import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

//http://archive.ics.uci.edu/ml/datasets/Online+Video+Characteristics+and+Transcoding+Time+Dataset
public class SQLLinearRegressionSQLContext {
		public static void main(String[] args){
			final long startTime = System.currentTimeMillis();
			SparkConf conf = new SparkConf().setAppName("Linear Regression in Spark SQL using SQLContext on " + args[0]);
			conf.set("eventLog.enabled", "false");
			JavaSparkContext jsc = new JavaSparkContext(conf);
			SQLContext hc = new SQLContext(jsc.sc());
			hc.sql("SET	hive.metastore.warehouse.dir=file:///home/madis/workspace/SparkHiveSQL/tables");
			
//			List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
//			loggers.add(LogManager.getRootLogger());
//			for ( Logger logger : loggers ) {
//			    logger.setLevel(Level.ERROR);
//			}
//			
			JavaRDD<String> points = jsc.textFile(args[0],8); 
			String schemaString = "x y";
			List<StructField> fields = new ArrayList<StructField>();
			for(String fieldName : schemaString.split(" ")){
				fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
			}
			StructType schema = DataTypes.createStructType(fields);
			JavaRDD<Row> rowRDD = points.map(
					new Function<String, Row>(){
						private static final long serialVersionUID = -5410204604525205517L;

						public Row call(String record) throws Exception {
							String[] fields = record.split("\t");
							return RowFactory.create(fields[4], fields[5]); 
						}
					});
			JavaRDD<Row>[] split = rowRDD.randomSplit(new double[] { 0.8, 0.2 });
			DataFrame data = hc.createDataFrame(split[0], schema);
			data.registerTempTable("data");
			DataFrame test = hc.createDataFrame(split[1], schema);
			test.registerTempTable("testdata");
			//calculate model aka find slope and count
			DataFrame a = hc.sql("select ((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x), 2)) slope, avg(y)-((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x),2))*avg(x) intercept from data");
			a.show();
			//test the model and calculate mean squared error
			a = hc.sql("select avg(error) MeanSquaredError from "
					+ "(select pow(y-(intercept +x*slope),2) error from testdata "
					+ "left join (select ((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x), 2)) slope, avg(y)-((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x),2))*avg(x) intercept from data) b on 1=1)a");
			a.show();
			final long endTime = System.currentTimeMillis();
			a.rdd().saveAsTextFile((args[0]+ " " + String.valueOf(endTime) + " Spark SQL linearregression sqlcontext out ")+String.valueOf(rowRDD.count()));
			System.out.println("Execution time: " + (endTime - startTime) );
			jsc.close();
			/* on initial data
			 * +------------------+-----------------+
			 * |             slope|        intercept|
			 * +------------------+-----------------+
			 * |0.5159989173914568|78.83729330624783|
			 * +------------------+-----------------+
			 * +-----------------+
			 * | MeanSquaredError|
			 * +-----------------+
			 * |6484.900479676353|
			 * +-----------------+
			 */
	}
}

