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

//http://archive.ics.uci.edu/ml/datasets/Online+Video+Characteristics+and+Transcoding+Time+Dataset
public class SQLLinearRegressionHiveContext {
		public static void main(String[] args){
			final long startTime = System.currentTimeMillis();
			SparkConf conf = new SparkConf().setAppName("Linear Regression in Spark SQL using HiveContext on " + args[0]);
			conf.set("eventLog.enabled", "false");
			JavaSparkContext jsc = new JavaSparkContext(conf);
			HiveContext hc = new HiveContext(jsc.sc());
			hc.sql("SET	hive.metastore.warehouse.dir=file:///home/madis/workspace/SparkHiveSQL/tables");
			
//			List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
//			loggers.add(LogManager.getRootLogger());
//			for ( Logger logger : loggers ) {
//			    logger.setLevel(Level.ERROR);
//			}
			
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
			
			
			//https://ayadshammout.com/2013/11/30/t-sql-linear-regression-function/
			/*
			 * SparkSQL (as of 1.6) does not support storing values in variables. This can be overcome by storing values in minitables, storing variables is also possible when using shell.
			 * Following uses 4*count(*), 2*sum(x) and sum(y). Let's hope Optimization gods are smart enough to see this.
			 */
			DataFrame a;
//			a = hc.sql("drop table linearvalues");
//			a = hc.sql("drop table output");
			//calculate model aka find slope and count
			a = hc.sql("select ((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x), 2)) intercept, avg(y)-((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x),2))*avg(x) slope from data");
			a.show();
			
			final long modelTime = System.currentTimeMillis();
			//test the model and calculate mean squared error
			a = hc.sql("select avg(error) MeanSquaredError from "
					+ "(select pow(y-(intercept +x*slope),2) error from testdata "
					+ "left join (select ((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x), 2)) slope, avg(y)-((count(*)*sum(x*y))-(sum(x)*sum(y)))/((count(*)*sum(pow(x,2)))-pow(sum(x),2))*avg(x) intercept from data) b on 1=1)a");
			a.show();			
			
			//this calculates R2
//			a = hc.sql("select (slope * sum(y) + intercept * sum(x*y) - sum(y) * sum(y)/count(*))/(sum(y*y)-sum(y) * sum(y) / count(*)) R2 from data, linearvalues group by intercept,slope");
			
			
			final long endTime = System.currentTimeMillis();
			a.rdd().saveAsTextFile((args[0]+" "+String.valueOf(endTime) +" SQL linearregression hivecontext out ")+String.valueOf(rowRDD.count()));
			System.out.println("Model creation time time: " + (modelTime - startTime) );
			System.out.println("Execution time: " + (endTime - startTime) );
			jsc.close();
			/*
			 *  +------------------+-----------------+
			 *	|         intercept|            slope|
			 *	+------------------+-----------------+
			 *	|0.5161316017291351|78.79018764377196|
			 *	+------------------+-----------------+
			 *	
			 *	+-----------------+
			 *	| MeanSquaredError|
			 *	+-----------------+
			 *	|6327.865714780089|
			 *	+-----------------+
			 */
	}
}

