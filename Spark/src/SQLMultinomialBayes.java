import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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
//https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable
//-Dspark.master=local
public class SQLMultinomialBayes {
	public static void main(String[] args){
		SparkConf conf = new SparkConf().setAppName("Spark multinomial Naive Bayes in SQL");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for ( Logger logger : loggers ) {
		    logger.setLevel(Level.ERROR);
		}
		HiveContext hc = new HiveContext(sc.sc());
		
		
		
		JavaRDD<String> points = sc.textFile("data/naide.txt"); //http://sqldatamine.blogspot.com.ee/2013/07/classification-using-naive-bayes.html
		//String schemaString = "a b c d e f";
		String schemaString = "id age prescription astigmatic tears lens"; //change here
		List<StructField> fields = new ArrayList<StructField>();
		for(String fieldName : schemaString.split(" ")){
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = points.map(
				new Function<String, Row>(){
					public Row call(String record) throws Exception {
						String[] fields = record.split(",");
						return RowFactory.create(fields[0], fields[1].trim(), fields[2], fields[3], fields[4], fields[5]); //change here
					}
				});
		
		DataFrame pdf = hc.createDataFrame(rowRDD, schema);
		pdf.registerTempTable("RAW");
		

		JavaRDD<String> test = sc.textFile("data/test.txt");
		String testschemaString = "id attribute value";
		List<StructField> testfields = new ArrayList<StructField>();
		for(String fieldName : testschemaString.split(" ")){
			testfields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType testschema = DataTypes.createStructType(testfields);
		JavaRDD<Row> testrowRDD = test.map(
				new Function<String, Row>(){
					public Row call(String record) throws Exception {
						String[] fields = record.split(",");
						return RowFactory.create(fields[0], fields[1].trim(), fields[2]); //change here
					}
				});
		
		DataFrame testpdf = hc.createDataFrame(testrowRDD, testschema);
		testpdf.registerTempTable("test");
		
		DataFrame a,b,c,d,e;
		a = hc.sql("drop table priors");
		a = hc.sql("create table priors (class string, pfreq Int, pprob float) stored as textfile location '/home/madis/workspace/Spark/tables/bayes'");
		a = hc.sql("insert into priors select lens, count(1), sum(1.0) / total from RAW "
				+ "join (select count(1) total from RAW) V on (1 = 1) " //http://grokbase.com/t/hive/user/1289zqna6n/nested-select-statements
				+ "group by lens, total"); // Correlated subqueries are not supported in Hive.
		a = hc.sql("select * from priors");
		a.show();
		b = hc.sql("drop table attrclass");
		b = hc.sql("create table attrclass (attribute string, value string, class string, acprob float) stored as textfile location '/home/madis/workspace/Spark/tables/bayes'");
		b = hc.sql("insert into attrclass select attribute, value, p.class, acfreq/pfreq acprob from "
				+ "(select age attribute, age value, lens class, sum(1.0) acfreq from RAW group by age, lens "
				+ "union all "
				+ "select 'prescription', prescription, lens, sum(1.0) from RAW group by prescription, lens "
				+ "union all "
				+ "select 'astigmatic', astigmatic, lens, sum(1.0) from RAW group by astigmatic, lens "
				+ "union all "
				+ "select 'tears', tears, lens, sum(1.0) from RAW group by tears, lens) rd "
				+ "join priors p on rd.class = p.class"); //sometimes this query returns empty table. just delete metastore_db and run again
		b = hc.sql("select * from attrclass");
		b.show();
//		c = hc.sql("drop table test");
//		c = hc.sql("create table test (id int, attribute string, value string) stored as textfile location '/home/madis/workspace/Spark/tables/bayes'");
//		c = hc.sql("insert into test select '1', 'age','presbyopic'");
//		c = hc.sql("insert into test select '2, 'prescription','hypermetrope'");
//		c = hc.sql("insert into test select '3', 'astigmatic','no'");
//		c = hc.sql("insert into test select '4', 'tears','normal'");
//		c = hc.sql("select * from test");
//		c.show();
		
//		e = hc.sql("select * from test");
//		e.show();
		d = hc.sql("drop table fit");
		d = hc.sql("create table fit (id string, class string, score float) stored as textfile location '/home/madis/workspace/Spark/tables/bayes' ");
		
//		d = hc.sql("select s.attribute, s.value, p.class p.pprob from attrclass s join priors p on p.class = s.class group by s.attribute, s.value, p.class, p.pprob");
		System.out.println("asi");
		d = hc.sql("select id, acs.class, log(pprob) + sum(log(fprob)) score from test t join "
				+ "(select sp.attribute, sp.value, sp.class, sp.pprob, COALESCE(acprob,2.0) fprob from "
				+ "(select attrclass.attribute, attrclass.value, priors.class, priors.pprob from attrclass "
				+ "join priors on (priors.class = attrclass.class) group by attrclass.attribute, attrclass.value, priors.class, priors.pprob) sp "
				+ "left join attrclass s on sp.attribute = s.attribute and sp.value = s.value and sp.class = s.class) acs "
				+ "on t.attribute = acs.attribute and t.value = acs.value group by id, acs.class, pprob");
		d.show();
		System.out.println("asja lõpp");
//		d = hc.sql("select * from fit");
//		d.show();
//		multinomial naive bayes
		// create a raw data table, create a table with the prior distribution, create a table with the attribute class distributions, create a table of test data,  run a classification and check the accuracy.
		sc.close();
	}
}
