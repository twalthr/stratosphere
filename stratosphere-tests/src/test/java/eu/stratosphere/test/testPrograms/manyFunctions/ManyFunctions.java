package eu.stratosphere.test.testPrograms.manyFunctions;

import java.io.File;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.accumulators.IntCounter;
import eu.stratosphere.api.common.operators.DeltaIteration;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.CoGroupFunction;
import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.io.avro.AvroInputFormat;
import eu.stratosphere.api.java.record.operators.CoGroupOperator;
import eu.stratosphere.api.java.record.operators.CrossOperator;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.JobExecutionResult;
import eu.stratosphere.types.BooleanValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class ManyFunctions implements Program, ProgramDescription {

	public static String customer;
	public static String lineitem;
	public static String nation;
	public static String orders;
	public static String ordersPath;
	public static String region;
	public static String outputTablePath;
	public static String outputAccumulatorsPath;
	public static String outputKeylessReducerPath;
	public static String outputKeylessReducerPath2;
	public static String outputOrderKeysPath;
	public static String outputOrderAvroPath;

	public static void main(String[] args) throws Exception {

		ManyFunctions manyFunctionsTest = new ManyFunctions();

		if (args.length < 9) {
			System.err.println(manyFunctionsTest.getDescription());
			System.exit(1);
		}

		customer = args[0];
		lineitem = args[1];
		nation = args[2];
		orders = args[3];
		ordersPath = args[4];
		region = args[5];
		outputTablePath = args[6];
		outputAccumulatorsPath = args[7];
		outputKeylessReducerPath = args[8];
		outputKeylessReducerPath2 = args[9];
		outputOrderKeysPath = args[10];
		outputOrderAvroPath = args[11];

		// Generate file for avro test
		DatumWriter<Order> orderDatumWriter = new SpecificDatumWriter<Order>(
				Order.class);
		DataFileWriter<Order> dataFileWriter = new DataFileWriter<Order>(
				orderDatumWriter);
		dataFileWriter.create(Order.getClassSchema(), new File(
				outputOrderAvroPath));
		Scanner s = new Scanner(new File(ordersPath));
		while (s.hasNextLine()) {
			@SuppressWarnings("resource")
			Scanner lineScanner = new Scanner(s.nextLine()).useDelimiter("\\|");

			Order o = new Order();
			o.setOOrderkey(lineScanner.nextInt());
			o.setOCustkey(lineScanner.nextInt());
			o.setOOrderstatus(lineScanner.next());
			o.setOTotalprice(lineScanner.nextFloat());
			o.setOOrderdate(lineScanner.next());
			o.setOOrderpriority(lineScanner.next());
			o.setOClerk(lineScanner.next());
			o.setOShipproprity(lineScanner.nextInt());
			o.setOComment(lineScanner.next());
			dataFileWriter.append(o);
			lineScanner.close();
		}
		dataFileWriter.flush();
		s.close();
		dataFileWriter.close();

		// Create plan and execute
		Plan plan = manyFunctionsTest.getPlan();

		JobExecutionResult result = LocalExecutor.execute(plan);

		PrintWriter out = new PrintWriter(outputAccumulatorsPath);
		out.println(result.getAccumulatorResult("count-american-customers"));
		out.println(result.getAccumulatorResult("count-europe-customers"));
		out.println(result.getAccumulatorResult("count-rest-customers"));
		out.close();

		// BEGIN: TEST 8
		int counter = (Integer) result
				.getAccumulatorResult("count-rest-customers");
		Scanner scanner = new Scanner(new File(outputKeylessReducerPath2));
		int counter2 = scanner.nextInt();
		scanner.close();

		if (counter != counter2)
			throw new Exception(
					"TEST 8 FAILED: Keyless Reducer and Accumulator count different");
		// END: TEST 8
	}

	@Override
	public Plan getPlan(String... args) {

		// Read TPC-H data from .tbl-files		
		// (supplier, part and partsupp not implemented yet)
		FileDataSource customerSource = new FileDataSource(
				new CsvInputFormat(), customer, "customer");
		CsvInputFormat.configureRecordFormat(customerSource)
				.recordDelimiter('\n').fieldDelimiter('|')
				.field(IntValue.class, 0).field(StringValue.class, 1)
				.field(StringValue.class, 2).field(IntValue.class, 3)
				.field(StringValue.class, 4).field(DoubleValue.class, 5)
				.field(StringValue.class, 6).field(StringValue.class, 7);

		FileDataSource lineitemSource = new FileDataSource(
				new CsvInputFormat(), lineitem, "lineitem");
		CsvInputFormat.configureRecordFormat(lineitemSource)
				.recordDelimiter('\n').fieldDelimiter('|')
				.field(IntValue.class, 0).field(IntValue.class, 1)
				.field(IntValue.class, 2).field(IntValue.class, 3)
				.field(IntValue.class, 4).field(FloatValue.class, 5)
				.field(FloatValue.class, 6).field(FloatValue.class, 7)
				.field(StringValue.class, 8).field(StringValue.class, 9)
				.field(StringValue.class, 10).field(StringValue.class, 11)
				.field(StringValue.class, 12).field(StringValue.class, 13)
				.field(StringValue.class, 14).field(StringValue.class, 15);

		FileDataSource nationSource = new FileDataSource(new CsvInputFormat(),
				nation, "nation");
		CsvInputFormat.configureRecordFormat(nationSource)
				.recordDelimiter('\n').fieldDelimiter('|')
				.field(IntValue.class, 0).field(StringValue.class, 1)
				.field(IntValue.class, 2).field(StringValue.class, 3);

		FileDataSource ordersSource = new FileDataSource(new CsvInputFormat(),
				orders, "orders");
		CsvInputFormat.configureRecordFormat(ordersSource)
				.recordDelimiter('\n').fieldDelimiter('|')
				.field(IntValue.class, 0).field(IntValue.class, 1)
				.field(StringValue.class, 2).field(FloatValue.class, 3)
				.field(StringValue.class, 4).field(StringValue.class, 5)
				.field(StringValue.class, 6).field(IntValue.class, 7)
				.field(StringValue.class, 8);

		FileDataSource regionSource = new FileDataSource(new CsvInputFormat(),
				region, "region");
		CsvInputFormat.configureRecordFormat(regionSource)
				.recordDelimiter('\n').fieldDelimiter('|')
				.field(IntValue.class, 0).field(StringValue.class, 1)
				.field(StringValue.class, 2);

		// BEGIN: TEST 1 - Usage of Join, Map, KeylessReducer, CsvOutputFormat, CoGroup

		// Join fields of customer and nation
		JoinOperator customerWithNation = JoinOperator
				.builder(JoinFields.class, IntValue.class, 3, 0)
				.input1(customerSource).input2(nationSource).build();
		joinQuickFix(customerWithNation);

		// Join fields of customerWithNation and region
		JoinOperator customerWithNationRegion = JoinOperator
				.builder(JoinFields.class, IntValue.class, 10, 0)
				.input1(customerWithNation).input2(regionSource).build();
		joinQuickFix(customerWithNationRegion);

		// Split the customers by regions
		MapOperator customersInAmerica = MapOperator
				.builder(FilterRegionAmerica.class)
				.input(customerWithNationRegion).build();
		MapOperator customersInEurope = MapOperator
				.builder(FilterRegionEurope.class)
				.input(customerWithNationRegion).build();
		MapOperator customersInOtherRegions = MapOperator
				.builder(FilterRegionOthers.class)
				.input(customerWithNationRegion).build();

		// Count customers of other regions
		ReduceOperator countCustomersOfOtherRegion = ReduceOperator
				.builder(ReduceCounter.class).input(customersInOtherRegions)
				.build();

		// Save keyless reducer results
		FileDataSink resultKR = new FileDataSink(new CsvOutputFormat(),
				outputKeylessReducerPath);
		resultKR.addInput(countCustomersOfOtherRegion);
		CsvOutputFormat.configureRecordFormat(resultKR).recordDelimiter('\n')
				.fieldDelimiter('|').field(IntValue.class, 0);

		// Union again and filter customer fields
		MapOperator unionOfRegions = MapOperator
				.builder(FilterCustomerFields.class)
				.input(customersInAmerica, customersInEurope,
						customersInOtherRegions).build();

		// Save test results to disk
		FileDataSink test1Sink = new FileDataSink(new CsvOutputFormat(),
				outputTablePath + "/Test1.tbl");
		test1Sink.addInput(unionOfRegions);
		CsvOutputFormat.configureRecordFormat(test1Sink).recordDelimiter('\n')
				.fieldDelimiter('|').field(IntValue.class, 0)
				.field(StringValue.class, 1).field(StringValue.class, 2)
				.field(IntValue.class, 3).field(StringValue.class, 4)
				.field(DoubleValue.class, 5).field(StringValue.class, 6)
				.field(StringValue.class, 7);

		// Test: Compare to input source
		CoGroupOperator testCustomerIdentity1 = CoGroupOperator
				.builder(CoGroupTestIdentity.class, IntValue.class, 0, 0)
				.input1(customerSource).input2(unionOfRegions).build();

		// END: TEST 1

		// BEGIN: TEST 2 - Usage of Join, Reduce, Map, Cross, CoGroup

		// Collect customers keys from customers that ever placed orders
		JoinOperator customersWithOrders = JoinOperator
				.builder(CollectCustomerKeysWithOrders.class, IntValue.class,
						0, 0).input1(lineitemSource).input2(ordersSource)
				.build();
		joinQuickFix(customersWithOrders);
		ReduceOperator removeDuplicates = ReduceOperator
				.builder(RemoveDuplicates.class, IntValue.class, 0)
				.input(customersWithOrders).build();

		// Cross LineItems and Orders
		CrossOperator lineitemsWithOrders = CrossOperator
				.builder(CrossJoinFields.class).input1(lineitemSource)
				.input2(ordersSource).build();

		// Filter customer key
		MapOperator customerKeyWithOrders2 = MapOperator
				.builder(FilterCustomerKeyFromLineItemsOrders.class)
				.input(lineitemsWithOrders).build();
		ReduceOperator removeDuplicates2 = ReduceOperator
				.builder(RemoveDuplicates.class, IntValue.class, 0)
				.input(customerKeyWithOrders2).build();

		// Save test results to disk
		FileDataSink test2Sink = new FileDataSink(new CsvOutputFormat(),
				outputTablePath + "/Test2.tbl");
		test2Sink.addInput(removeDuplicates2);
		CsvOutputFormat.configureRecordFormat(test2Sink).recordDelimiter('\n')
				.fieldDelimiter('|').field(IntValue.class, 0);

		// Test: Compare customer keys
		CoGroupOperator testCustomerIdentity2 = CoGroupOperator
				.builder(CoGroupTestIdentity.class, IntValue.class, 0, 0)
				.input1(removeDuplicates).input2(removeDuplicates2).build();

		// END: TEST 2

		// BEGIN: TEST 3 - Usage of Delta Iterations to determine customers woth no orders
		DeltaIteration iteration = new DeltaIteration(0);
		iteration.setMaximumNumberOfIterations(10000); // Exception otherwise

		// Add a flag field to each customer (initial value: false)
		MapOperator customersWithFlag = MapOperator.builder(AddFlag.class)
				.input(customerSource).build();

		iteration.setInitialSolutionSet(customersWithFlag);
		iteration.setInitialWorkset(customersWithFlag);

		// As input for each iteration
		// Exception otherwise
		JoinOperator iterationInput = JoinOperator
				.builder(WorkSolutionSetJoin.class, IntValue.class, 0, 0)
				.name("JOIN ITERATION").input1(iteration.getWorkset())
				.input2(iteration.getSolutionSet()).build();

		// Pick one customer from working set
		ReduceOperator oneCustomer = ReduceOperator
				.builder(PickOneRecord.class).input(iterationInput).build();

		// Determine all customers from input with no orders (in this case:
		// check if the picked customer has no orders)
		CoGroupOperator customerWithNoOrders = CoGroupOperator
				.builder(CustomersWithNoOrders.class, IntValue.class, 0, 1)
				.input1(oneCustomer).input2(ordersSource).build();

		// Set the flag for the customer with no order
		MapOperator customerWithSetFlag = MapOperator.builder(SetFlag.class)
				.input(customerWithNoOrders).build();

		// Set changed customers (delta)
		iteration.setSolutionSetDelta(customerWithSetFlag);

		// Remove checked customer from previous working set
		CoGroupOperator filteredWorkset = CoGroupOperator
				.builder(RemoveCheckedCustomer.class, IntValue.class, 0, 0)
				.input1(iteration.getWorkset()).input2(oneCustomer).build();

		// Define workset for next iteration
		iteration.setNextWorkset(filteredWorkset);

		// Remove unflagged customer
		MapOperator filteredFlaggedSolutionSet = MapOperator
				.builder(FilterFlaggedCustomers.class).input(iteration).build();

		// Extract only the customer keys
		MapOperator customerKeysWithNoOrders = MapOperator
				.builder(FilterFirstFieldIntKey.class)
				.input(filteredFlaggedSolutionSet).build();

		// Save the customers without orders in file
		FileDataSink test3Sink = new FileDataSink(new CsvOutputFormat(),
				outputTablePath + "/Test3.tbl");
		test3Sink.addInput(customerKeysWithNoOrders);
		CsvOutputFormat.configureRecordFormat(test3Sink).recordDelimiter('\n')
				.fieldDelimiter('|').field(IntValue.class, 0);

		// Union all customers WITH orders from previous test with all customers WITHOUT orders
		MapOperator unionCustomers = MapOperator.builder(DummyMapper.class)
				.input(customerKeysWithNoOrders, testCustomerIdentity2).build();

		// Filter for customers keys of test 1
		MapOperator allCustomerKeys = MapOperator
				.builder(FilterFirstFieldIntKey.class)
				.input(testCustomerIdentity1).build();

		// Test if unionCustomers contains all customers again
		CoGroupOperator testCustomerIdentity3 = CoGroupOperator
				.builder(CoGroupTestIdentity.class, IntValue.class, 0, 0)
				.input1(unionCustomers).input2(allCustomerKeys).build();
		// END: TEST 3

		// BEGIN: TEST 4 - Usage of TextInputFormat

		// Get all order keys by joining with all customers that placed orders from previous test
		JoinOperator allOrderKeys = JoinOperator
				.builder(OrderKeysFromCustomerKeys.class, IntValue.class, 0, 1)
				.input1(testCustomerIdentity3).input2(ordersSource).build();

		// Get the string lines of the orders file
		FileDataSource ordersTextInputSource = new FileDataSource(
				new TextInputFormat(), orders);

		// Extract order keys out of string lines
		MapOperator stringExtractKeys = MapOperator
				.builder(ExtractKeysFromTextInput.class)
				.input(ordersTextInputSource).build();

		// Save the orders in file
		FileDataSink test4Sink = new FileDataSink(new CsvOutputFormat(),
				outputTablePath + "/Test4.tbl");
		test4Sink.addInput(stringExtractKeys);
		CsvOutputFormat.configureRecordFormat(test4Sink).recordDelimiter('\n')
				.fieldDelimiter('|').field(IntValue.class, 0);

		// Test if extracted values are correct
		CoGroupOperator testOrderIdentity = CoGroupOperator
				.builder(CoGroupTestIdentity.class, IntValue.class, 0, 0)
				.input1(allOrderKeys).input2(stringExtractKeys).build();

		// END: TEST 4

		// BEGIN: TEST 5 - Usage of AvroInputFormat

		// extract orders from avro file
		FileDataSource ordersAvroInputSource = new FileDataSource(
				new AvroInputFormat(), "file://" + outputOrderAvroPath);

		// Extract keys
		MapOperator extractKeys = MapOperator
				.builder(FilterFirstFieldIntKey.class)
				.input(ordersAvroInputSource).build();

		// Save the order keys in file
		FileDataSink test5Sink = new FileDataSink(new CsvOutputFormat(),
				outputTablePath + "/Test5.tbl");
		test5Sink.addInput(extractKeys);
		CsvOutputFormat.configureRecordFormat(test5Sink).recordDelimiter('\n')
				.fieldDelimiter('|').field(IntValue.class, 0);

		CoGroupOperator testOrderIdentity2 = CoGroupOperator
				.builder(CoGroupTestIdentity.class, IntValue.class, 0, 0)
				.input1(testOrderIdentity).input2(extractKeys).build();

		// END: TEST 5

		// BEGIN: TEST 6 - date count

		// Count different order dates
		MapOperator orderDateCountMap = MapOperator
				.builder(OrderDateCountMap.class).input(ordersAvroInputSource)
				.build();

		// Sum up
		ReduceOperator orderDateCountReduce = ReduceOperator
				.builder(OrderDateCountReduce.class)
				.keyField(StringValue.class, 0).input(orderDateCountMap)
				.build();

		// Save the orders in file
		FileDataSink test6Sink = new FileDataSink(new CsvOutputFormat(),
				outputTablePath + "/Test6.tbl");
		test6Sink.addInput(orderDateCountReduce);
		CsvOutputFormat.configureRecordFormat(test6Sink).recordDelimiter('\n')
				.fieldDelimiter('|').field(StringValue.class, 0)
				.field(IntValue.class, 1);

		// do the same with the original orders file

		// Count different order dates
		MapOperator orderDateCountMap2 = MapOperator
				.builder(OrderDateCountMap.class).input(ordersSource).build();

		// Sum up
		ReduceOperator orderDateCountReduce2 = ReduceOperator
				.builder(OrderDateCountReduce.class)
				.keyField(StringValue.class, 0).input(orderDateCountMap2)
				.build();

		// Check if date count is correct
		CoGroupOperator testOrderIdentity3 = CoGroupOperator
				.builder(CoGroupTestIdentity.class, StringValue.class, 0, 0)
				.name("testOrderIdentity3").input1(orderDateCountReduce).input2(orderDateCountReduce2)
				.build();

		// END: TEST 6

		// BEGIN: TEST 7

		// Sum up counts
		ReduceOperator sumUp = ReduceOperator.builder(SumUpDateCounts.class)
				.input(testOrderIdentity3).build();

		// Count all orders
		ReduceOperator orderCount = ReduceOperator.builder(ReduceCounter.class)
				.input(testOrderIdentity2).build();

		// Check if the values are equal
		CoGroupOperator testCountOrdersIdentity = CoGroupOperator
				.builder(CoGroupTestIdentity.class, IntValue.class, 0, 0).name("testCountOrdersIdentity")
				.input1(sumUp).input2(orderCount).build();

		// Write count to disk
		FileDataSink test7Sink = new FileDataSink(new CsvOutputFormat(),
				outputTablePath + "/Test7.tbl");
		test7Sink.addInput(testCountOrdersIdentity);
		CsvOutputFormat.configureRecordFormat(test7Sink).recordDelimiter('\n')
				.fieldDelimiter('|').field(IntValue.class, 0);

		// END: TEST 7		

		Plan p = new Plan(test7Sink, "FullTest");
		p.addDataSink(test1Sink);
		p.addDataSink(test2Sink);
		p.addDataSink(test3Sink);
		p.addDataSink(test4Sink);
		p.addDataSink(test5Sink);
		p.addDataSink(test6Sink);
		p.addDataSink(resultKR);
		return p;
	}

	@Override
	public String getDescription() {
		return "Parameters: [customer] [lineitem] [nation] [orders] [region] [output]";
	}

	// Quick fix for Join bug
	private void joinQuickFix(JoinOperator j) {
		j.setParameter(PactCompiler.HINT_LOCAL_STRATEGY,
				PactCompiler.HINT_LOCAL_STRATEGY_MERGE);
	}

	// Joins the fields of two record into one record
	public static class JoinFields extends JoinFunction {

		@Override
		public void join(Record r1, Record r2, Collector<Record> out)
				throws Exception {

			Record newRecord = new Record(r1.getNumFields() + r2.getNumFields());

			int[] r1Positions = new int[r1.getNumFields()];
			for (int i = 0; i < r1Positions.length; ++i) {
				r1Positions[i] = i;
			}
			newRecord.copyFrom(r1, r1Positions, r1Positions);

			int[] r2Positions = new int[r2.getNumFields()];
			int[] targetR2Positions = new int[r2.getNumFields()];
			for (int i = 0; i < r2Positions.length; ++i) {
				r2Positions[i] = i;
				targetR2Positions[i] = i + r1Positions.length;
			}
			newRecord.copyFrom(r2, r2Positions, targetR2Positions);

			out.collect(newRecord);
		}

	}

	// Filter for region "AMERICA"
	public static class FilterRegionAmerica extends MapFunction {

		private IntCounter numLines = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator("count-american-customers",
					this.numLines);
		}

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {

			if (record.getField(13, StringValue.class).toString()
					.equals("AMERICA")) {
				out.collect(record);
				this.numLines.add(1);
			}
		}

	}

	// Filter for region "EUROPE"
	public static class FilterRegionEurope extends MapFunction {

		private IntCounter numLines = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator("count-europe-customers",
					this.numLines);
		}

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			if (record.getField(13, StringValue.class).toString()
					.equals("EUROPE")) {
				out.collect(record);
				this.numLines.add(1);
			}

		}

	}

	// Filter for regions other than "AMERICA" and "EUROPE"
	public static class FilterRegionOthers extends MapFunction {

		private IntCounter numLines = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			getRuntimeContext().addAccumulator("count-rest-customers",
					this.numLines);
		}

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			if (!record.getField(13, StringValue.class).toString()
					.equals("AMERICA")
					&& !record.getField(13, StringValue.class).toString()
							.equals("EUROPE")) {
				out.collect(record);
				this.numLines.add(1);
			}

		}

	}

	// Extract customer fields out of customer-nation-region record
	public static class FilterCustomerFields extends MapFunction {

		@Override
		public void map(Record cnr, Collector<Record> out) throws Exception {
			Record newRecord = new Record(8);
			int[] positions = new int[8];
			for (int i = 0; i < positions.length; ++i) {
				positions[i] = i;
			}
			newRecord.copyFrom(cnr, positions, positions);
			out.collect(newRecord);
		}

	}

	// Test if each key has an equivalent key and fields of both inputs are equals
	public static class CoGroupTestIdentity extends CoGroupFunction {

		@Override
		public void coGroup(Iterator<Record> records1,
				Iterator<Record> records2, Collector<Record> out)
				throws Exception {

			int count1 = 0;
			Record lastR1 = null;
			while (records1.hasNext()) {
				lastR1 = records1.next();
				count1++;
			}

			int count2 = 0;
			Record lastR2 = null;
			while (records2.hasNext()) {
				lastR2 = records2.next();
				count2++;
			}

			if (count1 != 1 || count2 != 1)
				throw new Exception(
						"TEST FAILED: The count of the two inputs do not match: "
								+ count1 + " / " + count2);

			if (lastR1.getNumFields() != lastR2.getNumFields())
				throw new Exception(
						"TEST FAILED: The number of fields of the two inputs do not match: "
								+ lastR1.getNumFields() + " / "
								+ lastR2.getNumFields());
			out.collect(lastR2);
		}

	}

	// Join LineItems with Orders, collect all customer keys with orders
	// (records from LineItems is are not used, result contains duplicates)
	public static class CollectCustomerKeysWithOrders extends JoinFunction {

		@Override
		public void join(Record l, Record o, Collector<Record> out)
				throws Exception {
			out.collect(new Record(o.getField(1, IntValue.class)));
		}

	}

	// Removes duplicate keys
	public static class RemoveDuplicates extends ReduceFunction {

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			Record record = records.next();
			out.collect(record);
		}
	}

	// Crosses two input streams and returns records with merged fields
	public static class CrossJoinFields extends CrossFunction {

		@Override
		public void cross(Record r1, Record r2, Collector<Record> out)
				throws Exception {
			Record newRecord = new Record(r1.getNumFields() + r2.getNumFields());

			int[] r1Positions = new int[r1.getNumFields()];
			for (int i = 0; i < r1Positions.length; ++i) {
				r1Positions[i] = i;
			}
			newRecord.copyFrom(r1, r1Positions, r1Positions);

			int[] r2Positions = new int[r2.getNumFields()];
			int[] targetR2Positions = new int[r2.getNumFields()];
			for (int i = 0; i < r2Positions.length; ++i) {
				r2Positions[i] = i;
				targetR2Positions[i] = i + r1Positions.length;
			}
			newRecord.copyFrom(r2, r2Positions, targetR2Positions);

			out.collect(newRecord);
		}

	}

	// Filters the customer key from the LineItem-Order records
	public static class FilterCustomerKeyFromLineItemsOrders extends
			MapFunction {

		@Override
		public void map(Record lo, Collector<Record> out) throws Exception {
			if (lo.getField(0, IntValue.class).getValue() == lo.getField(16,
					IntValue.class).getValue()) {
				out.collect(new Record(lo.getField(17, IntValue.class)));
			}

		}

	}

	// Counts the input records
	public static class ReduceCounter extends ReduceFunction {

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {

			int counter = 0;

			while (records.hasNext()) {
				records.next();
				counter++;
			}
			out.collect(new Record(new IntValue(counter)));
		}

	}

	// Gets all order keys of a customer key
	public static class OrderKeysFromCustomerKeys extends JoinFunction {

		@Override
		public void join(Record c, Record o, Collector<Record> out)
				throws Exception {
			out.collect(new Record(o.getField(0, IntValue.class)));
		}
	}

	// Parses the first key from a string line
	public static class ExtractKeysFromTextInput extends MapFunction {

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			String line = record.getField(0, StringValue.class).getValue();
			@SuppressWarnings("resource")
			Scanner s = new Scanner(line).useDelimiter("\\|");
			int orderKey = s.nextInt();
			out.collect(new Record(new IntValue(orderKey)));
			s.close();
		}

	}

	// Creates string/integer pairs of order dates
	public static class OrderDateCountMap extends MapFunction {

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			out.collect(new Record(record.getField(4, StringValue.class),
					new IntValue(1)));
		}

	}

	// Sums up the counts for a certain given order date 
	public static class OrderDateCountReduce extends ReduceFunction {

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			Record element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				int cnt = element.getField(1, IntValue.class).getValue();
				sum += cnt;
			}

			element.setField(1, new IntValue(sum));
			out.collect(element);
		}
	}

	// Sum up all date counts
	public static class SumUpDateCounts extends ReduceFunction {

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			int count = 0;
			while (records.hasNext()) {
				count += records.next().getField(1, IntValue.class).getValue();
			}
			out.collect(new Record(new IntValue(count)));
		}
	}

	// Join which directly outputs the Workset (only necessary to fulfill iteration constraints)
	public static class WorkSolutionSetJoin extends JoinFunction {

		@Override
		public void join(Record worksetC, Record solutionC,
				Collector<Record> out) throws Exception {
			out.collect(worksetC);
		}

	}

	// Outputs the first record of the input stream
	public static class PickOneRecord extends ReduceFunction {

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			if (records.hasNext()) {
				out.collect(records.next());
			}
			while (records.hasNext())
				records.next();
		}

	}

	// Returns only Customers that have no matching Order
	public static class CustomersWithNoOrders extends CoGroupFunction {

		@Override
		public void coGroup(Iterator<Record> c, Iterator<Record> o,
				Collector<Record> out) throws Exception {

			// if no order is present output customer
			if (c.hasNext() && !o.hasNext()) {
				out.collect(c.next());
			}
		}

	}

	// Adds a flag field to each record.
	public static class AddFlag extends MapFunction {

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			record.addField(new BooleanValue(false));
			out.collect(record);
		}

	}

	// Sets the last (Boolean) flag to "true".
	public static class SetFlag extends MapFunction {

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			record.setField(record.getNumFields() - 1, new BooleanValue(true));
			out.collect(record);
		}

	}

	// Only return customers that are not in input2
	public static class RemoveCheckedCustomer extends CoGroupFunction {

		@Override
		public void coGroup(Iterator<Record> workingSet,
				Iterator<Record> checkedCustomer, Collector<Record> out)
				throws Exception {
			if (!checkedCustomer.hasNext()) {
				while (workingSet.hasNext())
					out.collect(workingSet.next());
			}
		}

	}

	// Returns all customers with set flag
	public static class FilterFlaggedCustomers extends MapFunction {

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			if (record.getField(record.getNumFields() - 1, BooleanValue.class)
					.getValue()) {
				out.collect(record);
			}
		}

	}

	// Returns only the first integer field as record
	public static class FilterFirstFieldIntKey extends MapFunction {

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			out.collect(new Record(record.getField(0, IntValue.class)));
		}
	}

	// Dummy mapper. For testing purposes.
	public static class DummyMapper extends MapFunction {

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			out.collect(record);
		}

	}

}
