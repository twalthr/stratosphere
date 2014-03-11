/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.example.java.relational;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.KeySelector;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple7;
import eu.stratosphere.util.Collector;

/**
 * This program implements a modified version of the TPC-H query 1.
 * 
 * The original query can be found at
 * http://www.tpc.org/tpch/spec/tpch2.16.0.pdf (page 29).
 * 
 * This program implements the following SQL equivalent:
 * 
 * SELECT
 *        l_returnflag, 
 *        l_linestatus, 
 *        SUM(l_quantity) as sum_qty, 
 *        SUM(l_extendedprice) as sum_base_price, 
 *        SUM(l_extendedprice*(1-l_discount)) as sum_disc_price, 
 *        SUM(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, 
 *        AVG(l_quantity) as avg_qty, 
 *        AVG(l_extendedprice) as avg_price, 
 *        AVG(l_discount) as avg_disc, 
 *        COUNT(*) as count_order 
 * FROM
 *        lineitem 
 * WHERE
 *        l_shipdate < DATE '1998-09-03'
 * GROUP BY
 *        l_returnflag,
 *        l_linestatus 
 */
public class TPCHQuery1 {

	/**
	 * Custom type for a Lineitem.
	 */
	public static class Lineitem {

		public Integer l_quantity;
		public Double l_extendedprice;
		public Double l_discount;
		public Double l_tax;
		public String l_returnflag;
		public String l_linestatus;
		public String l_shipdate;

		public Lineitem() {
			// default constructor
		}

		public Lineitem(Integer l_quantity, Double l_extendedprice, Double l_discount, Double l_tax, String l_returnflag,
				String l_linestatus, String l_shipdate) {
			this.l_quantity = l_quantity;
			this.l_extendedprice = l_extendedprice;
			this.l_discount = l_discount;
			this.l_tax = l_tax;
			this.l_returnflag = l_returnflag;
			this.l_linestatus = l_linestatus;
			this.l_shipdate = l_shipdate;
		}
	}

	/**
	 * Custom type for a line of the pricing summary report.
	 */
	public static class PricingSummaryReportItem {
		public String l_returnflag;
		public String l_linestatus;
		public Double sum_qty;
		public Double sum_base_price;
		public Double sum_disc_price;
		public Double sum_charge;
		public Double avg_qty;
		public Double avg_price;
		public Double avg_disc;
		public Integer count_order;

		public PricingSummaryReportItem() {
			this(null, null, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0);
		}

		public PricingSummaryReportItem(String l_returnflag, String l_linestatus, Double sum_qty, Double sum_base_price,
				Double sum_disc_price, Double sum_charge, Double avg_qty, Double avg_price, Double avg_disc, Integer count_order) {
			this.l_returnflag = l_returnflag;
			this.l_linestatus = l_linestatus;
			this.sum_qty = sum_qty;
			this.sum_base_price = sum_base_price;
			this.sum_disc_price = sum_disc_price;
			this.sum_charge = sum_charge;
			this.avg_qty = avg_qty;
			this.avg_price = avg_price;
			this.avg_disc = avg_disc;
			this.count_order = count_order;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("l_returnflag: " + this.l_returnflag);
			sb.append("\nl_linestatus: " + this.l_linestatus);
			sb.append("\nsum_qty: " + this.sum_qty);
			sb.append("\nsum_base_price: " + this.sum_base_price);
			sb.append("\nsum_disc_price: " + this.sum_disc_price);
			sb.append("\nsum_charge: " + this.sum_charge);
			sb.append("\navg_qty: " + this.avg_qty);
			sb.append("\navg_price: " + this.avg_price);
			sb.append("\navg_disc: " + this.avg_disc);
			sb.append("\ncount_order: " + this.count_order);
			sb.append("\n\n");
			return sb.toString();
		}
	}

	public static void main(String[] args) throws Exception {

		final String lineitemPath;

		if (args.length < 1) {
			throw new IllegalArgumentException("Invalid number of parameters: [lineitem.tbl]");
		} else {
			lineitemPath = args[0];
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);

		// read the required fields into a tuple
		// l_quantity l_extendedprice l_discount l_tax l_returnflag l_linestatus l_shipdate
		DataSet<Tuple7<Integer, Double, Double, Double, String, String, String>> lineitems = env.readCsvFile(lineitemPath)
				.fieldDelimiter('|').includeFields("0000111111100000")
				.types(Integer.class, Double.class, Double.class, Double.class, String.class, String.class, String.class);

		// convert the tuple to custom type for better readability
		DataSet<Lineitem> customLineitem = lineitems
				.map(new MapFunction<Tuple7<Integer, Double, Double, Double, String, String, String>, Lineitem>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Lineitem map(Tuple7<Integer, Double, Double, Double, String, String, String> t) throws Exception {
						return new Lineitem(t.T1(), t.T2(), t.T3(), t.T4(), t.T5(), t.T6(), t.T7());
					}
				});

		// filter by shipdate
		DataSet<Lineitem> filteredByShipDate = customLineitem.filter(new FilterFunction<Lineitem>() {
			private static final long serialVersionUID = 1L;
			private final String DATE_CONSTANT = "1998-09-03";

			private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			private final Date constantDate;

			{
				try {
					this.constantDate = format.parse(DATE_CONSTANT);
				} catch (ParseException e) {
					throw new RuntimeException("Date constant could not be parsed.");
				}
			}

			@Override
			public boolean filter(Lineitem li) throws Exception {
				try {
					Date shipDate = format.parse(li.l_shipdate);
					if (shipDate.before(constantDate)) {
						return true;
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return false;
			}
		});

		// aggregate by returnflag and linestatus and calculate PricingSummaryReportItems
		DataSet<PricingSummaryReportItem> aggregated = filteredByShipDate.groupBy(new KeySelector<Lineitem, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(Lineitem li) {
				return li.l_returnflag.concat(li.l_linestatus);
			}
		}).reduceGroup(new GroupReduceFunction<Lineitem, PricingSummaryReportItem>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void reduce(Iterator<Lineitem> lineitems, Collector<PricingSummaryReportItem> out) throws Exception {
				final PricingSummaryReportItem report = new PricingSummaryReportItem();
				Lineitem li = null;
				while (lineitems.hasNext()) {
					li = lineitems.next();

					report.sum_qty += li.l_quantity;
					report.sum_base_price += li.l_extendedprice;
					report.sum_disc_price += li.l_extendedprice * (1 - li.l_discount);
					report.sum_charge += li.l_extendedprice * (1 - li.l_discount) * (1 + li.l_tax);

					report.avg_qty += li.l_quantity;
					report.avg_price += li.l_extendedprice;
					report.avg_disc += li.l_discount;

					report.count_order += 1;
				}

				if (li != null) {
					report.avg_qty /= report.count_order;
					report.avg_price /= report.count_order;
					report.avg_disc /= report.count_order;

					report.l_linestatus = li.l_linestatus;
					report.l_returnflag = li.l_returnflag;

					out.collect(report);
				}
			}
		});

		// print the result and execute
		aggregated.print();
		env.execute();
	}
}
