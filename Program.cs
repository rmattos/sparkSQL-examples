using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using System;
using System.Collections.Generic;
using System.Linq;

namespace sparkSQL_examples
{
    class Program
    {
        static void Main(string[] args)
        {
            var sparkContext = new SparkContext(new SparkConf());

            SqlContext sqlContext = new SqlContext(sparkContext);

            // TXT FILE ****************************************************************************************************

            var customerSchema = new StructType(new List<StructField>
                                                {
                                                    new StructField("id", new StringType(), false),
                                                    new StructField("name", new StringType(), false),
                                                    new StructField("state", new StringType(), false),
                                                    new StructField("city", new StringType(), false),
                                                    new StructField("zip_code", new StringType(), false)
                                                });

            var customerDF = sqlContext.TextFile(@"D:\GitHub\sparkSQL-examples\data\customer.txt", customerSchema);

            customerDF.RegisterTempTable("customers");

            customerDF.ShowSchema();
            customerDF.Show();

            var count = customerDF.Count();

            Console.WriteLine($@"Total of Customers: {count}");

            var resultFiltered = sqlContext.Sql("SELECT * FROM customers WHERE id = 500");

            resultFiltered.ShowSchema();
            resultFiltered.Show();

            var filteredCount = resultFiltered.Count();

            Console.WriteLine($@"Total of Customers Filtered: {filteredCount}");

            Console.ReadKey();


            // JSON FILE ****************************************************************************************************

            var ordersDF = sqlContext.Read().Json(@"D:\GitHub\sparkSQL-examples\data\order.json");

            ordersDF.RegisterTempTable("orders");

            ordersDF.ShowSchema();
            ordersDF.Show();

            var resultList = ordersDF.Collect().ToArray();

            foreach (var order in resultList)
            {
                Console.WriteLine($@"Person Id: {order.Get("personid")}");
                Console.WriteLine($@"Order Id: {order.Get("orderid")}");

                Row orderDetail = order.Get("order");

                Console.WriteLine($@"Quantity: {orderDetail.Get("itemcount")}");
                Console.WriteLine($@"Total Amount: {orderDetail.Get("totalamount")}");

                Console.WriteLine("--------------------");
            }

            Console.ReadKey();
        }
    }
}
