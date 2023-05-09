# HashJoins
# KIU Java Course Assignment
TPC-H
This assignment allows you to apply a variety of basic concepts you have learnt in the course of this semester.

TPC-H is one of the most important benchmarks in the area of databases. All large database systems try to optimize their performance for this benchmark. In order to compare how efficient a database processes queries, it makes sense to program queries by hand in order to determine what a database system might achieve if it incurred no overhead.

The goal of this assignment is to read in three files, parse their contents and join them in order to answer simple queries to the given data set as efficiently as possible.

The queries which your program is meant to answer are as follows. What is average quantity of line items of a customer's order belonging to a particular market segment. The signature of the corresponding method is given by public long getAverageQuantityPerMarketSegment(String marketsegment).

Subsequently, your approach is detailed so that no further database background is required.

Reading in Data
The template provides you with classes Customer, LineItem and Order representign the corresponding database tables. The class Database with your implementation has already the static attribute private static Path baseDataDirectory together with a setter method to change its value for tests.

Your first task is to read in the three included *.tbl files form the baseDataDirectory. Your implemented methods should look as follows:

public static Stream<LineItem> processInputFileLineItem(), public static Stream<Customer> processInputFileCustomer() and public static Stream<Order> processInputFileOrder(). .tbl files are .csv files which instead of , use | for separating values.

The Quantity of Lineitems you should parse with Integer.parseInt(str) * 100 in order to subsequently operate on int values only.

HashJoins
In order to realize Joins of tables, you should use the classes of the interface Map in Java. First, you should map for each Customer, custKey -> marketSegment. Then, you should map for each Order, orderkey -> marketSegment by using custkey. Now, you should use this second Map for iterating over all LineItems in order to determine to which marketSegment it belongs.

By this approach, you should then be able to determine the average quantity of LineItems für a given marketSegment.

Implementation
Implement the method public long getAverageQuantityPerMarketSegment(String marketsegment) of the class Database which as parameter receives a String marketsegment and returnd the average quantity of LineItem per order in marketsegment. During computation of the average, store the number as well as the total sum as long. Only at the very end, compute the quotient of these two values and return it again as long value (integer division). You are permitted to introduce further auxiliary methods for this class.
