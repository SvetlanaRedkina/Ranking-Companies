CREATE TABLE Companies (
	id INT NOT NULL AUTO_INCREMENT,
	RevenueStream VARCHAR(50) NOT NULL,
	Company VARCHAR(50) NOT NULL,
	RevenueBillions INT NOT NULL,
	PRIMARY KEY (id)
	);

INSERT INTO Companies (RevenueStream, Company, RevenueBillions)
VALUES
	('iPhone', 'Apple', 166), 
    ('Google Ads','Alphabet', 20),
	('Subscription Services', 'Amazon', 14),
	('Services','Apple', 37),
	('Physical Stores','Amazon', 17),
	('iPad', 'Apple', 23),
    ('Google Other', 'Alphabet', 20),
	('Web Services', 'Amazon', 25),
	('Mac','Apple', 23),
	('Online Stores', 'Amazon', 123),
	('Other Products', 'Apple', 17),
	('3rd Party Seller Services', 'Amazon', 42),
	('Advertising (Google Properties)', 'Alphabet', 96);

SELECT *
FROM Companies;

/* Task 1: Apply rank() in order to sort the table based on Revenue in descending order.
A note:
Rank() without a partition will assign rank to each row based on RevenueBillions in descending order.
Notice that there is a tie between 'Google Ads' and 'Google Other', and rank() assigns both rows to the same rank 9. 
The next row's ('Physical Stores') rank value is incremented by 2 and is equal to 11. */

SELECT RevenueStream,
	 Company,
	 RevenueBillions,
	 rank() OVER (
		ORDER BY RevenueBillions DESC
		) AS Ranked
FROM Companies;

/* Output:

RevenueStream						 	Company					RevenueBillions		                Ranked

iPhone							 	Apple					166					1
Online Stores        				         	Amazon					123					2
Advertising (Google Properties)				 	Alphabet				96					3
3rd Party Seller Services				 	Amazon					42					4
Services						 	Apple					37					5
Web Services						 	Amazon					25					6
iPad							 	Apple					23					7
Mac							 	Apple					23					7
Google Ads						 	Alphabet				20					9
Google Other					         	Alphabet				20					9
Physical Stores						 	Amazon					17					11
Other Products						 	Apple					17					11
Subscription Services				         	Amazon					14					13 
*/


/* Task 2: Partition the table based on Company, then apply rank() in order to sort each partition based on Revenue in descending order.
A note: 
The value of the rank is reset to 1 every time the query jumps on a new partition. 
As in Task 1, 'Google Ads' and 'Google Other' are assigned to the same rank equal to 2. */

SELECT RevenueStream,
	 Company,
	 RevenueBillions,
	 rank() OVER (
		PARTITION BY Company ORDER BY RevenueBillions DESC
		) AS RankedPartitioned
FROM Companies;

/* Output:

RevenueStream							Company				RevenueBillions			RankedPartitioned

Advertising (Google Properties)					Alphabet			96				1
Google Ads							Alphabet			20				2
Google Other							Alphabet			20				2
Online Stores							Amazon				123				1
3rd Party Seller Services					Amazon				42				2
Web Services							Amazon				25				3
Physical Stores							Amazon				17				4
Subscription Services						Amazon				14				5
iPhone								Apple				166				1
Services							Apple				37				2
iPad								Apple				23				3
Mac								Apple				23				3
Other Products							Apple				17				5
*/


/* Task 3: Apply dense_rank() in order to rank the table based on Companies' Revenues in descending order.
A note:  
Dense_rank() works similarly to rank() but the ranks are assigned in a consecutive order.
For instance, 'Google Ads' and 'Google Other' share the rank 8 but the next row, 'Physical Stores', gets the rank 9 (the value is incremented by 1 rather than 2). */

SELECT RevenueStream,
	 Company,
	 RevenueBillions,
	 dense_rank() OVER (
		ORDER BY RevenueBillions DESC
		) AS DenseRanked
FROM Companies;

/* Output:

RevenueStream							Company				RevenueBillions			DenseRanked

iPhone								Apple				166				1
Online Stores							Amazon				123				2
Advertising (Google Properties)					Alphabet			96				3
3rd Party Seller Services					Amazon				42				4
Services							Apple				37				5
Web Services							Amazon				25				6
iPad								Apple				23				7
Mac								Apple				23				7
Google Ads							Alphabet			20				8
Google Other							Alphabet			20				8
Physical Stores							Amazon				17				9
Other Products							Apple				17				9
Subscription Services						Amazon				14				10
*/


/* Task 4: Partition the table based on Company, then apply dense_rank() in order to sort each partition based on Revenue (descending) in consecutive order.
A note:
Since we use dense_rank(), when there is a tie between values within a partition, the next row's rank is incremented by 1.
For example, 'iPad' and 'Mac' are assigned to rank 3. The next row, Apple's 'Other Products', is ranked 4. */

SELECT RevenueStream,
	 Company,
	 RevenueBillions,
	 dense_rank() OVER (
		PARTITION BY Company ORDER BY RevenueBillions DESC
		) AS DenseRankedPartitioned
FROM Companies;

/* Output:

RevenueStream							Company				RevenueBillions			DenseRankedPartitioned

Advertising (Google Properties)					Alphabet			96				1
Google Ads							Alphabet			20				2
Google Other							Alphabet			20				2
Online Stores							Amazon				123				1
3rd Party Seller Services					Amazon				42				2
Web Services							Amazon				25				3
Physical Stores							Amazon				17				4
Subscription Services						Amazon				14				5
iPhone								Apple				166				1
Services							Apple				37				2
iPad								Apple				23				3
Mac								Apple				23				3
Other Products							Apple				17				4
*/


/* Task 5: Apply row_number() to rank the table based on RevenueBillions in descending order.
A note:
Row_number() generates a sequential number for each row based on RevenueBillions and does not skip any values. */

SELECT RevenueStream,
	 Company,
	 RevenueBillions,
	 row_number() OVER (
		ORDER BY RevenueBillions DESC
		) AS RowNumber
FROM Companies;

/* Output:

RevenueStream							Company				RevenueBillions			RowNUmber

iPhone								Apple				166				1
Online Stores							Amazon				123				2
Advertising (Google Properties)					Alphabet			96				3
3rd Party Seller Services					Amazon				42				4
Services							Apple				37				5
Web Services							Amazon				25				6
iPad								Apple				23				7
Mac								Apple				23				8
Google Ads							Alphabet			20				9
Google Other							Alphabet			20				10
Physical Stores							Amazon				17				11
Other Products							Apple				17				12
Subscription Services						Amazon				14				13
*/


/* Task 6: Using row_number(), partition the table by Company and rank it based on RevenueBillions.
A note:
Row_number's logic is quite different from rank() and dense_rank() when we sort values withtin the partition.
We can see that the row with Amazon's 'Subscription Services' and 14B in revenue is assigned to rank 1. 
After that the rank's value is reset and the row with 'Physical Stores' and 17B in revenue is also assigned to rank 1.*/

SELECT RevenueStream,
	 Company,
	 RevenueBillions,
	 row_number() OVER (
		PARTITION BY RevenueBillions ORDER BY RevenueBillions DESC
		) AS RowNumberPartitioned
FROM Companies;

/* Output:

RevenueStream							Company				RevenueBillions			RowNumberPartitioned
Subscription Services						Amazon				14				1
Physical Stores							Amazon				17				1
Other Products							Apple				17				2
Google Ads							Alphabet			20				1
Google Other							Alphabet			20				2
iPad								Apple				23				1
Mac								Apple				23				2	
Web Services							Amazon				25				1
Services							Apple				37				1
3rd Party Seller Services					Amazon				42				1
Advertising (Google Properties) 				Alphabet			96				1
Online Stores							Amazon				123				1
iPhone								Apple				166				1
*/


/* It is also helpful to know lead() and lag() functions, if one wants to get a preceding or succeeding value within a partition.
Lead() function can be applied when we are dealing with the dates that overlap. The example below helps to illustrate it. 
Let's create a table projectDates and insert some values into the table. */

CREATE TABLE projectDates (
	 Project_id INT NOT NULL AUTO_INCREMENT,
	 Project_name VARCHAR(50) NOT NULL,
	 In_project DATE NOT NULL,
	 Out_project DATE NOT NULL,
	 PRIMARY KEY (Project_id)
	);

SELECT *
FROM projectDates;

INSERT INTO projectDates (Project_name, In_project, Out_project)
VALUES
	 ('Advanced Joins', '2021-04-05', '2021-04-08'),
	 ('Stored Procedures', '2021-04-06', '2021-04-09'),
	 ('ACID Transactions', '2021-04-08', '2021-04-12'),
	 ('Regular Expressions','2021-04-14','2021-04-16');

SELECT *
FROM projectDates;

/* We can see that a student is working on both 'Advanced Joins' and 'Stored Procedures' projects from 2021-04-06 to 2021-04-08.
Let's find out the number of days when both projects are in progress. */

SELECT Project_name,
	 In_project,
	 Out_project,
	 Out_project - lead(In_project) OVER (
		ORDER BY In_project
		) + 1 AS DaysInProjects
FROM projectDates;

/*Output:

Project_name            	In_project		Out_project		DaysInProjects

Advanced Joins			2021-04-05		2021-04-08		3
Stored Procedures		2021-04-06		2021-04-09		2
ACID Transactions		2021-04-08		2021-04-12		-1
Regular Expressions		2021-04-16		2021-04-16		NULL
*/

/* A note about the logic behind the query: 
The student is working on 'Advanced Joins' and 'Stored Procedures' at the same time for 3 days.
If we subtract 2021-04-06 from 2021-04-08, we will get 2, this is why we add 1 to count all 3 days (the 6, 7, and 8th).
Interestingly, if we look at 'Stored Procedures' and 'ACID Transactions' projects, we will see that the value of DaysInProject is -1. 
By the same logic, the query subtracts 2021-04-14 from 2021-04-12 and adds 1 (12 - 14 + 1 = -1). */

/* The next problem explains lag() function and shows how it can be applied in a bit different setting.
First, we create a table and insert values into it. */

CREATE TABLE petCompanies (
	Year INT NOT NULL,
	Company VARCHAR(50) NOT NULL,
	Service VARCHAR(50) NOT NULL,
	Revenue INT NOT NULL
	);

INSERT INTO petCompanies (Year, Company, Service, Revenue)
VALUES
	(2020, 'Cute Paws', 'Mobile Grooming', 43500),
	(2020,'Woof&Meow','Food&Supplies Delivery', 61000),
	(2020,'Pets First', 'Mobile Grooming', 35000),
	(2020, 'Cute Paws', 'Pet Sitting', 31000),
	(2020, 'Woof&Meow', 'Pet Photography', 19000),
	(2020, 'Pets First', 'Pet Consulting', 25000),
	(2019, 'Cute Paws', 'Pet Massage', 31000),
	(2019, 'Woof&Meow', 'Mobile Grooming', 30000),
	(2019, 'Pets First', 'Mobile Grooming', 45000),
	(2019, 'Cute Paws', 'Pet Sitting', 49000),
	(2019, 'Woof&Meow', 'Food&Supplies Delivery', 120000),
	(2019, 'Pets First', 'Pets Consulting', 14500);

SELECT *
FROM petCompanies;

/* Lag() is helpful when we want to compare the values.
First, we partition the table by Year and then order it by Company.
After that, lag function returns the preceeding value of Revenue.
For instance, in 2019 'Cute Paws' has 49,000 in revenue in 'Pet Sitting' service and 31,000 in revenue in 'Pet Massage'.
Lag() returns the preceeding value, 
and if we look at the row with 'Pet Sitting', we will see that the other service the company provides, which is 'Pet Massage', yields smaller revenue. */

SELECT Year,
	 Company,
	 Service,
	 Revenue,
	 lag(Revenue, 1) OVER (
		PARTITION BY Year ORDER BY Company
		) AS LagRevenue
FROM petCompanies;

/* Output:

Year		Company			Service					Revenue			LagRevenue

2019		Cute Paws		Pet Massage				31000			NULL
2019		Cute paws		Pet Sitting				49000			31000
2019		Pets First		Mobile Grooming				45000			49000
2019		Pets First		Pets Consulting				14500			45000
2019		Woof&Meow		Mobile Grooming				30000			14500
2019		Woof&Meow		Food&Supplies Delivery	 		120000			30000
2020		Cute Paws		Mobile Grooming				43500			NULL
2020		Cute Paws		Pet Sitting				31000			43500
2020		Pets First		Mobile Grooming				35000			31000
2020		Pets First		Pet Consulting				25000			35000
2020		Woof&Meow		Food&Supplies Delivery			61000			25000
2020		Woof&Meow		Pet Photography				19000			61000
*/


/* The logic is similar, if we apply lead() to petCompanies table.
The only difference is that lead() is used to get value from row that succeeds the current row.
Going back to 2019 'Cute Paws' example, we can see that the 'Pet Massage' returns 31,000 in revenue.
The succeeding value is equal to 49,00 and comes from 'Pet Sitting' service. */

SELECT Year,
	 Company,
	 Service,
	 Revenue,
	 lead(Revenue, 1) OVER (
		PARTITION BY Year ORDER BY Company
		) AS LeadRevenue
FROM petCompanies;

/* Output:

Year		Company			Service					Revenue			LeadRevenue

2019		Cute Paws		Pet Massage				31000			49000
2019		Cute paws		Pet Sitting				49000			45000
2019		Pets First		Mobile Grooming				45000			14500
2019		Pets First		Pets Consulting				14500			30000
2019		Woof&Meow		Mobile Grooming				30000			120000
2019		Woof&Meow		Food&Supplies Delivery			120000			NULL
2020		Cute Paws		Mobile Grooming				43500			31000
2020		Cute Paws		Pet Sitting				31000			35000
2020		Pets First		Mobile Grooming				35000			25000
2020		Pets First		Pet Consulting				25000			61000
2020		Woof&Meow		Food&Supplies Delivery			61000			19000
2020		Woof&Meow		Pet Photography				19000			NULL
*/
