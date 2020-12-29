# Introduction

This scraping system is used to extract data from job portals such as stepstone, monster, careerbuilder,
etc. by iterating over categories (or industries, jobnames, etc.), then drilling down the pagination and
finally extracting information from the job ad page.

Your task is to create a new Scraper in Groovy/Java similar to the included class `StepstoneJobsScraper.groovy` (in
the directory `./src/main/io/techmap/scrape/scraper/webscraper/jobscraper`). You mainly need to adapt the CSS
selectors to for the targeted Website (as described in your Task).Try to scrape as many information as
possible from the page by checking the fields of the data classes `Job.groovy`, `Location.groovy`, and `Company.groovy`
(as well as the shared classes in `./src/main/io/techmap/scrape/data/shared`).

To get accustomed to the system, test if the system works on your computer by executing the steps in
"Executing the System" (see below). The system should run out-of-the-box and scrape 5 pages from the
Stepstone website.

## General Process:
1. Copy the class `StepstoneJobsScraper.groovy` and rename appropriately (NOTE: for job scraper the class must end with
`JobsScraper.groovy` while company scraper must end with `CompaniesScraper.groovy` - see `Main.scrapeSource()` that uses a classloader)
2. Go to the website to scrape and find a list of categories, industries, sectors or jobnames the website uses to group
their job ads (favor industries before categories before jobnames)
3. Adapt method `scrape()` in your Class to select the groups (i.e., categories, industries or jobnames)
* NOTE: If the website uses multiple levels to group jobs, loop over them in the `scrape()` method and
collect the data in the Map `extraData` (e.g. workopolis.com uses "Job functions" and then "Job Titles" which should
be stored in `extraData.category` and `extraData.jobname`).
4. Adapt the CSS selectors in the method `scrapePageGroup()` to iterate through the pagination of the
new website. You mainly need to find the number of jobs in this group and find or construct the link
to the next page.
5. In the method `scrapePageList()` you only need to extract the URL and the idInSource (i.e., the Job ID
which should be part of the URL and often looks like a number (e.g., "6691744" for Stepstone), number-letter
mix (e.g., "JDH0GQ605XQT911R5Y0" for Careerbuilder) or maybe UUIDs).
	* NOTE: Please remove parameters in the URL if they are not needed to load the job page (i.e., if
	they do not contain the idInSource).
6. The main part is done in the method `scrapePage()` - you need to understand the job page structure and
create minimal CSS selectors to scrape data such as the job title, description, location or company. Please
try to identify JSON objects in the page's source code and use this data.
	* TIP: use the icognito mode of chrome or another browser to view the page and check via the developer
	tools or the page's source code if the page itself contains the data or if it is created/loaded via
	JavaScript functions. The current test system does not include a headless browser to access this data.
	* NOTE: The most important JSON object is probable the Schema.org Job Posting description (look for
	"application/ld+json" in the page's source code). See https://schema.org/JobPosting for more info.
7. Finally, test your scarper by executing it in your IDE, shell/cli, or via docker (see "Executing the System"
below).
	* NOTE: please check at least 50 pages from different groups / industries to get a feeling for the
	available data on the job ad page. Sometimes job portals have different pages for job ad by own customers,
	scraped job ads or pages with references to PDF documents.

### Please Note:
* No other classes than your Scraper class should be changed or added - if you find a Bug please comment
  and send an Email or Pull Request.
* Some classes and method might not seem to do much - this is due to the reduction from the main system -
  many methods are gutted.

### Problem handling
> TIP: If scraping with **JSoup** is not possible - due to blocking, use of tokens, or heavy use of Javascript (e.g., that loads Jobs as JSON) you can
use a **Selenium** variant via Geb (see gebish.org). An example can be found in the `TotaljobJobsScraper.groovy` class.


## Executing the System
To execute the system you can run it from the shell/cli, an IDE such as IntelliJ IDEA or via a Docker container. The system takes three arguments:
1. The first argument is used to indicate the command "scrape" (in this test system the only other command is "selftest")
2. The second argument is used to indicate the source portal / website, e.g. "Stepstone" (it is used to identify the class - a new scraper needs a new name such as "Careerbuilder")
3. The third argument is used to indicate the number of pages to scrape during development - we recommend that you use an IDE with debugging mode (e.g., IntelliJ IDEA Community) or keep this value low.

### To execute from the shell/cli:
Install gradle (version 6.x), groovy (version 3.x), and Java (version 11.x) before running one of the following commands:
* `gradle run --args=selftest` to test if the dependencies are loaded etc.
The output should look somewhat like this:
```
2020-09-11 14:14:56.616 INFO  Main                       - Program is using Java version 11.0.8
2020-09-11 14:14:56.621 INFO  Main                       - Program is using Groovy version 3.0.4
2020-09-11 14:14:56.622 INFO  Main                       - Program is NOT running in Docker container!
2020-09-11 14:14:56.659 INFO  Main                       - Max memory available (GigaBytes): 4
2020-09-11 14:14:56.660 INFO  Main                       - Total memory in use (GigaBytes):  0.25
2020-09-11 14:14:56.661 INFO  Main                       - Free memory (GigaBytes):          0.201986856758594512939453125
2020-09-11 14:14:56.662 INFO  Main                       - Available processors (cores):     8
2020-09-11 14:14:57.453 INFO  Main                       - Using IP Adress: '94.134.180.250'
2020-09-11 14:14:57.471 INFO  Main                       - Starting with args: [selftest]
2020-09-11 14:14:57.472 INFO  Main                       - Selftest successful: Looks like I'm running OK!
2020-09-11 14:14:57.472 INFO  Main                       - Finnished run with args: [selftest]
```
* `gradle run --args=scrape,StepstoneJobs,2 > techmap_run.log` to scrape pages from a Stepstone website
The output should look somewhat like this:
```
2020-09-11 14:23:37.275 INFO  Main                       - Program is using Java version 11.0.8
2020-09-11 14:23:37.280 INFO  Main                       - Program is using Groovy version 3.0.5
2020-09-11 14:23:37.281 INFO  Main                       - Program is NOT running in Docker container!
2020-09-11 14:23:37.316 INFO  Main                       - Max memory available (GigaBytes): 4
2020-09-11 14:23:37.317 INFO  Main                       - Total memory in use (GigaBytes):  0.25
2020-09-11 14:23:37.318 INFO  Main                       - Free memory (GigaBytes):          0.202434055507183074951171875
2020-09-11 14:23:37.319 INFO  Main                       - Available processors (cores):     8
2020-09-11 14:23:38.054 INFO  Main                       - Using IP Adress: '94.134.180.250'
2020-09-11 14:23:38.082 INFO  Main                       - Starting with args: [scrape, Stepstone, 2]
2020-09-11 14:23:38.134 INFO  StepstoneScraper           - Using userAgent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.123 Safari/537.36
2020-09-11 14:23:38.135 INFO  AWebScraper                - Start of scraping 'https://www.stepstone.nl/en'
{"orgAddress":{"geoPoint":{"lat":52.36669921875,"lng":4.6500000953674},"district":"","street":"","valid":true,"countryCode":"","companyName":"","county":"","quarter":"","country":"Nederland","addressLine":"Haarlem, Nederland", ...
{"orgAddress":{"geoPoint":{"lat":null,"lng":null},"district":"","street":"","valid":true,"countryCode":"nl","companyName":"","county":"","quarter":"","country":"","addressLine":"Waalwijk","state":"","postCode":null,"source":null, ...
2020-09-11 14:23:39.633 INFO  StepstoneScraper           - Scraped     2 of     51 jobs in category Accommodation, Catering & Tourism in 0.918 seconds
2020-09-11 14:23:39.633 INFO  StepstoneScraper           - End of scraping     2 jobs in 1.480 seconds
2020-09-11 14:23:39.633 INFO  Main                       - End of scraping 2 jobs of source stepstone_nl
2020-09-11 14:23:39.634 INFO  Main                       - Finnished run with args: [scrape, Stepstone, 1]
```
> TIP: Please note that the scraped data is printed directly to stdout - to view it in a more readable way
you can switch on `toPrettyString()` in the method `AWebScraper.crossreferenceAndSaveData()` or copy the
lines and use the website http://jsonviewer.stack.hu/

### To execute in IntelliJ IDEA:
Install the plugins for Gradle and Groovy - the IntelliJ IDEA Community Edition is sufficient (you do not need
to have the Ultimate edition). Add a "Gradle Task" to the Execution configuration
(Menu "Run" --> "Edit configurations...") and set the task to `run --args=scrape,StepstoneJobs,2`.
You should be able to run or debug the system from the menu.

### To execute with docker
Install docker on your system and run the following commands in the directory with the Dockerfile:
* `docker build -t tss_test .` to build the docker image
* `docker run tss_test:latest scrape StepstoneJobs 2` to run the docker image
