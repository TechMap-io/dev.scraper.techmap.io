# Introduction to this Scraping system

This scraping system is used to extract data from job portals such as stepstone, monster, ...
by iterating over categories (or industries, jobnames, etc.) and then drilling down the pagination. It mainly uses JSoup
to access the pages via CSS selectors.

Your task is to create a new Scraper Groovy class similar to the class `StepstoneScraper.groovy` 
in the directory `./src/main/io/techmap/scrape/scraper/webscraper` for the given website. Try to scrape as many
information as possible from a page by checking the data classes `Job.groovy`, `Location.groovy`, and `Company.groovy` 
(as well as the shared classes in `./src/main/io/techmap/scrape/data/shared`).

##### Please Note:
* It is best if you just copy the class `StepstoneScraper.groovy` and adapt the CSS selectors to the new website.
* No other classes should be changed or added - if you find a Bug please comment and send an Email or Pull Request.
* Some Classes and method might not seem to do much - this is due to the reduction from the main system - many methods are gutted.

## Executing the System
To execute the system you can run it from the shell/cli, an IDE such as IntelliJ IDEA or via a Docker container. The system takes three arguments:
1. The first is used to indicate the command "scrape" (in this test system the only other command is "selftest")
2. The second is used to indicate the source portal / website, e.g. "Stepstone" (it is used to identify the class - a new scraper needs a new name such as "careerbuilder") 
3. The third is used to indicate the number of pages to scrape during development you should use an IDE with debugging (e.g., IntelliJ CE) or keep the value low.

#### To execute from the cli /shell:
Install gradle (version 6.x) and run the commands:
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
* `gradle run --args=scrape,Stepstone,2 > techmap_run.log` to scrape pages from a Stepstone website
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

#### To execute in IntelliJ IDEA:
Add a "Gradle Task" to the Execution configuration(Menu "Run" --> "Edit configurations...") and set the task to `run --args=scrape,Stepstone,2`

#### To execute with docker
TBD ...
