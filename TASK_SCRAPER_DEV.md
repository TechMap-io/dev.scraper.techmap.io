## Task: Develop a Scraper for a Job Portal with our Scraping System (One Groovy / Java Class with CSS Selectors)

Your task is to develop a scraper for the website [SimplyHired](https://www.simplyhired.com/)
based on our system on [Github](https://github.com/TechMap-io/dev.scraper.techmap.io) - just
copy the scraper for Stepstone and adapt the new Scraper to SimplyHired.

#### Task Notes
* Required Skills: Groovy or Java, CSS (Selectors), Developer Tools (Chrome, etc.)
* Effort per scraper: approx. 4-8 hours
* Potential for 30+ other scrapers / projects - if we like your code & productivity.

#### General Steps:
1. Checkout the Github project
	* `git clone https://github.com/TechMap-io/dev.scraper.techmap.io`
2. Test if the system runs on your machine
	* see README.md
3. Create a (feature) branch for your scraper (just use the website's name, e.g., "SimplyHired")
	* `git checkout -b SimplyHired master`
4. Copy & rename the class StepstoneScraper.groovy to SimplyhiredScraper.groovy
5. Adapt the CSS selectors and Groovy sourcecode to work with the SimplyHired website
6. Test if it scrapes all relevant data (check the Job.groovy, Location.groovy and Company.groovy classes)
7. Final check with 50+ pages from different categories if problems arise.
8. Final cleanup your code
9. Final commit your code / branch and create a pull request
	* `git status` (check your changes)
	* `git add SimplyHired.groovy` (add your Scraper class - there should be no other changes (create an issue or Email if you must change something))
	* `git commit -m "SimplyHired Job Scraper"` (commit your Scraper class)
	* `git push origin SimplyHired` (push your branch to GitHub)
	* Create the [pull request](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request) on GitHub
