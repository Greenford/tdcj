# TDCJ Inmate Scraping
## Background and Motivation
The Texas Department of Criminal Justice is the state's prison agency, and Texas itself is single state in the US with the largest number of prisoners, having more than 150 thousand not counting the federal prisons and county jails. Having volunteered at the Inside Books Project (IBP), a books-to-prison project serving only inmates in Texas, I have a variety of interests in the lives of prisoners. 

TDCJ Makes the individual data of prisoners available for anyone who knows their name or TDCJ ID number. I want to answer questions about the data as a whole. 

With this purpose, I've made a web scraper to capture all of the data on TDCJ inmates, and have done a sample analysis of the data. 

## Hypothesis
Racial discrimination has seemed especially prominent in the criminal justice system. I wanted to see if there was a detectable difference in treatment between the TDCJ population along lines of recorded race. The metric I chose to investigate was sentence length. I chose to separate my investigations between crimes, as I imagine the sentences for murder and other violent heinous crimes wouldn't be comparable to lesser nonviolent ones such as possession of a controlled substance. I chose to limit the scope of my analysis to 5 most common nonviolent crimes. 

Null Hypothesis: The mean sentence date per crime is the same across every race. 

Alternate Hypothesis: The mean sentence date per crime is different for each race. 

## Engineering
### Web Scraper
The web scraper was made using Python and Selenium, and has had instances running on both my personal computer and on Amazon EC2 instances. I had the scrapers query the website by TDCJ number. 

Starting with personal experience from my time at IBP, I believed TDCJ numbers to be assigned consecutively and ascending as the agency received new prisoners. I started with a guess of where possible TDCJ numbers were distributed, scraped random samples from the whole range for a short time, and then took the max TDCJ number and had scrapers spread out from there. The TDCJ numbers I've currently scraped are distributed as so:
(image)
As of January 2019 I've only scraped 107k valid TDCJ numbers and expect at least 50k more numbers to be in the tail. Finding them will be slower than normal, as they are significanly more spread out and being those few who were sentenced to long stays. 

## Exploratory Data Analysis
A histogram of the races below shows that the vast plurality of inmates are either white, hispanic, and black in order of population within TDCJ. 
(image)

A separate grouping by crime was as follows: 

I chose to run analysis on only the white, black, and hispanic populations, the next higest incarcerated being asian and likely not having enough data from while to draw conclusions. This way, I chose to test 3 main hypotheses back to back using the possible 2-combinations of those race categories as the basis: B-W difference, B-H difference, and H-W difference. 

## Data Transforms
I applied two data transforms:
1. Sentences above 100 years were reduced to 100 years. This is because prisoners enduring life sentences could have the numerical sentences recorded as 9999 years, 99 months, and 99 days, and this would skew the means significantly. I chose to reduce it to 100 years because I expect few prisoners to make it past 100 years, sadly, having some sense of the hardship of life in prison and the poor medical care provided. 

2. The next I applied, and here I duplicated tests again, was to map different strings seeming to describe the same crime to the same string. 
(image) 
Tests on the left have not had this mapping applied, while tests on the right have had it. 

## Results

## Future Work
