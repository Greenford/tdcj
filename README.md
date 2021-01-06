# Investigating Sentence Length Disparity in the Texas Prison System
## Background and Motivation
The Texas Department of Criminal Justice is the state's prison agency, and Texas itself is single state in the US with the largest number of prisoners, having more than 150 thousand not counting the federal prisons and county jails. Having volunteered at the Inside Books Project* (IBP), a books-to-prison project serving only inmates in Texas, I have a variety of interests in the lives of prisoners. 

TDCJ makes the individual data of prisoners publicly available [publicly available](offender.tdcj.texas.gov/OffenderSearch/). I want to answer questions about the data as a whole. 

With this purpose, I've made a web scraper to capture all of the data on TDCJ inmates, and have done a sample analysis of the data. 

## Hypothesis
Racial discrimination has seemed especially prominent in the criminal justice system. I wanted to see if there was a detectable difference in treatment between the TDCJ population along lines of recorded race. The metric I chose to investigate was sentence length. I chose to separate my investigations between crimes, as I imagine the sentences for murder and other violent heinous crimes wouldn't be comparable to lesser nonviolent ones such as possession of a controlled substance. I chose to limit the scope of my analysis to 5 most common nonviolent crimes. 

Null Hypothesis: The mean sentence date per crime is the same across every race. 

Alternate Hypothesis: The mean sentence date per crime is different for each race. 

The significance level will be a standard 0.05. Because we're testing for 5 different crimes, we apply a Bonferroni correction so that the significance level is 0.01 each.

## Engineering
### Web Scraper
The web scraper was made using Python and Selenium, and has had instances running on both my personal computer and on Amazon EC2 instances. I had the scrapers query the website by TDCJ number. The data was stored in a MongoDB. 

Starting with personal experience from my time at IBP, I believed TDCJ numbers to be assigned consecutively and ascending as the agency received new prisoners. I started with a guess of where possible TDCJ numbers were distributed, scraped random samples from the whole range for a short time, and then took the max TDCJ number and had scrapers spread out from there. The TDCJ numbers I've currently scraped are distributed as so:

![Image of Distribution of TDCJ Numbers](https://github.com/Greenford/tdcj/blob/master/images/TDCJnumdist.png)

As of January 2019 I've only scraped 107k valid TDCJ numbers and expect at least 50k more numbers to be in the tail. Finding them will be slower than normal, as they are significanly more spread out and being those few who were sentenced to long stays. 

### Analysis
Data was then migrated to a PostgreSQL database and then analyzed using a jupyter notbook with Pandas and Scipy. 

## Exploratory Data Analysis
The below histogram of race shows that most inmates are either Black, Hispanic, or White. 

![Image of Histogram of TDCJ Population by Race](https://github.com/Greenford/tdcj/blob/master/images/racedist.png)

A separate grouping by crime was as follows: 

I chose to run analysis on only the white, black, and hispanic populations, the next higest incarcerated being Asian and likely not having enough data from while to draw conclusions. This way, I chose to test 3 main hypotheses back to back using the possible 2-combinations of those race categories as the basis: B-W difference, B-H difference, and H-W difference. 

## Data Transforms
I applied two data transforms:
1. Sentences above 100 years were reduced to 100 years. This is because prisoners enduring life sentences could have the numerical sentences recorded as 9999 years, 99 months, and 99 days, and this would skew the means significantly. I chose to reduce it to 100 years because I expect few prisoners to make it past 100 years, sadly, having some sense of the hardship of life in prison and the poor medical care provided. 

2. The next I applied, and here I duplicated tests again, was to map different strings seeming to describe the same crime to the same string. 

![Image of Example synonym mappings](https://github.com/Greenford/tdcj/blob/master/images/synonyms.png) 
 

## Results
Tests on the left have not had the second data transform applied, while tests on the right have.

We can see that the results on the left are largely statistically significant, although different races are receiving longer sentences for different crimes, which is not in line with the behavior I anticipated, with one race (W) receiving consistently shorter sentences and the others receiving higher ones. 

The results on the right (after the synonym mapping) show no significant difference in sentence lengths. Additionally, all the sentences are in the neighborhood of 7 years, even the crimes that in the left results had much shorter sentences. 

![Image of Burglary of a Habitation Sentence Means](https://github.com/Greenford/tdcj/blob/master/images/Burg-H.png)

![Image of Possession of a Contraband Substance Sentence Means](https://github.com/Greenford/tdcj/blob/master/images/2Poss.png)

See the rest of the images in the [images folder](/images)

## Discussion
It's obvious that the original strings I was testing for are subsets of the family of synonym strings (and the largest subsets at that), but the differences between subsets is unclear. 

Furthermore, the post-synonym-transform results all being about 7 years in mean simply doesn't seem right. Possible confounding factors include:
1. sentences for the same crimes may vary with concomitant crimes offenders are tried for simultaneously.
2. sentences for the same crimes may vary with the number of repeat offenses. 
3. in addition to being semantic indicators, the mapped synonyms may be symbolic indicators for different crimes. 

At this point I'd need to consult a subject matter expert in TDCJ coding to be able to make sense of the findings. 

## Future Work
In terms of investigating sentence lengths for racial discrimination, I can control for whether the inmates have been sentenced for cumulative crimes or sentenced for multiple crimes simulataneously. I can also see if there are differences by county or time period, as the above data covered the whole of Texas and inmates sentenced between 1966 to 2019. 

*[Inside Books Project](insidebooksproject.org)

*Special thanks to Dan Murphy and Jonathan Starr of the Inside Books Project for sharing their knowledge of TDCJ.* 
