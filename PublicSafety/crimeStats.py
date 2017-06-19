"""
Statistical analysis tools for crime data.
Code started by Connor Hornibrook, 2017
"""


def combination(n, k):
	""" Code for combination formula i.e. "n choose k" """
	from math import factorial
	return factorial(n) / (factorial(n - k) * factorial(k))


def hypergeometric(N, G, n, x):
	"""
	The hypergeometric distribution formula that was original employed by Robert Cheetham for the Philadelphia
	Police Department. This formula can determine whether or not a given data set is showing "unusually" high
	crime. "N" and "n" represent data pertaining to the entire city, while "G" and "x" pertain to data within a
	sample/test area.

	"Unusually" high areas of crime--or spikes--are based on time and geography. All four input parameters need to
	be derived from date ranges. Essentially, you define an overall date range, and a date range to test for spikes.
	This is demonstrated below with example dates with an overall test period of a year:

		Beginning Overall                                End Overall
		   Date                                           Date / end spike test date
		    |                               Start spike      |
			|                               test date        |
			|_____________________________________|__________|
		11/10/2016                            10/10/2017   11/10/2017


	N is the total number of crimes for the overall test period that occurred in the entire city.
	n is the total number of crimes for the spike test period that occurred in the entire city.
	G is the total number of crimes for the overall test period that occurred in the test area.
	x is the total number of crimes for the spike test period that occurred in the test area.

	The formula uses these four values to generate a p-value, which is used to judge whether or not
	the test area is spiking. Robert Cheetham's original application used a value of .005 as a cutoff point.
	If the p-value was less than .005, then the area was spiking. Likewise, .995 was used as a lower boundary
	for "anti-spikes"; areas of unusually low crime for the test period.

	More information can be found at:  https://irevolutions.org/2009/03/16/crime-mapping-analytics/

	Credit: Robert Cheetham
	"""
	result = 0
	for i in range(0, x + 1):
		result += (combination(G, i) * combination(N - G, n - i)) / combination(N, n)
	return result
