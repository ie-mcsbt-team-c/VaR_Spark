# VaR_Spark
Value at Risk Calculation using Spark and Python 

# Introduction

VaR definition: Loss with a given probability in a given period.

For instance, “The VaR of my portfolio has a 1-week 5% of 1.000.000€” means that the
probability of losing more than 1M€ is 5%. In other words, one out of 20 days I will lose
1.000.000€ or more.

Methods for calculation:
• Parametric calculation
• Historical simulation
• Montecarlo

We will use the Montecarlo method, which naturally is the “best” of theese three
methods.

Goal of this assignment: Calculate the VaR with a time horizon of one week (5 work
days) with a probability of 5% for a portfolio of instruments contained in symbols.txt

# What to deliver

• The names of the team members

• The programs used to complete the exercise. 

The programs should be selfdocumented. The quality of the documentation is an important factor in the
evaluation.


• A paper explaining your approach. It must contain:

  o The general approach
	
  o The method used to align the data coming from different sources (remember
that all data must be available for the same dates) and the method used to fill
the empty values.

  o We have assumed that the returns of factors follow a normal distribution.
Show graphically that this is the case.

o We have assumed that a linear model with “factorized” factors can represent
all the instruments. Show how good is this assumption and discuss how can it
be improved

  o The approach to parallelize with Spark
