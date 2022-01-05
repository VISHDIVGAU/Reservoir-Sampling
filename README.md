# Reservoir-Sampling
Imagine you’re working with a terabyte-scale dataset and you have a MapReduce application you want to test with that dataset. Running your MapReduce application against the dataset may take hours, and constantly iterating with code refinements and rerunning against it isn’t an optimal workflow.

To solve this problem you look to sampling, which is a statistical methodology for extracting a relevant subset of a population. In the context of MapReduce, sampling provides an opportunity to work with large datasets without the overhead of having to wait for the entire dataset to be read and processed.

Reservoir Sampling https://en.wikipedia.org/wiki/Reservoir_sampling

In the reservoir sampling algorithm, you first fill up an array of size K (the reservoir) with the rows being sampled.
Once full, every additional item i (where i > k) can replace an item r in the reservoir by choosing a random number j between 0 and i; If j < k-1, then element i replaces element j.

After all values are seen, the reservoir is the sample (of size K).

Dataset: drive_stats_2019_Q1.zip. (Daily hard disk failure log for the first quarter of 2019.)

Here we have to claculate THE TOTAL NUMBER OF HARD DISK FAILURES PER MODEL FOR THE A K-SAMPLED SUBSET OF THE INPUT DATA
• Sample the dataset using Reservoir Sampling in MapReduce code. The value K can be supplied via parameter (environment variable) or hard coded in your code.
