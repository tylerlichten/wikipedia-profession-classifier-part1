# wikipedia-profession-classifier-part1
###Pre-processing of Wikipedia Articles for Naive Bayesian Profession Classification

**School:** Brandeis University  
**Course:** COSI 129a: Introduction to Big Data Analysis  
**Professors:** Marcus Verhagen, James Pustejovsky, Pengyu Hong, Liuba Shrira  
**Head TA:** Tuan Do  
**Semester:** Fall 2016  

**Team Members:** Nikolaos Tsikoudis, Dimokritos Stamatakis, Tyler Lichten, William Edgecomb 

**Description**: Our task was to write a series of Hadoop MapReduce jobs to process a 46 GB Wikipedia dump file using one of Brandeis University's multi-machine clusters. The input file contains Wikipedia files in raw XML, and we filtered the file for those articles that match a name in the list of names contained in resource file people.txt. Then we constructed an articles-to-lemmas Sequence File in which the title of each article is associated with a list of \<lemma, count\> pairs such that each lemma appears count times within its associated article. In the process each article is cleaned of xml formatting, tokenized, lemmatized, and removed of stopwords. Then from the articles-to-lemmas index, we construct a lemmas-to-articles Sequence File in which each lemma is associated with a list of \<article, count\> pairs such that each article contains count instances of the associated lemma. The first ouputted file, the articles-to-lemma index, is then used in part 2 of the assignment (https://github.com/wedgec/wikipedia-profession-classifier-part2), where we train and evaluate a naive Bayesian model for classifying professions based on a Wikipedia article's content. For a more complete description and discussion of work for part 1, please refer to our report PDF. See also the assignment instructions. 
