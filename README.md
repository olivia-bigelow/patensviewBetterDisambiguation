# patensviewBetterDisambiguation

This project serves as a means to improve patentsview disambiguation. It has some code for pairing the dataset to Revelio, but for work simply with patentsivew, this section is unnecessary. 

This code improves upon patentsviews disambiguation process, leveraging the many to one nature of the inventor IDs, it places all rows of the persistent IDs in a graph space, and adds edges to any rows that have an equal value for any feature. 

It also reports the best feature in the dataset. 
