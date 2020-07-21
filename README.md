<h1>Distributed key-value store</h1>
<br>
It is an implementation of a distributed key-value store in an emulated network layer. For more details, look to MP2-specification-document.pdf. 
<hr>
<b>DKVS</b> is a structured data model that allow "AP" data storage/retrieval[Availability and Partition]. DKVS provided more scalability and fault tolerance to the storage system. 
<br><br>
In the above implementation,
The storage systems placed in a virtual ring based on their hash value of respective addresses. Each node stores (key,value) pairs whose hash(key) is in the range (hash(prev_node),hash(current_node)]. The replication factor is 3 for the above ring. So a pair is replicated at owner node* and two immediate successors to the owner node. The query is made consistent with the support of the quorum of replies. So, Read/Write consistency is 2. The membership in each node is maintained through all-all heart beating.

<br><br>
*Owner node -> The node to which hash(key) maps to.<br>
<i>Credits : Programming assignment from cloud computing concepts part-2 in coursera.</i>
