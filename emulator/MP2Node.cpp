/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;
	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();
	//Add current Node too
	curMemList.push_back(Node(memberNode->addr));

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode

	sort(curMemList.begin(), curMemList.end());
	//Need to check change in memberList
	if(!ring.empty())
	{
		for(int i=0;i<curMemList.size();i++)
		{
			if(curMemList[i].getHashCode()==ring[i].getHashCode())
			{
				//If change found
				change=true;
				break;
			}
		}
	}
	hasMyReplicas=findNodes(memberNode->addr.addr);
	ring = curMemList;
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if(change)
		stabilizationProtocol();
	//Not ring has been stablized
	change=false;
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	 //This request is recieved from client to the coordinator
	 //Cordinator has to find correct replicas and frwd the request
	handleCoordinator(CREATE,key,value);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	 handleCoordinator(READ,key,"");
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	 handleCoordinator(UPDATE,key,value);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	 handleCoordinator(DELETE,key,"");
}


//Handle coordinator functionalities
/*
Following are the functionalities of the handler function
1.Get resp node for the key
2.Construct the message
3.Dispatch the message
4.Make a entry in transactions vector
*/
void MP2Node::handleCoordinator(MessageType msgType,string key,string value)
{
	int id=g_transID++;
	vector<Node> nodes;
	Message msg(id,memberNode->addr,msgType,key,value);
	TransactionHandler.push_back(Transaction(id,msgType,this->par->getcurrtime(),key,value));
	nodes=findNodes(key);
	sendMsg(msg,nodes);
}




/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	return ht->create(key,value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	return ht->update(key,value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;
	//Flag for recording success or failure of the query
	bool flag;
	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		flag=true;//Marked as True intilay
		Message msg(message);
		/*
			* Handle the message types here
 		*/
		switch(msg.type)
		{
			case CREATE:
				flag=createKeyValue(msg.key,msg.value,msg.replica);
				Log_and_Reply(msg,flag);
				break;

			case READ:
				msg.value=readKey(msg.key);
				if (msg.value.empty())
					flag=false;
				Log_and_Reply(msg,flag);
				break;

			case UPDATE:
				flag=updateKeyValue(msg.key,msg.value,msg.replica);
				Log_and_Reply(msg,flag);
				break;

			case DELETE:
				flag=deletekey(msg.key);
				Log_and_Reply(msg,flag);
				break;

			case REPLY:
				AckReply(msg);
				break;

			case READREPLY:
				if (!msg.value.empty())
					msg.success=true;
				AckReply(msg);
				break;

			default:
				break;
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	 HandleTransactionList(10);
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */

void MP2Node::HandleTransactionList(int timeout){
	bool flag;
	vector<int> index;
	for(int i=0;i<TransactionHandler.size();i++)
	{
		flag=true;
		if(TransactionHandler[i].SuccessReplies>=2)
			make_log(TransactionHandler[i],true);
		else if(TransactionHandler[i].NegativeReplies>=2)
			make_log(TransactionHandler[i],false);
		else if(this->par->getcurrtime()-TransactionHandler[i].createdTime>=timeout)
			make_log(TransactionHandler[i],false);
		else
			flag=false;
		if(flag)
			index.push_back(i);
	}
	for(int i=0;i<index.size();i++)
		TransactionHandler.erase(TransactionHandler.begin()+index[i]-i);
	index.clear();
}

void MP2Node::make_log(Transaction msg,bool result){
	bool isCoordinator=true;
	switch(msg.type)
	{
		case CREATE:
			if(result)
				log->logCreateSuccess(&memberNode->addr,isCoordinator,msg.Tid,msg.key,msg.value);
			else
				log->logCreateFail(&memberNode->addr,isCoordinator,msg.Tid,msg.key,msg.value);
			break;

		case READ:
			if(result)
				log->logReadSuccess(&memberNode->addr,isCoordinator,msg.Tid,msg.key,msg.value);
			else
				log->logReadFail(&memberNode->addr,isCoordinator,msg.Tid,msg.key);
			break;

		case UPDATE:
			if(result)
				log->logUpdateSuccess(&memberNode->addr,isCoordinator,msg.Tid,msg.key,msg.value);
			else
				log->logUpdateFail(&memberNode->addr,isCoordinator,msg.Tid,msg.key,msg.value);
			break;

		case DELETE:
			if(result)
				log->logDeleteSuccess(&memberNode->addr,isCoordinator,msg.Tid,msg.key);
			else
				log->logDeleteFail(&memberNode->addr,isCoordinator,msg.Tid,msg.key);
			break;
		}
}

void MP2Node::AckReply(Message msg){
	for(int i=0;i<TransactionHandler.size();i++)
	{
		if(msg.transID==TransactionHandler[i].Tid)
		{
			if(msg.success)
				TransactionHandler[i].SuccessReplies+=1;
			else
				TransactionHandler[i].NegativeReplies+=1;
			if(msg.type==READREPLY)
				TransactionHandler[i].value=msg.value;
			break;
		}
	}
}

void MP2Node::Log_and_Reply(Message msg1,bool result){
	Message msg(msg1);
	Address to_addr(msg.fromAddr);
	msg.fromAddr=memberNode->addr;
	msg.success=result;
	bool isCoordinator=false;
	if(msg.transID!=-1)
	{
	switch(msg.type)
	{
		case CREATE:
			msg.type=REPLY;
			if(result)
				log->logCreateSuccess(&memberNode->addr,isCoordinator,msg.transID,msg.key,msg.value);
			else
				log->logCreateFail(&memberNode->addr,isCoordinator,msg.transID,msg.key,msg.value);
			break;

		case READ:
			msg.type=READREPLY;
			if(result)
				log->logReadSuccess(&memberNode->addr,isCoordinator,msg.transID,msg.key,msg.value);
			else
				log->logReadFail(&memberNode->addr,isCoordinator,msg.transID,msg.key);
			break;

		case UPDATE:
			msg.type=REPLY;
			if(result)
				log->logUpdateSuccess(&memberNode->addr,isCoordinator,msg.transID,msg.key,msg.value);
			else
				log->logUpdateFail(&memberNode->addr,isCoordinator,msg.transID,msg.key,msg.value);
			break;

		case DELETE:
			msg.type=REPLY;
			if(result)
				log->logDeleteSuccess(&memberNode->addr,isCoordinator,msg.transID,msg.key);
			else
				log->logDeleteFail(&memberNode->addr,isCoordinator,msg.transID,msg.key);
			break;
	}
	//Send reply to coordinator
		emulNet->ENsend(&memberNode->addr, &to_addr, msg.toString());
	}
}

vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}


//Create a function for sending msg

void MP2Node::sendMsg(Message msg,vector<Node> nodes){
	//cout<<msg->key<<endl;
	for(int i=0;i<nodes.size();i++)
		emulNet->ENsend(&memberNode->addr, nodes[i].getAddress(), msg.toString());
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */

size_t MP2Node::getprevhash(){
	int index;
	for(int i=0;i<ring.size();i++)
	{
		if(ring[i].getHashCode()==hasMyReplicas[i].getHashCode())
		{
			index=i;
			break;
		}
	}
	if(index==0)
		return ring[ring.size()-1].getHashCode();
	return ring[index-1].getHashCode();
}

void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	 //Stablize the replicas in ring if churn is found
	 //Move the dht to new replicas
	 size_t myhashcode=hasMyReplicas[0].nodeHashCode;

	 vector<Node> NewReplicas=findNodes(memberNode->addr.addr);
	 int index;
	 bool flag=false;
	 HashTable htcopied;
	 map<string, string>::iterator itr;
	 if(hasMyReplicas[1].getHashCode()!=NewReplicas[1].getHashCode())
	 {
		 flag=true;
		 if(hasMyReplicas[2].getHashCode()==NewReplicas[1].getHashCode())
		 	index=1;
		 else
		 {
			if(hasMyReplicas[2].getHashCode()!=NewReplicas[2].getHashCode())
				index=2;
			else
				index=3;
		 }
	 }
	 else if(hasMyReplicas[2].getHashCode()!=NewReplicas[2].getHashCode())
	 {
		 flag=true;
		 index=1;
	 }


	//In case of change in node's hash val;
	if (flag)
	{
		//Change hasMyReplicas variables
		hasMyReplicas=NewReplicas;
		size_t prevhash=getprevhash();
		//Nesscary decls
		MessageType type=CREATE;
		ReplicaType replica=SECONDARY;
		Address frmaddr(memberNode->addr);
		//Transaction ID -1 means no reply or logging is required
		int transID=-1;
		//Vector of nodes to send hashto
		vector<Node> tosend;


		//Switching the type of change in hasMyReplicas
		switch(index)
		{
			//Send a CREATE msg with (key,value) to the respetive nodes, make special TID=-1 so that that query wont be logged
			case 1:
				//If index 1 send HT to NewReplicas[2]
				tosend.push_back(NewReplicas[2]);
				break;
			case 2:
				//If index 2 send HT to all
				tosend.push_back(NewReplicas[1]);
				tosend.push_back(NewReplicas[2]);
				break;
			case 3:
				//If index 3 send HT to NewReplicas[1]
				tosend.push_back(NewReplicas[1]);
				break;
		}//End of switch


		//Iterating through current hashtable
		for (itr = ht->hashTable.begin(); itr != ht->hashTable.end(); ++itr)
		{
			//Getting all the required key to send
      if((hashFunction(itr->first)>prevhash)and(hashFunction(itr->first)<=myhashcode))
			{
				Message msg(transID,frmaddr,type,itr->first,itr->second);
				sendMsg(msg,tosend);
			}//End of if
    }//End of for
		//Log to mention new hasMyReplicas
		log->LOG(hasMyReplicas[0].getAddress(),"Changed replicas");
		log->LOG(hasMyReplicas[1].getAddress(),"Changed replicas");
		log->LOG(hasMyReplicas[2].getAddress(),"Changed replicas");
	}//End of if
}
