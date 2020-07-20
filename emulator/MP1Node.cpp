/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }
		memberNode->heartbeat++;
    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
	 //Extracting header from msg
	 MessageHdr* header=reinterpret_cast<MessageHdr *>(data);
	 //Extracting msgtype from header
	 MsgTypes mtype = header->msgType;
	 //Assigning remaining as payload
	 char* payload=data+sizeof(MessageHdr);
	 //Addr of the sender
	 Address *addr = reinterpret_cast<Address*>(payload);
	 //hbt of the sender
	 long *hb = reinterpret_cast<long*>(payload+sizeof(Address)+1);

	 //Move pointer to desired position
	 payload+=sizeof(Address)+1+sizeof(long)+1;


	 //Switching according to msgtype
	 switch(mtype)
	 {
		 case JOINREQ:
			Reply2Request(addr,hb);
			break;
		 case JOINREP:
		 	 //Joined in the grp
			 memberNode->inGroup=true;
			 //Retrive size of memlist
			 extMemList(payload);
			 break;
		case HEARTBEAT:
			if(!ispresent(addr))
				Mementry(addr,hb);
			else
				keepAlive(addr,hb);
			break;
	 }

}


//Reply to join req
void MP1Node::Reply2Request(Address *addr,long *hb){

	MessageHdr *msg;
	long sze=(long)memberNode->memberList.size();
	int constantadd;
	//Check if already present if not send JOINREP
	if(!ispresent(addr))
		Mementry(addr,hb);


	//Send a accepeted msg to the requestor
	//esc->An escape sequence
	//Message contetents : [MessageHdr,Sender_Addr,esc,Hearbeat,esc,sizeofmemlist,esc,sizeofmemlist*[id,heartbeat]]
	constantadd=sizeof(addr->addr) + sizeof(long)+sizeof(long)+3;
	size_t msgsize = sizeof(MessageHdr)+constantadd+sze*(sizeof(int)+sizeof(long));
	msg = (MessageHdr *) malloc(msgsize * sizeof(char));
	msg->msgType = JOINREP;
	memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
	memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
	memcpy((char *)(msg+1)+1+sizeof(memberNode->addr.addr)+sizeof(long)+1,&sze,sizeof(long));
	//Embed the list
	for(int i=0;i<sze;i++){
		memcpy((char *)(msg+1)+constantadd+i*(sizeof(int)+sizeof(long)),&memberNode->memberList[i].id,sizeof(int));
		memcpy((char *)(msg+1)+constantadd+i*(sizeof(int)+sizeof(long))+sizeof(int),&memberNode->memberList[i].heartbeat,sizeof(long));
	}
	emulNet->ENsend(&memberNode->addr, addr, (char *)msg, msgsize);
}

//Func to handle incomming membership data
void MP1Node::extMemList(char *payload){
	//Decls
	int *id;
	long *hbt;
	//Retrive the length of memlist embeded and move pointer
	long *sze=reinterpret_cast<long*>(payload);
	payload+=sizeof(long)+1;
	//Iterate over data
	for(int i=0;i<*sze;i++){
		//get id
		id=reinterpret_cast<int*>(payload+i*(sizeof(int)+sizeof(long)));
		hbt=reinterpret_cast<long*>(payload+i*(sizeof(int)+sizeof(long))+sizeof(int));
		//Check if existing already
		if(!ispresent(*id))
			//Add to memberlist
			Mementry(*id,hbt);
	}
}

//Func to create entry in membershiplist
void MP1Node::Mementry(Address *addr,long *hb){
	//Get id and port from addr
	//Create a MemberListEntry
	int id;
	short port;
	long timestamp=this->par->getcurrtime();
	memcpy(&id,&addr->addr[0],sizeof(int));
	memcpy(&port,&addr->addr[4],sizeof(short));
	MemberListEntry mem(id,port,*hb,timestamp);

	//Enter that created MemberListEntry to membershiplist
	memberNode->memberList.push_back(mem);

	//Log the new node in the group
	log->logNodeAdd(&memberNode->addr, addr);
}
//Overloaded
void MP1Node::Mementry(int id,long *hb){
	char cid[2];
	//Since port in this simulation is dummy
	short port=0;
	long timestamp=this->par->getcurrtime();
	//Create a MemberListEntry
	MemberListEntry mem(id,port,*hb,timestamp);

	//Enter that created MemberListEntry to membershiplist
	memberNode->memberList.push_back(mem);

	//Make id to addr
	sprintf(cid, "%d", id);
	Address addr(strcat(cid,".0.0.0:0"));
	//Log the new node in the group
	log->logNodeAdd(&memberNode->addr, &addr);
}



//Func to check member presence in memlist
bool MP1Node::ispresent(Address *addr){
	int id;
	memcpy(&id,&addr->addr[0],sizeof(int));
	for(int i=0;i<memberNode->memberList.size();i++){
		if(id==memberNode->memberList[i].getid()){
			return true;
		}
	}
	return false;
}

//Overloaded
bool MP1Node::ispresent(int id){
	for(int i=0;i<memberNode->memberList.size();i++){
		if(id==memberNode->memberList[i].getid()){
			return true;
		}
	}
	return false;
}



//Update for Received heartbeat
void MP1Node::keepAlive(Address *addr,long *hb){
	int id;
	memcpy(&id,&addr->addr[0],sizeof(int));
	for(int i=0;i<memberNode->memberList.size();i++){
		if(id==memberNode->memberList[i].getid()){
			memberNode->memberList[i].setheartbeat(*hb);
			memberNode->memberList[i].settimestamp(this->par->getcurrtime());
		}
	}
}
/*
My note:
Create a sep function which handles new Memberentry in membershiplist[void] done
Create a func which checks for a member if already present in membershiplist[bool] done
Create a func that multicast "heartbeat" msg to all know members
Create a func that handles heartbeat msg
*/


/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
	 	//Send heartbeat2all
		sendbeat();
		removeNode(2);
		//Update list and mark defaulters as absent
    return;
}

//Multicasting heartbeat
void MP1Node::sendbeat(){
	char id[2];
	MessageHdr *msg;
	for(int i=0;i<memberNode->memberList.size();i++){
		//Get the addr to send the msg
		sprintf(id, "%d", memberNode->memberList[i].getid());
		Address addr(strcat(id,".0.0.0"));
		//printAddress(&addr);
		//Construct heartbeat msg
		size_t msgsize = sizeof(MessageHdr) + sizeof(addr.addr) + sizeof(long) + 1;
		msg = (MessageHdr *) malloc(msgsize * sizeof(char));
		msg->msgType = HEARTBEAT;
		memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
		memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
		//Send msg
		emulNet->ENsend(&memberNode->addr, &addr, (char *)msg, msgsize);
	}
}


//Remove probably failed member
void MP1Node::removeNode(int threshold){
	char cid[2];
	for(int i=0;i<memberNode->memberList.size();i++){
		if (this->par->getcurrtime()-memberNode->memberList[i].gettimestamp()>=threshold){
			sprintf(cid, "%d", memberNode->memberList[i].getid());
			Address addr(strcat(cid,".0.0.0:0"));
			memberNode->memberList.erase(memberNode->memberList.begin()+i);
			log->logNodeRemove(&memberNode->addr, &addr);
		}
	}
}
/**
 * FUNCTION NAME: isNullAddress8
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
