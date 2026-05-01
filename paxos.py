"""Barrier-based synchronous Paxos implementation.

This file implements the main version project. Each Paxos
node runs as a separate process, communicates through ZeroMQ PUSH/PULL sockets,
and uses a multiprocessing Barrier to keep all nodes aligned at phase boundaries.
"""

import multiprocessing
import random
import threading
import time

import zmq
from multiprocessing import Process


import sys

# Used only to reduce interleaving in selected console prints from different processes.
displayLock = threading.Lock()


def paxosProcess(numProc:int,prob:float,numRounds:int,ownID: int,initialValue: int,barrier):
    """Run one Paxos node for a fixed number of synchronous rounds.

    Args:
        numProc: Total number of Paxos nodes.
        prob: Probability that an outgoing algorithm message is replaced by CRASH.
        numRounds: Number of rounds to execute before terminating.
        ownID: Unique process/node id in the range [0, numProc - 1].
        initialValue: Binary value initially proposed by this node.
        barrier: Multiprocessing barrier used for phase synchronization.
    """
    # Paxos state kept locally by each node. maxVotedRound/maxVotedVal
    # remember the latest round/value this node voted for.
    currentRound = 0
    maxVotedRound = -1
    maxVotedVal = None
    proposeVal = None
    decision = None

    # ZeroMQ setup: every node owns one PULL socket and connects one PUSH
    # socket to every node, including itself. This creates an all-to-all topology.
    context = zmq.Context()

    pushSockets = []
    pullSocket = context.socket(zmq.PULL)
    pullSocket.bind("tcp://127.0.0.1:" + str(5550 + ownID))

    # Create outgoing PUSH sockets. pushSockets[i] sends to node i.
    for process in range(0, numProc): 
        currentAddress = "tcp://127.0.0.1:" + str(5550 + process)
        context = zmq.Context()
        pushSocket = context.socket(zmq.PUSH)
        pushSocket.connect(currentAddress)
        pushSockets.append(pushSocket)

    # Give all processes time to bind/connect their sockets before round 0.
    time.sleep(5)

    # Main Paxos loop. The project uses a fixed number of rounds instead of
    # waiting for guaranteed termination under probabilistic crashes.
    for currentRound in range(0,numRounds):
        # Round-robin leader election: round r is led by node r % numProc.
        leader = True if (currentRound % numProc) == ownID else False

        phase = "JOIN"
        if(leader):
            # Leader/proposer behavior for the current round.
            print("ROUND ",currentRound," STARTED WITH INITIAL VALUE: ",initialValue)

            # Phase 1: broadcast START, unless the leader is simulated as crashed.
            # A crash is represented by sending CRASH <leader_id> instead of START.
            failProb = random.randint(0, 100)
            failProb = failProb / 100
            #print(failProb)
            if (failProb <= prob):
                for socket in pushSockets:
                    socket.send_string("CRASH " +str(currentRound % numProc))

            else:
                for socket in pushSockets:
                    socket.send_string("START")


            # Collect N Phase-1 responses. START counts as the leader successfully
            # receiving its own broadcast; JOIN counts as an acceptor joining.
            ownStartReceived = False
            successMessageCount = 0
            maxRoundMessage = -1
            maxValMessage = -9999
            for i in range(0,numProc):
                recievedMessage = pullSocket.recv_string()
                tokenizedMessage = recievedMessage.split()
                if(tokenizedMessage[0] == "START"):
                    print("LEADER OF ",currentRound," RECEIVED IN ",phase," PHASE: ", recievedMessage)
                    successMessageCount += 1
                    ownStartReceived = True
                elif(tokenizedMessage[0] == "JOIN"):
                    print("LEADER OF ", currentRound, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)
                    successMessageCount += 1
                    currentVotedRoundInfo = tokenizedMessage[1]
                    currentVotedValue = tokenizedMessage[2]
                    if(int(currentVotedRoundInfo) > int(maxRoundMessage)):
                        maxRoundMessage = currentVotedRoundInfo
                        maxValMessage = currentVotedValue
                elif (tokenizedMessage[0] == "CRASH"):
                    print("LEADER OF ", currentRound, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)


            if(successMessageCount > numProc/2):
                # A majority joined the round, so the leader can choose a proposal.
                # Paxos safety requires selecting the value associated with the
                # highest previous voted round in the join quorum, if one exists.
                #NONE OF THE NODES HAVE VOTED BEFORE
                if(ownStartReceived):
                    if(maxRoundMessage == -1):
                        if(maxVotedVal != None):
                            proposeVal = maxVotedVal
                        else:
                            proposeVal = initialValue
                    else:
                        if(maxVotedRound > int(maxRoundMessage)):
                            proposeVal = maxVotedVal
                        else:
                            proposeVal = maxValMessage


                else:

                    if(maxRoundMessage == -1):
                        proposeVal = initialValue
                    else:
                        proposeVal = maxValMessage

                # Phase 2: broadcast the selected proposal, again passing through
                # the probabilistic crash simulation.
                failProb = random.randint(0, 100)
                failProb = failProb / 100
                #print(failProb)
                if (failProb <= prob):
                    for socket in pushSockets:
                        socket.send_string("CRASH " + str(currentRound % numProc))

                else:
                    for socket in pushSockets:
                        socket.send_string("PROPOSE " + str(proposeVal))

                phase = "VOTE"

                # Collect N Phase-2 responses. VOTE and the leader\'s own PROPOSE
                # both count as positive votes.
                voteCount = 0
                for i in range(0, numProc):
                    recievedMessage = pullSocket.recv_string()
                    tokenizedMessage = recievedMessage.split()
                    if (tokenizedMessage[0] == "VOTE"):
                        print("LEADER OF ", currentRound, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)
                        voteCount += 1
                    elif (tokenizedMessage[0] == "PROPOSE"):
                        print("LEADER OF ", currentRound, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)
                        voteCount += 1
                        maxVotedRound = currentRound
                        maxVotedVal = proposeVal

                    elif (tokenizedMessage[0] == "CRASH"):
                        print("LEADER OF ", currentRound, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)

                # A vote majority decides the proposal for this round. All paths
                # wait on the barrier before allowing the next round to start.
                #REACHED TO A DECISION
                if(voteCount > numProc/2):
                    print("LEADER OF ",currentRound," DECIDED ON VALUE ",proposeVal)
                    barrier.wait()

                else:
                    #print("NO VOTE QUORUM FORMED")
                    barrier.wait()
                    doNothing = 0

            # If the leader cannot form a join quorum, it sends ROUNDCHANGE
            # directly, without probabilistic failure, to preserve synchronization.
            #NO JOIN QUORUM FORMED
            else:
                print("LEADER OF ROUND ",currentRound," CHANGED ROUND")
                for index ,socket in enumerate(pushSockets):
                    if(index != ownID):
                        socket.send_string("ROUNDCHANGE")

                barrier.wait()



        # Acceptor behavior for non-leader nodes.
        #NOT THE LEADER OF THE CURRENT ROUND
        else:
            # Phase 1: wait for START or CRASH from the current leader.
            recievedMessage = pullSocket.recv_string()
            displayLock.acquire()
            print("ACCEPTOR ",ownID," RECEIVED IN ",phase," PHASE: ",recievedMessage)
            displayLock.release()
            # Reply to the leader with JOIN and the latest vote information,
            # unless this outgoing response is converted into a simulated crash.
            failProb = random.randint(0, 100)
            failProb = failProb / 100
            #print(failProb)
            if (failProb <= prob):
                pushSockets[currentRound % numProc].send_string("CRASH " +str(currentRound % numProc))

            else:
                pushSockets[currentRound % numProc].send_string("JOIN " + str(maxVotedRound) + " " + str(maxVotedVal))


            # Phase 2: wait for PROPOSE, CRASH, or ROUNDCHANGE from the leader.
            phase = "VOTE"
            recievedMessage = pullSocket.recv_string()
            tokenizedMessage = recievedMessage.split()
            if (tokenizedMessage[0] == "PROPOSE"):
                # Accept the proposed value locally before attempting to send VOTE.
                print("ACCEPTOR ", ownID, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)
                maxVotedRound = currentRound
                maxVotedVal = tokenizedMessage[1]
                failProb = random.randint(0, 100)
                failProb = failProb / 100
                #print(failProb)
                if (failProb <= prob):
                    pushSockets[currentRound % numProc].send_string("CRASH " + str(currentRound % numProc))

                else:
                    pushSockets[currentRound % numProc].send_string("VOTE")

                barrier.wait()
            elif (tokenizedMessage[0] == "CRASH"):
                print("ACCEPTOR ", ownID, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)
                pushSockets[currentRound % numProc].send_string("CRASH " + str(currentRound % numProc))
                barrier.wait()

            elif (tokenizedMessage[0] == "ROUNDCHANGE"):
                print("ACCEPTOR ", ownID, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)
                barrier.wait()



if __name__ == '__main__':
    # Command-line interface expected by the assignment:
    # python paxos.py <numProc> <crashProbability> <numRounds>

    numProc = int(sys.argv[1])
    prob  = float(sys.argv[2])
    numRounds = int(sys.argv[3])

    print("NUM_NODES: ",numProc,", CRASH PROB: ",prob,", NUM_ROUNDS: ",numRounds)

    allProcesses = []

    # Shared barrier used by all Paxos processes to align phase/round progress.
    barrier = multiprocessing.Barrier(numProc)

    # Spawn one OS process per Paxos node. Each node receives a random binary
    # initial value and an id equal to its loop index.
    for process in range(0,numProc):
        currentInitialValue = random.randint(0,1)
        p = Process(target=paxosProcess, args=(numProc,prob,numRounds,process,currentInitialValue,barrier))
        p.start()
        allProcesses.append(p)


    for process in allProcesses:
        process.join()
