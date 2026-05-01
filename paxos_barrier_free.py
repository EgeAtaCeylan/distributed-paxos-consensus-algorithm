"""Barrier-free synchronous Paxos implementation.

This file implements the barrier free version of the project. It uses the same
process-per-node and ZeroMQ PUSH/PULL networking model as paxos.py, but it does
not use a multiprocessing Barrier. Instead, it handles early or late messages
from adjacent rounds with explicit state flags.
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

def paxosProcess(numProc:int,prob:float,numRounds:int,ownID: int,initialValue: int):
    """Run one Paxos node without a barrier-based phase synchronizer.

    Instead of waiting at a barrier after each phase, this implementation records
    next-round messages that arrive early and answers them when the local node
    reaches that round.
    """

    # Paxos state kept locally by each node. maxVotedRound/maxVotedVal
    # remember the latest round/value this node voted for.
    currentRound = 0
    maxVotedRound = -1
    maxVotedVal = None
    proposeVal = None
    decision = None

    # ZeroMQ all-to-all setup: each node binds one PULL socket and connects
    # one PUSH socket to every node, including itself.
    context = zmq.Context()

    pushSockets = []
    pullSocket = context.socket(zmq.PULL)
    pullSocket.bind("tcp://127.0.0.1:" + str(5550 + ownID))
    # These flags are the core of the barrier-free design. They remember
    # next-round messages that arrived while this node was still processing the
    # current round, so the node can answer correctly when it advances.
    leaderRecievedNextRoundsCrash = False
    leaderRecievedNextRoundsStart = False
    acceptorRecievedNextRoundCrash = False
    acceptorRecievedNextRoundStart = False

    # Create outgoing PUSH sockets. pushSockets[i] sends to node i.
    for process in range(0, numProc):
        currentAddress = "tcp://127.0.0.1:" + str(5550 + process)
        context = zmq.Context()
        pushSocket = context.socket(zmq.PUSH)
        pushSocket.connect(currentAddress)
        pushSockets.append(pushSocket)

    # Give all processes time to bind/connect their sockets before round 0.
    time.sleep(5)

    # Main Paxos loop. Unlike paxos.py, there is no barrier wait in this loop.
    for currentRound in range(0,numRounds):
        # Round-robin leader election: round r is led by node r % numProc.
        leader = True if (currentRound % numProc) == ownID else False

        phase = "JOIN"
        if(leader):
            # Leader/proposer behavior for the current round.
            print("ROUND ",currentRound," STARTED WITH INITIAL VALUE: ",initialValue)

            # Phase 1: broadcast START unless this send is replaced by a
            # simulated CRASH message according to the input probability.
            failProb = random.randint(0, 100)
            failProb = failProb / 100
            #print(failProb)
            if (failProb <= prob):
                for socket in pushSockets:

                    socket.send_string("CRASH " +str(currentRound % numProc))

            else:
                for socket in pushSockets:
                    socket.send_string("START")


            # Collect Phase-1 messages for the current round.
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
                # A majority joined the round, so choose the safest proposal:
                # the value with the highest previous vote, or this node\'s
                # initial value if no prior vote exists in the quorum.
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

                # Phase 2: broadcast PROPOSE or simulate a leader crash.
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

                # Without a barrier, the leader can receive messages from the next
                # round while still collecting votes for the current round.
                # correctMessageCount only advances for current-round responses;
                # early next-round messages are remembered in flags and ignored for
                # the current vote quorum.
                correctMessageCount = 0
                voteCount = 0
                while(correctMessageCount != numProc):
                    recievedMessage = pullSocket.recv_string()
                    tokenizedMessage = recievedMessage.split()
                    if (tokenizedMessage[0] == "VOTE"):
                        print("LEADER OF ", currentRound, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)
                        voteCount += 1
                        correctMessageCount += 1
                    elif (tokenizedMessage[0] == "PROPOSE"):
                        print("LEADER OF ", currentRound, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)
                        correctMessageCount += 1
                        voteCount += 1
                        maxVotedRound = currentRound
                        maxVotedVal = proposeVal
                        #print("LEADER OF ", currentRound, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)



                    elif (tokenizedMessage[0] == "CRASH"):
                        crashedRound = tokenizedMessage[1]
                        # A next-round CRASH is recorded so this node can answer
                        # appropriately when it later enters that round.
                        if (crashedRound == currentRound + 1):
                            leaderRecievedNextRoundsCrash = True
                            print("LEADER OF " + str(
                                currentRound ) + " RECEIVED IN " + phase + " PHASE: " + recievedMessage)
                        else:
                            correctMessageCount += 1
                            print("LEADER OF ", currentRound, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)


                    elif (tokenizedMessage[0] == "START"):
                        # Early START from the next round: save it instead of
                        # counting it as a vote response for this round.
                        leaderRecievedNextRoundsStart = True
                        print("LEADER OF " + str(
                            currentRound ) + " RECEIVED IN " + phase + " PHASE: " + recievedMessage)



                #REACHED TO A DECISION
                if(voteCount > numProc/2):
                    print("LEADER OF ",currentRound," DECIDED ON VALUE ",proposeVal)
                    #barrier.wait()

                else:
                    #print("NO VOTE QUORUM FORMED")
                    doNothing = 1

            # No join quorum: send ROUNDCHANGE directly. There is no barrier here;
            # other nodes must handle the message based on their local phase.
            #NO JOIN QUORUM FORMED
            else:
                print("LEADER OF ROUND ",currentRound," CHANGED ROUND")
                for index ,socket in enumerate(pushSockets):
                    if(index != ownID):
                        socket.send_string("ROUNDCHANGE")





        # Acceptor behavior. The first part resolves any early next-round message
        # remembered during the previous iteration.
        #NOT THE LEADER OF THE CURRENT ROUND
        else:
            if (acceptorRecievedNextRoundCrash):

                pushSockets[currentRound % numProc].send_string("CRASH " + str(currentRound % numProc))
                acceptorRecievedNextRoundCrash = False
            if(leaderRecievedNextRoundsStart):
                leaderRecievedNextRoundsStart = False

                failProb = random.randint(0, 100)
                failProb = failProb / 100
                # print(failProb)
                if (failProb <= prob):
                    pushSockets[currentRound % numProc].send_string("CRASH " + str(currentRound % numProc))

                else:
                    pushSockets[currentRound % numProc].send_string(
                        "JOIN " + str(maxVotedRound) + " " + str(maxVotedVal))
            elif(acceptorRecievedNextRoundStart):
                acceptorRecievedNextRoundStart = False

                failProb = random.randint(0, 100)
                failProb = failProb / 100
                # print(failProb)
                if (failProb <= prob):
                    pushSockets[currentRound % numProc].send_string("CRASH " + str(currentRound % numProc))

                else:
                    pushSockets[currentRound % numProc].send_string(
                        "JOIN " + str(maxVotedRound) + " " + str(maxVotedVal))

            elif(leaderRecievedNextRoundsCrash):
                pushSockets[currentRound % numProc].send_string("CRASH " + str(currentRound % numProc))
                leaderRecievedNextRoundsCrash = False

            else:
                # No saved early message: block for the normal Phase-1 message.
                recievedMessage = pullSocket.recv_string()
                tokenizedMessage = recievedMessage.split()
                displayLock.acquire()
                print("ACCEPTOR ",ownID," RECEIVED IN ",phase," PHASE: ",recievedMessage)
                displayLock.release()
                # Message classification replaces the missing barrier: the acceptor
                # decides whether the message belongs to the current round, the
                # previous round, or a future round and replies accordingly.
                #START OR CRASH RECIEVED FOR THE NEXT ROUND
                #CHECK IF THE RECIEVED MESSAGE IS CRASH MESSAGE
                if(tokenizedMessage[0] == "CRASH"):
                    # CHECK IF THE RECIEVED CRASH MESSAGE IS FROM THE CURRENT ROUND

                    if(int(tokenizedMessage[1]) == (currentRound % numProc)):


                        failProb = random.randint(0, 100)
                        failProb = failProb / 100
                        # print(failProb)
                        if (failProb <= prob):
                            pushSockets[currentRound % numProc].send_string("CRASH " + str(currentRound % numProc))


                        else:
                            pushSockets[currentRound % numProc].send_string(
                                "JOIN " + str(maxVotedRound) + " " + str(maxVotedVal))



                    #CHECK IF THE RECIEVED CRASH MESSAGE IS FROM THE PREVIOUS ROUND
                    elif(int(tokenizedMessage[1]) == (currentRound -1 ) % numProc):

                        failProb = random.randint(0, 100)
                        failProb = failProb / 100

                        if (failProb <= prob):
                            pushSockets[(currentRound-1) % numProc].send_string("CRASH " + str(currentRound % numProc))

                        else:
                            pushSockets[(currentRound -1) % numProc].send_string(
                                "JOIN " + str(maxVotedRound) + " " + str(maxVotedVal))

                #ASSUME PROPOSE IS FROM THE ROUND BEFORE
                elif(tokenizedMessage[0] == "PROPOSE"):
                    maxVotedRound = currentRound -1
                    maxVotedVal = tokenizedMessage[1]
                    failProb = random.randint(0, 100)
                    failProb = failProb / 100

                    if (failProb <= prob):
                        pushSockets[(currentRound-1) % numProc].send_string("CRASH " + str(currentRound % numProc))

                    else:
                        pushSockets[(currentRound-1)% numProc].send_string("VOTE")

                # ASSUME START OF THE CURRENT ROUND
                elif (tokenizedMessage[0] == "START"):
                    failProb = random.randint(0, 100)
                    failProb = failProb / 100

                    if (failProb <= prob):
                        pushSockets[(currentRound) % numProc].send_string("CRASH " + str(currentRound % numProc))

                    else:
                        pushSockets[(currentRound) % numProc].send_string(
                            "JOIN " + str(maxVotedRound) + " " + str(maxVotedVal))


                elif(tokenizedMessage[0] == "ROUNDCHANGE"):
                    print("ACCEPTOR ", ownID, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)




            # Phase 2 handling. This may also receive early START/CRASH messages
            # from the next round, which are stored as flags.
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

                if (failProb <= prob):
                    pushSockets[currentRound % numProc].send_string("CRASH " + str(currentRound % numProc))

                else:
                    pushSockets[currentRound % numProc].send_string("VOTE")


            elif (tokenizedMessage[0] == "CRASH"):
                print("ACCEPTOR ", ownID, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)
                if (tokenizedMessage[1] == currentRound):

                    pushSockets[currentRound % numProc].send_string("CRASH " + str(currentRound % numProc))

                else:
                    acceptorRecievedNextRoundCrash = True

            elif (tokenizedMessage[0] == "ROUNDCHANGE"):
                print("ACCEPTOR ", ownID, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)


            elif (tokenizedMessage[0] == "START"):
                acceptorRecievedNextRoundStart = True
                print("ACCEPTOR ", ownID, " RECEIVED IN ", phase, " PHASE: ", recievedMessage)

if __name__ == '__main__':
    # Command-line interface expected by the assignment:
    # python paxos_barrier_free.py <numProc> <crashProbability> <numRounds>

    numProc = int(sys.argv[1])
    prob  = float(sys.argv[2])
    numRounds = int(sys.argv[3])

    print("NUM_NODES: ", numProc, ", CRASH PROB: ", prob, ", NUM_ROUNDS: ", numRounds)


    allProcesses = []

    # Spawn one OS process per Paxos node. Each node receives a random binary
    # initial value and an id equal to its loop index.
    for process in range(0,numProc):
        currentInitialValue = random.randint(0,1)
        p = Process(target=paxosProcess, args=(numProc,prob,numRounds,process,currentInitialValue))
        p.start()
        allProcesses.append(p)


    for process in allProcesses:
        process.join()
