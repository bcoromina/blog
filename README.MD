
### A tour of Distributed Transactions

<div style="text-align: justify;">
The goal of this post is to explain some patterns to implement distributed transactions. The reasoning that will move us from one to another will hopefully help to have a better understanding of them.
<div>

#### 1. Generic problem description

<div style="text-align: justify;">
In a distributed context, a transaction has to be completed. 

- The transaction involves two distributed elements. i.e. nodes in a cluster, microservices, actors in akka cluster, etc ... 

- Each distributed element holds state

- Transaction has to be executed keeping ACID properties but we will focus in:
    - Consistency: The transaction takes place at once or it doesn't happened at all
    - Isolation: Multiple concurrent transactions occurs independently without interference.

We assume that messages can be sent to distributed elements to perform operations that changes its state. We also assume that, to preserve state integrity, a distributed element will process received message in sequence to avoid concurrent message executions to mutate the state at the same time.

#### 2. An example in Banking

To make it easy to reason about, we will think of distributed elements being bank accounts and a transaction a financial transaction moving funds from one account to another.
Each account will hold its balances as state. Transaction will have a origin account from which money will be withdrawed and a destination account where money will land.

Accounts can not allow their balance to go negative so a transaction will fail if there's not enough funds in origin account. Let's call this a bussiness rule. Transaction can also fail because a bussiness rule is vilolated regarding destination account. Let's say the transaction could fail because of some security checks regarding the destination account.

So we have a transaction to execute involving 2 distributed accounts and the execution can fail in any of them. 



#### 3. Classic Two Phase Commit

The first solution attempt will be by implementing the 2PC pattern.
So there will be a Transaction Coordinator that executes the transaction in two phases: PreCommit, Commit/Revert

In the PreCommit phase the account will check business rules and reserve/adquire resources that will be needed to execute the transaction. So basically, in successful case, the PreCommit will result in a promise to carry out the state modification in the Commit phase, respecting the business rules, of course.

The Transaction Coordinator will send PreCommit messages to both accounts and, if all of them succeeded, it will send Commit message to both. In case of some PreCommit failed, it will send Revert message so the account can release resources reserved in PreCommit.

In our bancking example, origin account will have to reserve funds in PreCommit phase. Lets say the account has 10 dollars, we have to execute a transaction T1 for the value of 4, so we reserve 4 dollars in the PreCommit phase. That will prevent another PreCommit of T2 for 7 dollars to succeed if it's received between T1 Precommit and T1 Commit. In this example, if no reservation is done in the PreCommit, the Commit of T2 would cause a negative balance.

In the picture a TransactionCoordinator implements a 2PC to executing a transaction involving accounts A and B.

```mermaid
sequenceDiagram
    participant T as TrCoord
    note over A: State: A1
    T->> A: 1- PreCommit
    A-->>T: 3-PreCommitOK
     note over A: State:A2

    T ->>B:  2- PreCommit
    B-->>T: 4- PreCommitOK
    T ->> A: 5-Commit
    A-->>T: 6-CommitOK
    note over A: State:A3
    T ->> B: 7-Commit
    B-->>T: 8-CommitOK
```

**Drawback**

Example: A second transaction starts concurrently with a PreCommit to A just after 3-PreCommitOk. This second transaction will see state A2 which will be reverted if 2-PreCommit fails. So this second transaction will have a dirty read! and this would violate the isolation principle.

**A Disapointiong conclusion**: Breaking down the transaction in steps (PreCommitA, PrecommitB, CommitA and CommitB) makes the state of a certain account to be modified gradually allowing dirty reads.
We respect atomicity but we don't have isolation.

**A solution!**

But wait, if executing multiple transaction concurrently is causing dirty reads, why we don't reduce concurrency by letting one distributed element be the transaction coordinator? Let's give it a try!

#### 4. 2PC with Elected Coordinator

So given a transaction involving distributed elements A and B, we are going to elect one of them as the transaction coordinator. This can be done applying some specific strategy if it make sense for the business but in case of banking we are going to always elect origin account as Transaction Coordinator. Let's see how it goes:

```mermaid
sequenceDiagram
    A->>A: Precommit
    A->>A: OK
    A->>B: PreCommit
    B-->>A:  OK
    A->>A: Commit
    note over B: Exposes temporal state
    A->>B: Commit
    B-->>A: OK
```

A couple of things have changed in this new implementation. In Classic 2PC accounts received messages PreCommit, Commit and Reverse but now we add the Transaction itself, as a message that is send to the account that has been elected as the transaction coordinador. 
Remember that the messages for the same account are executed in order to preserve the state's integrity, so while account A is executing this transaction it cannot execute another command like PreCommit from other transaction. So no dirty read in A. 

However, account B could receive a PreCommit command before B's Commit. In that case the readed state after B PreCommit will not be reverted in anycase because A would have sent PreCommit to B only if it's own PreCommit has been succeeded. So in this case B exposes a temporal state that can be seen by another transaction. This is a dirty read but we would only see states of successful transactions, intermediate states of failed transactions would not be exposed. The transaction is not fully isolated but I would say we are in a better situation.  
