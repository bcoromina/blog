
### A tour of Distributed Transactions

<div style="text-align: justify;">
The goal of this post is to explain some patterns to implement distributed transactions. The reasoning that will move us from one to another will hopefully help to have a better understanding of them
<div>

#### 1. Generic problem description

<div style="text-align: justify;">
In a distributed context, a transaction has to be completed. 

- The transaction invloves two distributed elements. i.e. nodes in a cluster, microservices, actors in akka cluster, etc ... 

- Each distributed element holds state

- Transaction has to be executed keeping ACID properties but we will focus in:
    - Consistency: The transaction takes place at once or it doesn't happend at all
    - Isolation: Multiple concurrent transactions occurs independently without interference.

We assume that messages can be sent to distributed elements to perform operations that changes its state. We also assume that, to preserve state integrity, a distributed element will process received message in sequence to avoid two concurrent message executions to mutate the state at the same time.

#### 2. An example in Banking

To make it easy to reason about, we will think of distributed elements being bank accounts and a transaction a financial transaction moving funds from one account to another.
Then each account holds its balances as state. Transaction will have a origin account from which money will be withdrawed and a destination account where money will land.

Accounts can not allow their balance to go negative so a transaction will fail if there's not enough funds in origin account. Let's call this a bussiness rule. Transaction can also fail because a bussiness rule is vilolated regarding destination account. Let's say the transaction could fail because some security checks regarding the destination account.

So we have a transaction to execute involving 2 distributed accounts and the execution can fail in any of them. 

</div>

#### 3. Two phase commit

The first solution attemp will be by implementing the 2PC pattern.
So there will be a Transaction Coordinator that executes the transaction in two phases: PreCommit, Commit/Revert

In the PreCommit phase the account will check business rules and reserve/adquire resources that will be needed to execute the transaction. So basically, in successful case, the PreCommit will result in a promise to carry out the state modification in the Commit phase, respecting the business rules, of course.

The Transaction Coordinator will send PreCommit messages to both accounts and, if all of them succeeded, it will send Commit message to both. In case of some PreCommit failed, it will send Revert message so the account can release resources reserved in PreCommit.

In our bancking example, origin account will have to reserve funds in PreCommit phase. Lets say the account has 10 dollars, we have to execute a transaction T1 for the value of 4, so we reserve 4 dollars in the PreCommit phase. That will prevent another PreCommit of T2 for 7 dollars to succeed if it's received between T1 Precommit and T1 Commit. In this example, if no reservation is done in the PreCommit, the Commit of T2 would cause a negative balance.

In the picture a TransactionCoordinator implements a 2PC to executing a transaction involving accounts A and B.

```mermaid
sequenceDiagram
    TrCoord ->> A: 1- PreCommit
    TrCoord ->>B:  2- PreCommit
    A->>TrCoord: 3-PreCommitOK
    B->>TrCoord: 4- PreCommitOK
    TrCoord ->> A: 5-Commit
    A->>TrCoord: 6-CommitOK
    TrCoord ->> B: 7-Commit
    B->>TrCoord: 8-CommitOK