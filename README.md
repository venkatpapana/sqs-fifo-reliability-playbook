# SQS FIFO Reliability Playbook

Runnable examples and notes for exploring SQS FIFO reliability patterns across Node.js and Java.

This repo pairs with the Medium article **"We Moved to SQS FIFO and Found the Real Failure Modes"** and keeps the code examples, runbooks, and screenshots aligned across both languages.

## Published Article

- Level Up Coding: [We Moved to SQS FIFO and Found the Real Failure Modes](https://levelup.gitconnected.com/we-moved-to-sqs-fifo-and-found-the-real-failure-modes-39cc20bb67f0)

## What’s inside

- `nodejs-examples/` - Node.js scripts for local setup, producer, consumer, visibility heartbeat, end-to-end harness, and purge.
- `java-examples/` - Java 17 examples for the same FIFO scenarios and verification flow.

Each folder has its own README with language-specific commands and output expectations.

## Recommended flow

1. Set up LocalStack and the demo queues from `nodejs-examples/`.
2. Run the Node.js producer and consumer examples to capture the main FIFO ordering and dedup screenshots.
3. Run the visibility heartbeat example to show how long-running work keeps a message in-flight safely.
4. Run the end-to-end harness to verify ordering and duplicate suppression.
5. Run the matching Java examples if you want to compare output format and behavior side by side.

## Quick start

### Local setup

From `nodejs-examples/`:

```bash
npm run local:setup
```

If you want to reset queue state before a new demo run:

```bash
npm run purge
```

### Node.js examples

See [`nodejs-examples/README.md`](nodejs-examples/README.md) for the full list of commands and screenshot guidance.

### Java examples

See [`java-examples/README.md`](java-examples/README.md) for the Java 17 commands and the shared harness entry point.


## Notes

- The examples are designed for LocalStack or AWS SQS.
- The harness and output formatting are kept intentionally consistent between Node.js and Java so the blog can show a single story with two implementations.
- Queue reset and local bootstrap scripts live in the Node.js folder because they are shared across both language examples.

## Key Concepts Visualized


### FIFO vs Standard

This diagram contrasts Standard and FIFO queues so the article can explain why FIFO changes ordering and dedup behavior.
Look at the producer fan-out on the left and the delivery guarantees on the right.
Use it to explain why FIFO is a different reliability contract, not just a different queue type.

```mermaid
flowchart LR
		P[Producer] --> Std[SQS Standard]
		P --> Fifo[SQS FIFO]

		Std --> S1[Consumer A]
		Std --> S2[Consumer B]
		Fifo --> G1[Ordered by MessageGroupId]

		S1 --> StdOut[At-least-once, best-effort order]
		S2 --> StdOut
		G1 --> FifoOut[Per-group strict order + dedup window]

        classDef appStyle fill:#0F172A,stroke:#334155,stroke-width:2px,color:#F8FAFC;
        classDef hubStyle fill:#1E40AF,stroke:#1D4ED8,stroke-width:2px,color:#FFFFFF;
        classDef destStyle fill:#0F766E,stroke:#115E59,stroke-width:1.5px,color:#FFFFFF;

        class P,S1,S2 appStyle;
        class Std,Fifo,G1 hubStyle;
        class StdOut,FifoOut destStyle;
```

### Visibility timeout lifecycle

This diagram shows how a message becomes invisible during processing, then either gets deleted on success or redelivered when the visibility lease expires.
The key point is that visibility is a lease, not a delay.
Use it to explain why long-running work needs heartbeat-style visibility extension.

```mermaid
sequenceDiagram
		participant Q as SQS FIFO Queue
		participant C as Consumer

		C->>Q: Receive M1
		Q-->>C: M1 + ReceiptHandle
		Note over Q: M1 invisible for VisibilityTimeout

		alt success path
			C->>Q: Delete M1
			Note over Q: M1 removed permanently
		else timeout/failure path
			Note over Q: Visibility timeout expires
			Q-->>C: M1 redelivered
		end
```

### Long polling behavior

This diagram shows why a 20-second long poll reduces empty receives and improves burst pickup compared with short polling.
The tradeoff is fewer empty responses versus slightly longer waits for messages.
Use it to explain why long polling lowers noise and improves efficiency in demo runs.

```mermaid
flowchart TB
		Start[ReceiveMessage Call] --> Poll{WaitTimeSeconds}
		Poll -->|0 or short| Short[Short Poll]
		Poll -->|up to 20| Long[Long Poll]

		Short --> EmptyHigh[More empty responses]
		Short --> CostHigh[Higher API churn]

		Long --> EmptyLow[Fewer empty responses]
		Long --> CostLow[Lower polling overhead]
		Long --> BurstBetter[Better burst pickup]

        classDef appStyle fill:#0F172A,stroke:#334155,stroke-width:2px,color:#F8FAFC;
        classDef hubStyle fill:#1E40AF,stroke:#1D4ED8,stroke-width:2px,color:#FFFFFF;
        classDef destStyle fill:#0F766E,stroke:#115E59,stroke-width:1.5px,color:#FFFFFF;

        class Start,Poll appStyle;
        class Short,Long hubStyle;
        class EmptyHigh,CostHigh,EmptyLow,CostLow,BurstBetter destStyle;

```

### Message group ordering

This diagram shows that ordering is strict within a single `MessageGroupId`, while different groups can process in parallel.
Notice that one stream is serialized while another can advance independently.
Use it to explain how FIFO preserves business ordering without forcing global serialization.

```mermaid
flowchart LR
		subgraph GroupA[MessageGroupId: customer1#A100]
			A1[M1] --> A2[M2] --> A3[M3]
		end

		subgraph GroupB[MessageGroupId: customer1#B200]
			B1[M1] --> B2[M2]
		end

		A1 --> CA[Consumer Invoke 1]
		B1 --> CB[Consumer Invoke 2]

		CA --> A2
		CB --> B2

		Note1[Ordering is strict within each group]
		Note2[Different groups can run in parallel]

		A3 --> Note1
		B2 --> Note2

        classDef appStyle fill:#0F172A,stroke:#334155,stroke-width:2px,color:#F8FAFC;
        classDef hubStyle fill:#1E40AF,stroke:#1D4ED8,stroke-width:2px,color:#FFFFFF;
        classDef destStyle fill:#0F766E,stroke:#115E59,stroke-width:1.5px,color:#FFFFFF;

        class GroupA,GroupB,CA,CB hubStyle;
        class Note1,Note2 destStyle;        
```

### Dedup window

This diagram shows the fixed five-minute dedup window where repeated dedup IDs are suppressed before becoming eligible again.
The important detail is that duplicates are only blocked for a time window, not forever.
Use it to explain why a stable entity ID can suppress valid retries or rapid updates.

```mermaid
timeline
		title FIFO Deduplication Window (fixed 5 minutes)

		t0 : Send event E1 with dedupId D
		t0+30s : Send same dedupId D
					 : Suppressed (not delivered)
		t0+4m : Send same dedupId D
					: Suppressed (not delivered)
		t0+6m : Send same dedupId D
					: Eligible for delivery again
```

### Lambda partial batch retry

This diagram shows how Lambda partial batch failure lets SQS retry only the failed record instead of replaying the whole batch.
The point is to avoid reprocessing successful records just because one record failed.
Use it to explain why batch failure handling is a reliability feature, not just an implementation detail.

```mermaid
flowchart TD
		Batch[Lambda receives batch of 5] --> Proc[Process records one by one]
		Proc --> R1[1 success]
		Proc --> R2[2 success]
		Proc --> R3[3 failure]
		Proc --> R4[4 success]
		Proc --> R5[5 success]

		R3 --> Resp[Return batchItemFailures = record 3]
		Resp --> Retry[SQS retries only failed record]

        classDef appStyle fill:#0F172A,stroke:#334155,stroke-width:2px,color:#F8FAFC;
        classDef hubStyle fill:#1E40AF,stroke:#1D4ED8,stroke-width:2px,color:#FFFFFF;
        classDef destStyle fill:#0F766E,stroke:#115E59,stroke-width:1.5px,color:#FFFFFF;

        class Batch,Proc,Resp hubStyle;
        class R1,R2,R3,R4,R5,Retry destStyle;        
```

### Throughput tuning with batch size and concurrency

This diagram shows the tradeoff between backlog, batch size, and concurrency, and how they combine into downstream load.
It helps readers see that throughput tuning is really about in-flight work and downstream pressure.
Use it to explain why batch size and concurrency should be tuned together, not independently.

```mermaid
flowchart LR
		Input[Queue Backlog] --> Batch[Batch Size]
		Input --> Conc[Max Concurrency]
		Batch --> Inflight[Approx in-flight = batch * concurrency]
		Conc --> Inflight
		Inflight --> API[Downstream API Load]
		API --> Outcome[Latency, Errors, Cost]

        classDef appStyle fill:#0F172A,stroke:#334155,stroke-width:2px,color:#F8FAFC;
        classDef hubStyle fill:#1E40AF,stroke:#1D4ED8,stroke-width:2px,color:#FFFFFF;
        classDef destStyle fill:#0F766E,stroke:#115E59,stroke-width:1.5px,color:#FFFFFF;

        class Input,Batch,Conc,Inflight hubStyle;
        class API,Outcome destStyle;        
```
