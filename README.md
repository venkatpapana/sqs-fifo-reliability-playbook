# SQS FIFO Reliability Playbook

Runnable examples and notes for exploring SQS FIFO reliability patterns across Node.js and Java.

This repo pairs with the Medium article **"We Moved to SQS FIFO and Found the Real Failure Modes"** and keeps the code examples, runbooks, and screenshots aligned across both languages.

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
