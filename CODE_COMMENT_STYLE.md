# Code Comment Style

This document describes the code comment style applied to TiKV repositories. Since TiKV uses Rust as its development language, most of the styles or rules described in this guide are specific to Rust.

When you are to commit, be sure to follow the style to write good code comments.

## Why does a good comment matter?

- To speed up the reviewing process
- To help maintain the code
- To improve the API document readability
- To improve the development efficiency of the whole team

## Where/When to comment?

Write a comment where/when there is context that is missing from the code or is hard to deduce from the code. Specifically, use a comment:

- For important code
- For obscure code
- For tricky or interesting code
- For a complex code block
- If a bug exists in the code but you cannot fix it or you just want to ignore it for the moment
- If the code is not optimal but you don’t have a smarter way now
- To remind yourself or others of missing functionality or upcoming requirements not present in the code

A comment is generally used for:

- Public items exported from crate (Doc comments)
    - Module
    - Type
    - Constant
    - Function
    - Fields
    - Method
- Variable
- Complex algorithm
- Test case
- TODO
- FIXME

## How to comment?

### Format of a good comment

- Non-doc comment

    - Used to document implementation details.
    - Use `//` for a line comment
    > **Note**: Block comments (`/* ... */`) are not recommended unless for personal reasons or temporary purposes prior to being converted to line comments.
    
- Doc comment

    - Used to document the interface of code (structures, fields, macros, etc.).
    - Use `///` for item documentation (functions, attributes, structures, etc.).
    - Use `//!` for module level documentation.
    > For more detailed guidelines on Rust doc comments, see [Making Useful Documentation Comments](https://doc.rust-lang.org/book/ch14-02-publishing-to-crates-io.html#making-useful-documentation-comments).

- Place the single-line and block comment above the code it’s annotating.
- Fold long lines of comments.
- The maximum width for a line is 100 characters.
- Use relative URLs when appropriate for Rustdoc links.

### Language for a good comment

- Word
    
    - Use **American English** rather than British English.
        
        - color, canceling, synchronize     (Recommended)
        - colour, cancelling, synchronise   (Not recommended)
    
    - Use correct spelling.

    - Use **standard or official capitalization**.
        
        - TiKV, TiDB-Binlog, Region, gRPC, RocksDB, GC, k8s, [mydumper](https://github.com/maxbube/mydumper), [Prometheus Pushgateway](https://github.com/prometheus/pushgateway)   (Right)
        - Tikv, TiDB Binlog, region, grpc, rocksdb, gc, k8S, MyDumper, Prometheus PushGateway   (Wrong)

    - Use words and expressions consistently.
        
        - "dead link" vs. "broken link" (Only one of them can appear in a single document.)
    
    - Do not use lengthy compound words.

    - Do not abbreviate unless it is necessary (for readability purposes).

    - "we" should be used only when it means the code writer *and* the reader.

- Sentence

    - Use standard grammar and correct punctuation.
    - Use relatively short sentences.

- For each comment, capitalize the first letter and end the sentence with a period.

- When used for description, comments should be **descriptive** rather than **imperative**.

    - Opens the file   (Right)
    - Open the file    (Wrong)

- Use "this" instead of "the" to refer to the current thing.
    
    - Gets the toolkit for this component   (Recommended)
    - Gets the toolkit for the component    (Not recommended)

- The Markdown format is allowed.
    
    - Opens the `log` file

### Tips for a good comment

- Comment code while writing it.
- Do not assume the code is self-evident.
- Avoid unnecessary comments for simple code.
- Write comments as if they were for you.
- Make sure the comment is up-to-date.
- Make sure you keep comments up to date when you edit code.
- Let the code speak for itself.

Thanks for your contribution!