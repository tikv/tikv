# How to contribute

This document outlines some of the conventions on development workflow, commit message formatting, contact points and other
resources to make it easier to get your contribution accepted.

## Getting started

- Fork the repository on GitHub.
- Read the README.md for build instructions.
- Play with the project, submit bugs, submit patches!

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

- Create a topic branch from where you want to base your work. This is usually master.
- Make commits of logical units and add test case if the change fixes a bug or adds new functionality.
- Run tests and make sure all the tests are passed.
- Make sure your commit messages are in the proper format (see below).
- Push your changes to a topic branch in your fork of the repository.
- Submit a pull request.
- Your PR must receive LGTMs from two maintainers.

Thanks for your contributions!

## Review Flow

TiKV uses bors, a bot used by projects like Rust and Servo. If you are already familiar with how to use bors, please just use it.

Reviewers should follow this process:

- Find a PR to review, or be requested to review one.
- Review it. Give complete feedback, make sure the contributor can understand your suggestions. If needed, help them.
- If you approve it, use the 'Submit Review' button and choose 'Approve.'
- If you are the first approver, request someone else to review it as well.
- If you are the second approver, you can leave `bors r+` in the review or as a comment.
    + If the change is trivial, or you are sure it cannot fail, you can use `bors rollup` to allow bors to batch a merge.
- bors will merge the code after a time so long as there are no merge conflicts with master, and the tests pass.
    + **You do not need to hit the 'Update Branch' button unless there are conflicts or you think it will fail.**
- You will recieve a notification for the PR saying it was merged, or it failed.

If you need support using bors you should consult [the guide](https://bors.tech/documentation/getting-started/), you may also ping [@hoverbear](https://github.com/hoverbear/).

### Code style

The coding style suggested by the rust community and clippy project. See the [style doc](https://aturon.github.io/README.html) and [clippy](https://github.com/Manishearth/rust-clippy) for details.

Please follow this style to make TiKV easy to review, maintain and develop.

### Format of the Commit Message

We follow a rough convention for commit messages that is designed to answer two
questions: what changed and why. The subject line should feature the what and
the body of the commit should describe the why.

```
engine/raftkv: add comment for variable declaration.

Improve documentation.
```

The format can be described more formally as follows:

```
<subsystem>: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
<footer>(optional)
```

The first line is the subject and should be no longer than 70 characters, the
second line is always blank, and other lines should be wrapped at 80 characters.
This allows the message to be easier to read on GitHub as well as in various
git tools.

If the change affects more than one subsystem, you can use comma to separate them like `util/codec,util/types:`.

If the change affects many subsystems, you can use ```*``` instead, like ```*:```.

For the why part, if no specific reason for the change,
you can use one of some generic reasons like "Improve documentation.",
"Improve performance.", "Improve robustness.", "Improve test coverage."

### Signing off the Commit

The project now enables [DCO check](https://github.com/probot/dco#how-it-works) and the commit message must contain a `Signed-off-by` line for [Developer Certificate of Origin](https://developercertificate.org/)

You can use option `-s` for `git commit` to automatically add a `Signed-off-by` to the commit message. 
