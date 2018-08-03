# How to contribute

This document outlines some of the conventions on development workflow, commit message formatting, contact points and other
resources to make it easier to get your contribution accepted.

## Getting started

- Fork the repository on GitHub.
- Read the README.md for build instructions.
- Play with the project, submit bugs, submit patches!

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

### Step 1: Fork in the cloud

1. Visit [https://github.com/pingcap/tikv](https://github.com/pingcap/tikv)
2. Click Fork button (top right) to establish a cloud-based fork.

### Step 2: Clone fork to local storage

Define a local working directory:

```sh
# Define a local working directory:
$ working_dir=/.../src/github.com/pingcap
$ user={your github profile name}
```

Create your clone:

```sh
$ mkdir -p $working_dir
$ cd $working_dir
$ git clone https://github.com/$user/tikv.git
$ cd $working_dir/tikv
```

View the remote info:

```sh
$ git remote -v
origin	https://github.com/$user/tikv.git (fetch)
origin	https://github.com/$user/tikv.git (push)

$ git remote add upstream https://github.com/pingcap/tikv.git
$ git remote -v
origin	https://github.com/$user/tikv.git (fetch)
origin	https://github.com/$user/tikv.git (push)
upstream	https://github.com/pingcap/tikv.git (fetch)
upstream	https://github.com/pingcap/tikv.git (push)

# Never push to upstream master since you do not have write access.
$ git remote set-url --push upstream no_push
$ git remote -v
origin	https://github.com/$user/tikv.git (fetch)
origin	https://github.com/$user/tikv.git (push)
upstream	https://github.com/pingcap/tikv.git (fetch)
upstream	no_push (push)
```

### Step 3: Branch

Get your local master up to date:

```sh
cd $working_dir/tikv
git fetch upstream
git checkout master
git rebase upstream/master
```

Branch from master:

```sh
git checkout -b myfeature
```

### Step 4: Develop

#### Edit the code

You can now edit the code on the `myfeature` branch.

#### Run stand-alone mode

```sh
$ make build
```

#### Run Test

When you're ready to test out your changes, use the `dev` task. It will format your codebase, build with clippy enabled, and run tests. This should run without failure before you create a PR.

```sh
$ make dev
```

- Run tests and make sure all the tests are passed.
- Make sure your commit messages are in the proper format.

### Step 5: Keep your branch in sync

```sh
# While on your myfeature branch.
cd $working_dir/tikv
git fetch upstream
git rebase upstream/master
```

### Step 6: Commit

Commit your changes.

```sh
git commit
```

### Step 7: Push

When ready to review (or just to establish an offsite backup or your work), push your branch to your fork on github.com:

```sh
git push -f origin myfeature
```

### Step 8: Create a pull request

1. Visit your fork at [https://github.com/$user/tikv](https://github.com/$user/tikv) (replace $user obviously).
2. Click the Compare & pull request button next to your `myfeature` branch.

### Step 9: Get a code review

Once your pull request has been opened, it will be assigned to at least two reviewers. Those reviewers will do a thorough code review, looking for correctness, bugs, opportunities for improvement, documentation and comments, and style.

Commit changes made in response to review comments to the same branch on your fork.

Very small PRs are easy to review. Very large PRs are very difficult to review.

Thanks for your contributions!

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
