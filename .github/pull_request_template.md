<!--
Thank you for contributing to TiKV!

If you haven't already, please read TiKV's [CONTRIBUTING](https://github.com/tikv/tikv/blob/master/CONTRIBUTING.md) document.

If you're unsure about anything, just ask; somebody should be along to answer within a day or two.

PR Title Format:
1. module [, module2, module3]: what's changed
2. *: what's changed
-->

### What is changed and how it works?

Close #xxx <!-- Associate issue that describes the problem the PR tries to solve. -->

What's Changed:

### Related changes

- PR to update `pingcap/docs`/`pingcap/docs-cn`:
- PR to update `pingcap/tidb-ansible`:
- Need to cherry-pick to the release branch

### Check List <!--REMOVE the items that are not applicable-->

Tests <!-- At least one of them must be included. -->

- Unit test
- Integration test
- Manual test (add detailed scripts or steps below)
- No code

Side effects

- Performance regression
    - Consumes more CPU
    - Consumes more MEM
- Breaking backward compatibility

### Release note <!-- bugfixes or new feature need a release note -->

```release-note
Please add a release note.
If you don't think this PR needs a release note then fill it with None.
If this PR will be picked to release branch, then a release note is probably required.
```
