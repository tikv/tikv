# Process for becoming a maintainer

Depending on the status of your current organization, the process for becoming a maintainer may differ as described below:

## Your organization is not yet a maintainer

1. Express interest to the senior maintainers that your organization is interested in becoming a
  maintainer. Becoming a maintainer generally means that you are going to be spending substantial
  time (>25%) on TiKV for the foreseeable future. You are expected to have domain expertise and be extremely
  proficient in Rust. Ultimately your goal is to become a senior maintainer that will represent your
  organization.
2. We will expect you to start contributing increasingly complicated PRs, under the guidance
  of the existing senior maintainers.
3. We may ask you to do some PRs from our backlog.
4. As you gain experience with the code base and our standards, we will ask you to do code reviews
  for incoming PRs (i.e., all maintainers are expected to shoulder a proportional share of
  community reviews).
5. After a period of approximately 2-3 months of working together and making sure we see eye to eye,
  the existing senior maintainers will confer and decide whether to grant maintainer status or not.
  We make no guarantees on the length of time this will take, but 2-3 months is an approximate
  goal.

## Your organization is currently a maintainer

1. First decide whether your organization really needs more people with maintainer access. Valid
  reasons are "blast radius", a large organization that is working on multiple unrelated projects,
  etc.
2. Contact a senior maintainer for your organization and express interest.
3. Start doing PRs and code reviews under the guidance of your senior maintainer.
4. After a period of 1-2 months, the existing senior maintainers will discuss granting a "standard"
  maintainer access.
5. "Standard" maintainer access can be upgraded to "senior" maintainer access after another 1-2
  months of work and another conference among the existing senior maintainers.

## Maintainer responsibilities

* Monitor email aliases.
* Monitor Slack (delayed response is perfectly acceptable).
* Triage GitHub issues and perform pull request reviews for other maintainers and the community.
* During GitHub issue triage, apply all applicable [labels](https://github.com/tikv/tikv/labels)
  to each new issue. Labels are extremely useful for follow-up of future issues. Which labels to apply
  is somewhat subjective so just use your best judgment. A few of the most important labels that are
  not self explanatory are:
  * **beginner**: Mark any issue that can reasonably be accomplished by a new contributor with
    this label.
  * **help wanted**: Unless it is immediately obvious that someone is going to work on an issue (and
    if so assign it), mark it help wanted.
  * **question**: If it's unclear if an issue is immediately actionable, mark it with the
    question label. Questions are easy to search for and close out at a later time. Questions
    can be promoted to other issue types once it's clear they are actionable (at which point the
    question label should be removed).
* Make sure that ongoing PRs are moving forward at the right pace or closing them if they are not
  moving in a productive direction.
* Participate when called upon in the security release process. Note
  that although this should be a rare occurrence, if a serious vulnerability is found, the process
  may take up to several full days of work to implement. This possibility should be taken into account
  when discussing time commitment with employers.
* In general continue to be willing to spend at least 25% of your time working on TiKV (~1.25
  business days per week).
* We currently maintain an "on-call" rotation within the maintainers. Each on-call is 1 week long.
  Although all maintainers are welcome to perform all of the above tasks, it is the on-call
  maintainer's responsibility to triage incoming issues/questions and marshal ongoing work
  forward. To reiterate, it is *not* the responsibility of the on-call maintainer to answer all
  questions and do all reviews, but it is their responsibility to make sure that everything is
  being actively covered by someone.

## Cutting a release

We do releases approximately every 6 weeks. Here is how a regular release goes out:

1. Decide on the somewhat arbitrary time when a release will occur.
2. Take a look at open issues tagged with the current release, by
  [searching](https://github.com/tikv/tikv/issues) for
  "is:open is:issue milestone:[current milestone]".  Make your call on holding them off until
  they are fixed or bump them to the next milestone.
3. Do a final check of the [changelog](https://github.com/tikv/tikv/blob/master/CHANGELOG.md) and make any needed
  corrections.
4. **Wait for tests to pass on
  [master](https://internal.pingcap.net/idc-jenkins/job/build_tikv_master/).**
5. Create a [tagged release](https://github.com/tikv/tikv/releases). The release should
  start with "v" and be followed by the version number. E.g., "v1.6.0". **This must match the
  version in [Cargo.toml](Cargo.toml).**
6. Open a PR on the [Website repo](https://github.com/tikv/website) to reflect the new release.
7. If possible post on Twitter (or, ask @hoverbear to do it).
8. Do a new PR to update [Cargo.toml](Cargo.toml) to the next development release. E.g., "1.7.0-dev".

## When does a maintainer lose maintainer status

If a maintainer is no longer interested or cannot perform the maintainer duties listed above, they
should volunteer to be moved to emeritus status. In extreme cases this can also occur by a vote of
the maintainers per the voting process below.

# Conflict resolution and voting

In general, we prefer that technical issues and maintainer membership are amicably worked out
between the persons involved. If a dispute cannot be decided independently, the maintainers can be
called in to decide an issue. If the maintainers themselves cannot decide an issue, the issue will
be resolved by voting. The voting process is a simple majority in which each senior maintainer
receives two votes and each normal maintainer receives one vote.

# Adding new projects to the TiKV GitHub organization

New projects will be added to the tikv organization via GitHub issue discussion in one of the
existing projects in the organization. Once sufficient discussions have taken place (normally 3-5 business
days but depending on the volume of conversation), the maintainers of *the project where the issue
was opened* (since different projects in the organization may have different maintainers) will
decide whether the new project should be added. See [Conflict resolution and voting](#conflict-resolution-and-voting) if the maintainers
cannot easily decide.
