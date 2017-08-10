#!groovy

node {
    def TIDB_TEST_BRANCH = "rc3"
    def TIDB_BRANCH = "rc3"
    def PD_BRANCH = "rc3"

    fileLoader.withGit('git@github.com:pingcap/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/pingcap_tikv_branch.groovy').call(TIDB_TEST_BRANCH, TIDB_BRANCH, PD_BRANCH)
    }
}
