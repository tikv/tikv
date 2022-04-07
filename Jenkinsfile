#!groovy

node {
    def TIDB_TEST_BRANCH = "master"
    def TIDB_BRANCH = "master"
    def PD_BRANCH = "master"

    fileLoader.withGit('git@github.com:pingcap/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/pingcap_tikv_branch.groovy').call(TIDB_TEST_BRANCH, TIDB_BRANCH, PD_BRANCH)
    }
}
