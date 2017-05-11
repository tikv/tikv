#!groovy

node {
    def TIDB_TEST_BRANCH = "master"
    def TIDB_BRANCH = "rc2.2"
    def PD_BRANCH = "rc2.2"
    def UCLOUD_OSS_URL = "http://pingcap-dev.hk.ufileos.com"

    env.GOROOT = "/usr/local/go"
    env.GOPATH = "/go"
    env.PATH = "/home/jenkins/.cargo/bin:${env.GOROOT}/bin:/home/jenkins/bin:/bin:${env.PATH}"
    env.LIBRARY_PATH = "/usr/local/lib:${env.LIBRARY_PATH}"
    env.LD_LIBRARY_PATH = "/usr/local/lib:${env.LD_LIBRARY_PATH}"

    catchError {
        stage('Prepare') {
            // tikv
            node('centos7_build') {
                def ws = pwd()
                dir("go/src/github.com/pingcap/tikv") {
                    // checkout
                    checkout scm
                    // build
                    sh """
                    rustup override set $RUST_TOOLCHAIN_BUILD
                    CARGO_TARGET_DIR=/home/jenkins/.target make static_release
                    """
                }
                stash includes: "go/src/github.com/pingcap/tikv/**", name: "tikv"
            }

            // tidb
            node('centos7_build') {
                def ws = pwd()
                dir("go/src/github.com/pingcap/tidb") {
                    // checkout
                    git changelog: false, credentialsId: 'github-iamxy-ssh', poll: false, url: 'git@github.com:pingcap/tidb.git', branch: "${TIDB_BRANCH}"
                    sh "GOPATH=${ws}/go:$GOPATH make parser"
                    def tidb_sha1 = sh(returnStdout: true, script: "curl ${UCLOUD_OSS_URL}/refs/pingcap/tidb/${TIDB_BRANCH}/centos7/sha1").trim()
                    sh "curl ${UCLOUD_OSS_URL}/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz | tar xz"
                }
                stash includes: "go/src/github.com/pingcap/tidb/**", name: "tidb"
            }

            // tidb-test
            dir("go/src/github.com/pingcap/tidb-test") {
                // checkout
                git changelog: false, credentialsId: 'github-iamxy-ssh', poll: false, url: 'git@github.com:pingcap/tidb-test.git', branch: "${TIDB_TEST_BRANCH}"
            }
            stash includes: "go/src/github.com/pingcap/tidb-test/**", name: "tidb-test"

            // pd
            def pd_sha1 = sh(returnStdout: true, script: "curl ${UCLOUD_OSS_URL}/refs/pingcap/pd/${PD_BRANCH}/centos7/sha1").trim()
            sh "curl ${UCLOUD_OSS_URL}/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz | tar xz"

            unstash 'tikv'
            sh "cp go/src/github.com/pingcap/tikv/bin/tikv-server bin/tikv-server && rm -rf go/src/github.com/pingcap/tikv"

            stash includes: "bin/**", name: "binaries"
        }

        stage('Test') {
            def tests = [:]

            tests["TiKV Test"] = {
                node("test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tikv'

                    dir("go/src/github.com/pingcap/tikv") {
                        sh """
                        rustup override set $RUST_TOOLCHAIN_TEST
                        make test
                        """
                    }
                }
            }

            def run_integration_ddl_test = { ddltest ->
                def ws = pwd()
                deleteDir()
                unstash 'tidb'
                unstash 'tidb-test'
                unstash 'binaries'

                try {
                    sh """
                    killall -9 ddltest_tidb-server || true
                    killall -9 tikv-server || true
                    killall -9 pd-server || true
                    bin/pd-server --name=pd --data-dir=pd &>pd_ddl_test.log &
                    sleep 10
                    bin/tikv-server --pd-endpoints=127.0.0.1:2379 --data-dir=tikv --addr=0.0.0.0:20160 --advertise-addr=127.0.0.1:20160 &>tikv_ddl_test.log &
                    sleep 10
                    """

                    timeout(10) {
                        dir("go/src/github.com/pingcap/tidb-test") {
                            sh """
                            ln -s tidb/_vendor/src ../vendor
                            cp ${ws}/go/src/github.com/pingcap/tidb/bin/tidb-server ddl_test/ddltest_tidb-server
                            cd ddl_test && GOPATH=${ws}/go:$GOPATH ./run-tests.sh -check.f='${ddltest}'
                            """
                        }
                    }
                } catch (err) {
                    throw err
                } finally {
                    sh "killall -9 ddltest_tidb-server || true"
                    sh "killall -9 tikv-server || true"
                    sh "killall -9 pd-server || true"
                }
            }

            tests["Integration DDL Insert Test"] = {
                node("test") {
                    run_integration_ddl_test('TestDDLSuite.TestSimple.*Insert')
                }
            }

            tests["Integration DDL Update Test"] = {
                node("test") {
                    run_integration_ddl_test('TestDDLSuite.TestSimple.*Update')
                }
            }

            tests["Integration DDL Delete Test"] = {
                node("test") {
                    run_integration_ddl_test('TestDDLSuite.TestSimple.*Delete')
                }
            }

            tests["Integration DDL Other Test"] = {
                node("test") {
                    run_integration_ddl_test('TestDDLSuite.TestSimp(le\$|leMixed|leInc)')
                }
            }

            tests["Integration DDL Column and Index Test"] = {
                node("test") {
                    run_integration_ddl_test('TestDDLSuite.Test(Column|Index)')
                }
            }

            tests["Integration Connection Test"] = {
                node("test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'
                    unstash 'binaries'

                    try {
                        sh """
                        killall -9 tikv-server || true
                        killall -9 pd-server || true
                        bin/pd-server --name=pd --data-dir=pd &>pd_conntest.log &
                        sleep 10
                        bin/tikv-server --pd-endpoints=127.0.0.1:2379 --data-dir=tikv --addr=0.0.0.0:20160 --advertise-addr=127.0.0.1:20160 &>tikv_conntest.log &
                        sleep 10
                        """

                        dir("go/src/github.com/pingcap/tidb") {
                            sh """
                            GOPATH=`pwd`/_vendor:${ws}/go:$GOPATH CGO_ENABLED=1 go test --args with-tikv store/tikv/*.go
                            """
                        }
                    } catch (err) {
                        throw err
                    } finally {
                        sh "killall -9 tikv-server || true"
                        sh "killall -9 pd-server || true"
                    }
                }
            }

            def run_integration_other_test = { mytest ->
                def ws = pwd()
                deleteDir()
                unstash 'tidb'
                unstash 'tidb-test'
                unstash 'binaries'

                try {
                    sh """
                    killall -9 tikv-server || true
                    killall -9 pd-server || true
                    bin/pd-server --name=pd --data-dir=pd &>pd_${mytest}.log &
                    sleep 10
                    bin/tikv-server --pd-endpoints=127.0.0.1:2379 --data-dir=tikv --addr=0.0.0.0:20160 --advertise-addr=127.0.0.1:20160 &>tikv_${mytest}.log &
                    sleep 10
                    """

                    dir("go/src/github.com/pingcap/tidb-test") {
                        sh """
                        ln -s tidb/_vendor/src ../vendor
                        GOPATH=${ws}/go:$GOPATH TIKV_PATH='127.0.0.1:2379' TIDB_TEST_STORE_NAME=tikv make ${mytest}
                        """
                    }
                } catch (err) {
                    throw err
                } finally {
                    sh "killall -9 tikv-server || true"
                    sh "killall -9 pd-server || true"
                }
            }

            tests["Integration TiDB Test"] = {
                node('test') {
                    run_integration_other_test('tidbtest')
                }
            }

            tests["Integration MySQL Test"] = {
                node("test") {
                    run_integration_other_test('mysqltest')
                }
            }

            tests["Integration GORM Test"] = {
                node("test") {
                    run_integration_other_test('gormtest')
                }
            }

            tests["Integration Go SQL Test"] = {
                node("test") {
                    run_integration_other_test('gosqltest')
                }
            }

            parallel tests
        }

        currentBuild.result = "SUCCESS"
    }

    stage('Summary') {
        def getChangeLogText = {
            def changeLogText = ""
            for (int i = 0; i < currentBuild.changeSets.size(); i++) {
                for (int j = 0; j < currentBuild.changeSets[i].items.length; j++) {
                    def commitId = "${currentBuild.changeSets[i].items[j].commitId}"
                    def commitMsg = "${currentBuild.changeSets[i].items[j].msg}"
                    changeLogText += "\n" + commitId.take(7) + " - " + commitMsg
                }
            }
            return changeLogText
        }
        def changelog = getChangeLogText()
        def duration = (System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000

        def slackmsg = "${env.JOB_NAME}-${env.BUILD_NUMBER}: ${currentBuild.result}, Duration: ${duration}, Changelogs: ${changelog}"

        if (currentBuild.result != "SUCCESS") {
            slackSend channel: '#kv', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
        }
    }
}
