git remote rm tikv_up
set -uxeo pipefail
# ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts
git remote add tikv_up https://github.com/tikv/tikv.git
git fetch tikv_up a0e8a7a163302bc9a7be5fd5a903b6a156797eb8
if [[ $(git diff HEAD `git merge-base HEAD tikv_up/a0e8a7a163302bc9a7be5fd5a903b6a156797eb8` --name-only -- components | wc -l) -ne 0 ]]; then
    exit 1
fi
if [[ $(git diff HEAD `git merge-base HEAD tikv_up/a0e8a7a163302bc9a7be5fd5a903b6a156797eb8` --name-only -- src | wc -l) -ne 0 ]]; then
    exit 1
fi