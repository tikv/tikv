git remote rm tikv_up
set -uxeo pipefail
# ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts
git remote add tikv_up https://github.com/tikv/tikv.git
git fetch tikv_up master
if [[ $(git diff HEAD `git merge-base HEAD tikv_up/master` --name-only -- components | wc -l) -ne 0 ]]; then
    exit 1
fi
if [[ $(git diff HEAD `git merge-base HEAD tikv_up/master` --name-only -- src | wc -l) -ne 0 ]]; then
    exit 1
fi